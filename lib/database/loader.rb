#!/usr/bin/env ruby
require 'csv'
require 'rubygems'
require 'sequel'
require 'trollop'
require 'tempfile'
require 'sqlite3'
require 'pry'
require 'date'
require 'ruby-duration'
require 'nokogiri'
require 'open-uri'

seasons_h = { 
  #"season", "day 1", "reg season end + 1", "playoffs end + 1"
  "2014-15" => [ "2014-10-28", "2015-4-16", "2015-6-17" ],
  "2013-14" => [ "2013-10-29", "2014-4-17", "2014-6-16" ],
  "2012-13" => [ "2012-10-30", "2013-4-18", "2013-6-21" ],
  "2011-12" => [ "2011-12-25", "2012-4-27", "2012-6-22" ],
  "2010-11" => [ "2010-10-29", "2011-4-17", "2011-6-13" ],
  "2009-10" => [ "2009-10-27", "2010-4-15", "2010-6-18" ],
  "2008-09" => [ "2008-10-28", "2009-4-17", "2009-6-15" ],
  "2007-08" => [ "2007-10-30", "2008-4-17", "2008-6-18" ],
  "2006-07" => [ "2006-10-31", "2007-4-19", "2007-6-15" ],
  "2005-06" => [ "2005-11-1", "2006-4-20", "2006-6-21" ],
  "2004-05" => [ "2004-11-2", "2005-4-21", "2005-6-24" ],
  "2003-04" => [ "2003-10-28", "2004-4-15", "2004-6-16" ],
  "2002-03" => [ "2002-10-29", "2003-4-17", "2003-6-16" ],
  "2001-02" => [ "2001-10-30", "2002-4-18", "2002-6-13" ],
  "2000-01" => [ "2000-10-31", "2001-4-19", "2001-6-16" ],
  "1999-00" => [ "1999-11-2", "2000-4-20", "2000-6-20" ],
  "1998-99" => [ "1999-2-5", "1999-5-6", "1999-6-26" ],
  "1997-98" => [ "1997-10-31", "1998-4-20", "1998-6-15" ],
  "1996-97" => [ "1996-11-1", "1997-4-21", "1997-6-14" ],
  "1995-96" => [ "1995-11-3", "1996-4-22", "1996-6-17" ],
  "1994-95" => [ "1994-11-4", "1995-4-24", "1995-6-15" ],
  "1993-94" => [ "1993-11-5", "1994-4-25", "1994-6-23" ],
  "1992-93" => [ "1992-11-6", "1993-4-26", "1993-6-21" ],
  "1991-92" => [ "1991-11-1", "1992-4-20", "1992-6-15" ] 
}

# ruby 1.8 FasterCSV compatibility
if CSV.const_defined? :Reader
  require 'fastercsv'
  Object.send(:remove_const, :CSV)
  CSV = FasterCSV
end

OPTIONS = Trollop::options do
  banner <<-EOS
Usage:
	csv2sqlite [options] TABLENAME [...]

where [options] are:
EOS
  opt :irb_console,  "Open an IRB session after loading FILES into an in-memory DB"
  opt :sqlite_console,  "Execute 'sqlite3 FILENAME.db' afterwards"
  opt :output,  "FILENAME.db where to save the sqlite database", :type => :string
end

def getDatabase(filename)
  puts "Connecting to sqlite://#{filename}"
  database = Sequel.sqlite(filename)
  # database.test_connection # saves blank file
  return database
end

def populateTableFromCSV(database,filename)
  options = { :headers    => true,
              :header_converters => :symbol,
              :converters => :all  }
  data = CSV.table(filename, options)
  headers = data.headers
  tablename = File.basename(filename, '').gsub(/[^0-9a-zA-Z_]/,'_').to_sym

  puts "Dropping and re-creating table #{tablename}"
  DB.drop_table? tablename
  DB.create_table tablename do
    # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
    # primary_key :id
    # Float :price
    data.by_col!.each do |columnName,rows|
      columnType = getCommonClass(rows) || String
      column columnName, columnType
    end
  end
  data.by_row!.each do |row|
    database[tablename].insert(row.to_hash)
  end
end

def populateTableFromCSVnoCreate(database,filename)
  options = { :headers    => true,
              :header_converters => :symbol,
              :converters => :all  }
  data = CSV.table(filename, options)
  headers = data.headers
  tablename = File.basename(filename, '').gsub(/[^0-9a-zA-Z_]/,'_').to_sym

  puts "Dropping and re-creating table #{tablename}"
  DB.drop_table? tablename
  DB.create_table tablename do
    # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
    # primary_key :id
    # Float :price
    data.by_col!.each do |columnName,rows|
      columnType = getCommonClass(rows) || String
      column columnName, columnType
    end
  end
  data.by_row!.each do |row|
    database[tablename].insert(row.to_hash)
  end
end
# 
# :call-seq:
#   getCommonClass([1,2,3])         => FixNum
#   getCommonClass([1,"bob",3])     => String
#
# Returns the class of each element in +rows+ if same for all elements, otherwise returns nil
#
def getCommonClass(rows)
  return rows.inject(rows[0].class) { |klass, el| break if klass != el.class ; klass }
end


def launchConsole(database)
  require 'irb'
  require 'pp'
  require 'yaml'

  puts "Launching IRB Console.\n\n"
  puts "You can now interact with the database via DB. Examples:"
  puts "  DB.tables #=> SHOW tables"
  puts "  ds = DB[:posts] #=> SELECT * FROM posts"
  puts "  ds = DB[:posts].where(:id => 1) #==> SELECT * FROM posts WHERE id => 1"
  puts "  puts DB[:posts].all ; nil #=> executes query, pretty prints results" 
  puts ""
  puts "See http://sequel.rubyforge.org/rdoc/files/doc/dataset_basics_rdoc.html"
  puts "To launch sqlite3 console, type 'sqlite3'"
  puts ""

  puts "Available tables: "
  database.tables.each do |table|
    puts "  DB[:#{table.to_s}] - #{DB[table].count.to_s} records"
  end
  puts ""

  IRB.start
  catch :IRB_EXIT do
    # IRB.start should trap this but doesn't
    exit
  end
end

def sqlite3()
  launchSqliteConsole()
end

def launchSqliteConsole()
  File.exists?(DB_PATH) or Trollop.die "Unable to launch sqlite3; invalid file: #{DB_PATH}" 
  puts "Launching 'sqlite3 #{DB_PATH}'. Table schema:\n"
  # NB: Using Kernel.system instead of Kernel.exec to allow Tempfile cleanup
  system("sqlite3 #{DB_PATH} '.schema'")
  puts ""
  system("sqlite3 #{DB_PATH}")
  exit
end

if OPTIONS[:output]
  DB_PATH = OPTIONS[:output]
else
  DB_TMP = Tempfile.new(['csv2sqlite','.sqlite3'])
  DB_PATH = DB_TMP.path
end

DB = getDatabase(DB_PATH)

def median( hash )
  array = hash.sort_by{|k,v| v}
  len = array.length
  center = len / 2
  med = len % 2 ? array[center][1] : ( array[center][1] + array[center+1][1] ) / 2
end

def mean( value, total, games_played )
  if( value.is_a? Float )
    total = total + value
  else
    binding.pry
    p "error: #{value}"
  end

  if 0 == games_played
    mean = 0
  else
    mean = total / games_played
  end
  return total, mean.round(3)
end

def calculateAveragePoints( player )
  avg_FG2M = total_FG2M / games_played
  avg_FG3M = total_FG3M / games_played
  avg_FTM = total_FTM / games_played
  avg_FTA = total_FTA / games_played
  
  avg_PTS = row[:mean_FTM] + 2.0 * row[:mean_FG2M] + 3.0 * row[:mean_FG3M]
  avg_location_PTS = split.home.avg_PTS
  avg_location_PTS = split.away.avg_PTS
  avg_opponent_defensive_rating = (opp_defensive_rating / 100) # need to scale this and multiply by coefficient
  avg_opponent_defensive_rating = (opp_defensive_rating / 100) # need to scale this and multiply by coefficient
  avg_opponent_defender_rating = (opp_defender_rating / 100) # need to scale this and multiply by coefficient
  
  if split.games_played > 0
    avg_pts_REST_mode = split.rest_mode.PTS / split.games_played
  end

end

Trollop.die "Missing CSV file argument(s)" unless ARGV.count > 0
until ARGV.empty? do 
  file = ARGV.shift
  File.exists?(file) or Trollop.die "Invalid file: #{file}" 
  puts "Parsing file #{file}"
  seasons = Dir.glob('*').select {|f| File.directory? f and f.match /\d+/}
  seasons = seasons_h.keys

  database = DB

  seasontype = ["regularseason", "playoffs"]
  categories = ["traditional","advanced", "misc", "scoring", "usage", "fourfactors", "playertrack"]

  tables = [ "advanced_PlayerStats", "advanced_TeamStats", "fourfactors_sqlPlayersFourFactors", "fourfactors_sqlTeamsFourFactors", "misc_sqlPlayersMisc", "misc_sqlTeamsMisc", "playertrack_PlayerTrack", "playertrack_PlayerTrackTeam", "scoring_sqlPlayersScoring", "scoring_sqlTeamsScoring", "traditional_PlayerStats", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlPlayersUsage", "usage_sqlTeamsUsage" ]

  player_tables = [ "advanced_PlayerStats", "fourfactors_sqlPlayersFourFactors", "misc_sqlPlayersMisc", "playertrack_PlayerTrack", "scoring_sqlPlayersScoring", "traditional_PlayerStats", "usage_sqlPlayersUsage" ]
  team_tables = [ "advanced_TeamStats", "fourfactors_sqlTeamsFourFactors", "misc_sqlTeamsMisc", "playertrack_PlayerTrackTeam", "scoring_sqlTeamsScoring", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlTeamsUsage" ]
  
  #http://stats.nba.com/stats/commonplayerinfo?LeagueID=00&PlayerID=202699&SeasonType=Regular+Season

  binding.pry
  seasontype_url = [["Regular+Season", "regularseason"],["Playoffs", "playoffs"]]
=begin
  seasons.each{|season|
    p season
    seasontype_url.each{|type|
      tablename = type[1] + " player gamelogs"
      tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

      players = DB[ :"#{type[1]}_#{player_tables[5]}" ].where(:SEASON => season).distinct.entries

      players.each_with_index{|player,i|
        begin
        url = "http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID=#{player[:PLAYER_ID]}&Season=#{season}&SeasonType=#{type[0]}"
        p "#{i} / #{players.size} #{url}"
        begin
          doc = Nokogiri::HTML( open( url ) )
          json = JSON.parse( doc.text )
        rescue StandardError => e
          binding.pry
          p e
        end


#        json["resultSets"][0]["rowSet"].each{|row|
#          h = Hash.new
#          row.each_with_index{|item,i|
#            h[ json["resultSets"][0]["headers"][i] ] = item
#          }

        json["resultSets"].each{|resultSet|
          csv = ""
          csv = csv + resultSet["headers"].to_csv
          resultSet["rowSet"].each{|row|
            csv = csv + row.to_csv
          }
          binding.pry
          File.open( season + "/" + type[1] + "/playergamelogs" + resultSet["name"] + ".csv", "w" ){|f|
            f.write csv
          }
        }

          h["PLAYER_NAME"] = player[:PLAYER_NAME]
          h["TEAM_ID"] = player[:TEAM_ID]
          h["TEAM_ABBREVIATION"] = player[:TEAM_ABBREVIATION]
          h["TEAM_CITY"] = player[:TEAM_CITY]

          database[tablename].insert( h )
        }
        rescue StandardError => e
          binding.pry
          p e
        end
      }
    }
  }
  binding.pry
=end
=begin
  seasontype_url = ["Regular+Season", "Playoffs"]
  seasons.each{|season|
    seasontype_url.each{|type|
      tablename = type + " gamelogs"
      tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

      url = "http://stats.nba.com/stats/leaguegamelog?Counter=1000&Direction=DESC&LeagueID=00&PlayerOrTeam=T&Season=#{season}&SeasonType=#{type}&Sorter=PTS"
      p url
      doc = Nokogiri::HTML( open( url ) )
      json = JSON.parse( doc.text )

      if !DB.table_exists? tablename
        options = { :headers    => true,
                    :header_converters => nil,
                    :converters => nil } # This makes all fields strings

        #data = CSV.table(filename, options)
        headers = json["resultSets"][0]["headers"]

        puts "Dropping and re-creating table #{tablename}"
        DB.drop_table? tablename
        DB.create_table tablename do
          headers.each do |columnName|
            columnType = String
            column columnName, columnType
          end
        end
      end
      
      json["resultSets"][0]["rowSet"].each{|row|
        h = Hash.new
        row.each_with_index{|item,i|
          h[ json["resultSets"][0]["headers"][i] ] = item
        }
        database[tablename].insert( h )
      }
    }
  }
  binding.pry
=end
=begin
#Push CSV files into DB
  seasons.each{|season|
    seasontype.each{|type|
      tables.each{|category|
        Dir.glob(season+"/" + type + "/*" + category + ".csv").each_with_index{|filename,i|
          tablename = type + " " + category
          tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
          #if 0 == i
          if !DB.table_exists? tablename
            options = { :headers    => true,
                        :header_converters => nil,
                        :converters => nil }
                        #:converters => [:date_time, :float] }
                        #:converters => :all  }

            #season = season.gsub(/-/,"")
            #filename = filename.gsub(/-/,"_")

            data = CSV.table(filename, options)
            #data = CSV.foreach(filename, headers: true, converters: :all){|row|
            #  p row
            #}
            headers = data.headers
            #tablename = File.basename(filename, '').gsub(/[^0-9a-zA-Z_]/,'_').to_sym

            puts "Dropping and re-creating table #{tablename}"
            DB.drop_table? tablename
            DB.create_table tablename do
              # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
              # primary_key :id
              # Float :price
              data.by_col!.each do |columnName,rows|
                #if columnName.match /_ID/
                #  columnType = String
                #  column columnName, columnType
                #else
                  columnType = getCommonClass(rows) || String
                  column columnName, columnType
                #end
                #p "#{columnName} #{columnType}"
              end
            end
            data.by_row!.each do |row|
              database[tablename].insert(row.to_hash)
            end
          else
            p "processing #{filename}"
            options = { :headers    => true,
                        :header_converters => nil,
                        :converters => nil }
                        #:converters => [:date_time, :float ] }
                        #:converters => :all }
                
            data = CSV.table(filename, options)

            begin
              data.by_row!.each do |row|
                database[tablename].insert(row.to_hash)
              end
            rescue StandardError => e
              binding.pry
              p e
            end
          end
        }
      }
    }
  }
=end

#=begin
  #Calculate daily averages for every day in the past
  seasons.each{|season|
    seasontype.each{|type|
      averages = Hash.new

      tablename = season + " " + type + " daily averages"
      tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

      options = { :headers    => true,
                  :header_converters => nil,
                  :converters => :all  }

        players = Hash.new

        #headers = data.headers

        puts "Dropping and re-creating table #{tablename}"
        DB.drop_table? tablename
        DB.create_table tablename do
          # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
          # primary_key :id
          # Float :price
          column :player, :text
          column :date, :text
          column :date_of_data, :text
          column :team_abbreviation, :text
          column :player_name, :text

          column :games_played, :integer
          column :games_started, :integer
          column :wins, :integer
          column :losses, :integer
          column :ties, :integer
          column :win_pct, :decimal
          column :total_mins_played, :decimal
          column :mean_mins_played, :decimal
          column :median_mins_played, :decimal
          column :total_FGM, :decimal
          column :mean_FGM, :decimal
          column :median_FGM, :decimal
          column :total_FGA, :decimal
          column :mean_FGA, :decimal
          column :median_FGA, :decimal
          column :total_FG_PCT, :decimal
          column :FG_PCT, :decimal
          column :total_FG3M, :decimal
          column :total_FG3A, :decimal
          column :total_FG3_PCT, :decimal
          column :FG3_PCT, :decimal
          column :total_FTM, :decimal
          column :total_FTA, :decimal
          column :total_FT_PCT, :decimal
          column :FT_PCT, :decimal
          column :total_OREB, :decimal
          column :total_DREB, :decimal
          column :total_REB, :decimal
          column :total_AST, :decimal
          column :total_STL, :decimal
          column :total_BLK, :decimal
          column :total_TO, :decimal
          column :total_PF, :decimal
          column :total_PTS, :decimal
          column :total_plus_minus, :decimal
          column :total_TS_PCT, :decimal
          column :mean_TS_PCT, :decimal
          column :median_TS_PCT, :decimal
          column :total_EFG_PCT, :decimal
          column :mean_EFG_PCT, :decimal
          column :median_EFG_PCT, :decimal
          column :total_PCT_FGA_3PT, :decimal
          column :mean_PCT_FGA_3PT, :decimal
          column :median_PCT_FGA_3PT, :decimal
          column :total_FTA_RATE, :decimal
          column :mean_FTA_RATE, :decimal
          column :median_FTA_RATE, :decimal
          column :total_OREB_PCT, :decimal
          column :mean_OREB_PCT, :decimal
          column :median_OREB_PCT, :decimal
          column :total_DREB_PCT, :decimal
          column :mean_DREB_PCT, :decimal
          column :median_DREB_PCT, :decimal
          column :total_REB_PCT, :decimal
          column :mean_REB_PCT, :decimal
          column :median_REB_PCT, :decimal
          column :total_AST_PCT, :decimal
          column :mean_AST_PCT, :decimal
          column :median_AST_PCT, :decimal
          column :total_PCT_STL, :decimal
          column :mean_PCT_STL, :decimal
          column :median_PCT_STL, :decimal
          column :total_PCT_BLK, :decimal
          column :mean_PCT_BLK, :decimal
          column :median_PCT_BLK, :decimal
          column :total_TO_PCT, :decimal
          column :mean_TO_PCT, :decimal
          column :median_TO_PCT, :decimal
          column :total_USG_PCT, :decimal
          column :mean_USG_PCT, :decimal
          column :median_USG_PCT, :decimal
          column :total_OFF_RATING, :decimal
          column :mean_OFF_RATING, :decimal
          column :median_OFF_RATING, :decimal
          column :total_DEF_RATING, :decimal
          column :mean_DEF_RATING, :decimal
          column :median_DEF_RATING, :decimal
          column :total_NET_RATING, :decimal
          column :mean_NET_RATING, :decimal
          column :median_NET_RATING, :decimal
          column :total_AST_TO, :decimal
          column :mean_AST_TO, :decimal
          column :median_AST_TO, :decimal
          column :total_AST_RATIO, :decimal
          column :mean_AST_RATIO, :decimal
          column :median_AST_RATIO, :decimal
          column :total_PACE, :decimal
          column :mean_PACE, :decimal
          column :median_PACE, :decimal
          column :total_PIE, :decimal
          column :mean_PIE, :decimal
          column :median_PIE, :decimal
          column :mean_FG_PCT, :decimal
          column :median_FG_PCT, :decimal
          column :mean_FG3M, :decimal
          column :median_FG3M, :decimal
          column :mean_FG3A, :decimal
          column :median_FG3A, :decimal
          column :mean_FG3_PCT, :decimal
          column :median_FG3_PCT, :decimal
          column :mean_FTM, :decimal
          column :median_FTM, :decimal
          column :mean_FTA, :decimal
          column :median_FTA, :decimal
          column :mean_FT_PCT, :decimal
          column :median_FT_PCT, :decimal
          column :mean_OREB, :decimal
          column :median_OREB, :decimal
          column :mean_DREB, :decimal
          column :median_DREB, :decimal
          column :mean_REB, :decimal
          column :median_REB, :decimal
          column :mean_AST, :decimal
          column :median_AST, :decimal
          column :mean_STL, :decimal
          column :median_STL, :decimal
          column :mean_BLK, :decimal
          column :median_BLK, :decimal
          column :mean_TO, :decimal
          column :median_TO, :decimal
          column :mean_PF, :decimal
          column :median_PF, :decimal
          column :mean_PTS, :decimal
          column :median_PTS, :decimal
          column :mean_plus_minus, :decimal
          column :median_plus_minus, :decimal
          #misc
          column :total_PTS_OFF_TOV, :decimal
          column :mean_PTS_OFF_TOV, :decimal
          column :median_PTS_OFF_TOV, :decimal
          column :total_PTS_2ND_CHANCE, :decimal
          column :mean_PTS_2ND_CHANCE, :decimal
          column :median_PTS_2ND_CHANCE, :decimal
          column :total_PTS_FB, :decimal
          column :mean_PTS_FB, :decimal
          column :median_PTS_FB, :decimal
          column :total_PTS_PAINT, :decimal
          column :mean_PTS_PAINT, :decimal
          column :median_PTS_PAINT, :decimal
          #scoring
          column :total_PCT_FGA_2PT, :decimal
          column :mean_PCT_FGA_2PT, :decimal
          column :median_PCT_FGA_2PT, :decimal
          column :total_PCT_PTS_2PT, :decimal
          column :mean_PCT_PTS_2PT, :decimal
          column :median_PCT_PTS_2PT, :decimal
          column :total_PCT_PTS_2PT_MR, :decimal
          column :mean_PCT_PTS_2PT_MR, :decimal
          column :median_PCT_PTS_2PT_MR, :decimal
          column :total_PCT_PTS_3PT, :decimal
          column :mean_PCT_PTS_3PT, :decimal
          column :median_PCT_PTS_3PT, :decimal
          column :total_PCT_PTS_FB, :decimal
          column :mean_PCT_PTS_FB, :decimal
          column :median_PCT_PTS_FB, :decimal
          column :total_PCT_PTS_FT, :decimal
          column :mean_PCT_PTS_FT, :decimal
          column :median_PCT_PTS_FT, :decimal
          column :total_PCT_PTS_OFF_TOV, :decimal
          column :mean_PCT_PTS_OFF_TOV, :decimal
          column :median_PCT_PTS_OFF_TOV, :decimal
          column :total_PCT_PTS_PAINT, :decimal
          column :mean_PCT_PTS_PAINT, :decimal
          column :median_PCT_PTS_PAINT, :decimal
          column :total_AST_2PM, :decimal
          column :mean_AST_2PM, :decimal
          column :median_AST_2PM, :decimal
          column :total_PCT_AST_2PM, :decimal
          column :mean_PCT_AST_2PM, :decimal
          column :median_PCT_AST_2PM, :decimal
          column :total_UAST_2PM, :decimal
          column :mean_UAST_2PM, :decimal
          column :median_UAST_2PM, :decimal
          column :total_PCT_UAST_2PM, :decimal
          column :mean_PCT_UAST_2PM, :decimal
          column :median_PCT_UAST_2PM, :decimal
          column :total_AST_3PM, :decimal
          column :mean_AST_3PM, :decimal
          column :median_AST_3PM, :decimal
          column :total_PCT_AST_3PM, :decimal
          column :mean_PCT_AST_3PM, :decimal
          column :median_PCT_AST_3PM, :decimal
          column :total_UAST_3PM, :decimal
          column :mean_UAST_3PM, :decimal
          column :median_UAST_3PM, :decimal
          column :total_PCT_UAST_3PM, :decimal
          column :mean_PCT_UAST_3PM, :decimal
          column :median_PCT_UAST_3PM, :decimal
          column :total_AST_FGM, :decimal
          column :mean_AST_FGM, :decimal
          column :median_AST_FGM, :decimal
          column :total_PCT_AST_FGM, :decimal
          column :mean_PCT_AST_FGM, :decimal
          column :median_PCT_AST_FGM, :decimal
          column :total_UAST_FGM, :decimal
          column :mean_UAST_FGM, :decimal
          column :median_UAST_FGM, :decimal
          column :total_PCT_UAST_FGM, :decimal
          column :mean_PCT_UAST_FGM, :decimal
          column :median_PCT_UAST_FGM, :decimal
          column :PCT_AST_2PM, :decimal
          column :PCT_UAST_2PM, :decimal
          column :PCT_AST_3PM, :decimal
          column :PCT_UAST_3PM, :decimal
          column :PCT_AST_FGM, :decimal
          column :PCT_UAST_FGM, :decimal
          column :PCT_FGA_2PT, :decimal
          column :PCT_PTS_2PT, :decimal
          column :PCT_PTS_3PT, :decimal
          column :PCT_PTS_FB, :decimal
          column :PCT_PTS_FT, :decimal
          column :PCT_PTS_OFF_TOV, :decimal
          column :PCT_PTS_PAINT, :decimal
          
          #tracking
          column :total_DIST, :decimal
          column :mean_DIST, :decimal
          column :median_DIST, :decimal
          column :total_ORBC, :decimal
          column :mean_ORBC, :decimal
          column :median_ORBC, :decimal
          column :total_DRBC, :decimal
          column :mean_DRBC, :decimal
          column :median_DRBC, :decimal
          column :total_RBC, :decimal
          column :mean_RBC, :decimal
          column :median_RBC, :decimal
          column :total_TCHS, :decimal
          column :mean_TCHS, :decimal
          column :median_TCHS, :decimal
          column :total_SAST, :decimal
          column :mean_SAST, :decimal
          column :median_SAST, :decimal
          column :total_FTAST, :decimal
          column :mean_FTAST, :decimal
          column :median_FTAST, :decimal
          column :total_PASS, :decimal
          column :mean_PASS, :decimal
          column :median_PASS, :decimal
          column :total_CFGM, :decimal
          column :mean_CFGM, :decimal
          column :median_CFGM, :decimal
          column :total_CFGA, :decimal
          column :mean_CFGA, :decimal
          column :median_CFGA, :decimal
          column :total_CFG_PCT, :decimal
          column :mean_CFG_PCT, :decimal
          column :median_CFG_PCT, :decimal
          column :total_UFGM, :decimal
          column :mean_UFGM, :decimal
          column :median_UFGM, :decimal
          column :total_UFGA, :decimal
          column :mean_UFGA, :decimal
          column :median_UFGA, :decimal
          column :total_UFG_PCT, :decimal
          column :mean_UFG_PCT, :decimal
          column :median_UFG_PCT, :decimal
          column :total_DFGM, :decimal
          column :mean_DFGM, :decimal
          column :median_DFGM, :decimal
          column :total_DFGA, :decimal
          column :mean_DFGA, :decimal
          column :median_DFGA, :decimal
          column :total_DFG_PCT, :decimal
          column :mean_DFG_PCT, :decimal
          column :median_DFG_PCT, :decimal

          #these are aggregate stats
          column :TS_PCT, :decimal
          column :EFG_PCT, :decimal
          column :PCT_FGA_3PT, :decimal
          column :FTA_RATE, :decimal
          column :total_possessions, :decimal
          column :OREB_PCT, :decimal
          column :DREB_PCT, :decimal
          column :REB_PCT, :decimal
          column :AST_PCT, :decimal
          column :PCT_STL, :decimal
          column :PCT_BLK, :decimal
          column :TO_PCT, :decimal
          column :USG_PCT, :decimal
          column :OFF_RATING, :decimal
          column :floorP, :decimal
          column :DEF_RATING, :decimal
          column :NET_RATING, :decimal
          column :AST_TO, :decimal
          column :AST_RATIO, :decimal
          column :TO_RATIO, :decimal
          column :PACE, :decimal
          column :PIE, :decimal
          #opponent stats
          column :total_o_FGM, :decimal
          column :mean_o_FGM, :decimal
          column :median_o_FGM, :decimal
          column :total_o_FGA, :decimal
          column :mean_o_FGA, :decimal
          column :median_o_FGA, :decimal
          column :total_o_FG_PCT, :decimal
          column :o_FG_PCT, :decimal
          column :total_o_FG3M, :decimal
          column :total_o_FG3A, :decimal
          column :total_o_FG3_PCT, :decimal
          column :o_FG3_PCT, :decimal
          column :total_o_FTM, :decimal
          column :total_o_FTA, :decimal
          column :total_o_FT_PCT, :decimal
          column :o_FT_PCT, :decimal
          column :total_o_OREB, :decimal
          column :total_o_DREB, :decimal
          column :total_o_REB, :decimal
          column :total_o_AST, :decimal
          column :total_o_STL, :decimal
          column :total_o_BLK, :decimal
          column :total_o_TO, :decimal
          column :total_o_PF, :decimal
          column :total_o_PTS, :decimal
          column :total_o_plus_minus, :decimal
          column :total_o_TS_PCT, :decimal
          column :mean_o_TS_PCT, :decimal
          column :median_o_TS_PCT, :decimal
          column :total_o_EFG_PCT, :decimal
          column :mean_o_EFG_PCT, :decimal
          column :median_o_EFG_PCT, :decimal
          column :total_o_PCT_FGA_3PT, :decimal
          column :mean_o_PCT_FGA_3PT, :decimal
          column :median_o_PCT_FGA_3PT, :decimal
          #column :total_o_FG3MAr, :decimal
          #column :mean_o_FG3MAr, :decimal
          #column :median_o_FG3MAr, :decimal
          column :total_o_FTA_RATE, :decimal
          column :mean_o_FTA_RATE, :decimal
          column :median_o_FTA_RATE, :decimal
          column :total_o_OREB_PCT, :decimal
          column :mean_o_OREB_PCT, :decimal
          column :median_o_OREB_PCT, :decimal
          column :total_o_DREB_PCT, :decimal
          column :mean_o_DREB_PCT, :decimal
          column :median_o_DREB_PCT, :decimal
          column :total_o_REB_PCT, :decimal
          column :mean_o_REB_PCT, :decimal
          column :median_o_REB_PCT, :decimal
          column :total_o_AST_PCT, :decimal
          column :mean_o_AST_PCT, :decimal
          column :median_o_AST_PCT, :decimal
          column :total_o_PCT_STL, :decimal
          column :mean_o_PCT_STL, :decimal
          column :median_o_PCT_STL, :decimal
          column :total_o_PCT_BLK, :decimal
          column :mean_o_PCT_BLK, :decimal
          column :median_o_PCT_BLK, :decimal
          column :total_o_TO_PCT, :decimal
          column :mean_o_TO_PCT, :decimal
          column :median_o_TO_PCT, :decimal
          column :total_o_USG_PCT, :decimal
          column :mean_o_USG_PCT, :decimal
          column :median_o_USG_PCT, :decimal
          column :total_o_OFF_RATING, :decimal
          column :mean_o_OFF_RATING, :decimal
          column :median_o_OFF_RATING, :decimal
          column :total_o_DEF_RATING, :decimal
          column :mean_o_DEF_RATING, :decimal
          column :median_o_DEF_RATING, :decimal
          column :total_o_NET_RATING, :decimal
          column :mean_o_NET_RATING, :decimal
          column :median_o_NET_RATING, :decimal
          column :total_o_AST_TO, :decimal
          column :mean_o_AST_TO, :decimal
          column :median_o_AST_TO, :decimal
          column :total_o_AST_RATIO, :decimal
          column :mean_o_AST_RATIO, :decimal
          column :median_o_AST_RATIO, :decimal
          column :total_o_PACE, :decimal
          column :mean_o_PACE, :decimal
          column :median_o_PACE, :decimal
          column :total_o_PIE, :decimal
          column :mean_o_PIE, :decimal
          column :median_o_PIE, :decimal
          column :mean_o_FG_PCT, :decimal
          column :median_o_FG_PCT, :decimal
          column :mean_o_FG3M, :decimal
          column :median_o_FG3M, :decimal
          column :mean_o_FG3A, :decimal
          column :median_o_FG3A, :decimal
          column :mean_o_FG3_PCT, :decimal
          column :median_o_FG3_PCT, :decimal
          column :mean_o_FTM, :decimal
          column :median_o_FTM, :decimal
          column :mean_o_FTA, :decimal
          column :median_o_FTA, :decimal
          column :mean_o_FT_PCT, :decimal
          column :median_o_FT_PCT, :decimal
          column :mean_o_OREB, :decimal
          column :median_o_OREB, :decimal
          column :mean_o_DREB, :decimal
          column :median_o_DREB, :decimal
          column :mean_o_REB, :decimal
          column :median_o_REB, :decimal
          column :mean_o_AST, :decimal
          column :median_o_AST, :decimal
          column :mean_o_STL, :decimal
          column :median_o_STL, :decimal
          column :mean_o_BLK, :decimal
          column :median_o_BLK, :decimal
          column :mean_o_TO, :decimal
          column :median_o_TO, :decimal
          column :mean_o_PF, :decimal
          column :median_o_PF, :decimal
          column :mean_o_PTS, :decimal
          column :median_o_PTS, :decimal
          column :mean_o_plus_minus, :decimal
          column :median_o_plus_minus, :decimal
          #misc
          column :total_o_PTS_OFF_TOV, :decimal
          column :mean_o_PTS_OFF_TOV, :decimal
          column :median_o_PTS_OFF_TOV, :decimal
          column :total_o_PTS_2ND_CHANCE, :decimal
          column :mean_o_PTS_2ND_CHANCE, :decimal
          column :median_o_PTS_2ND_CHANCE, :decimal
          column :total_o_PTS_FB, :decimal
          column :mean_o_PTS_FB, :decimal
          column :median_o_PTS_FB, :decimal
          column :total_o_PTS_PAINT, :decimal
          column :mean_o_PTS_PAINT, :decimal
          column :median_o_PTS_PAINT, :decimal
          #scoring
          column :total_o_PCT_FGA_2PT, :decimal
          column :mean_o_PCT_FGA_2PT, :decimal
          column :median_o_PCT_FGA_2PT, :decimal
          column :total_o_PCT_PTS_2PT, :decimal
          column :mean_o_PCT_PTS_2PT, :decimal
          column :median_o_PCT_PTS_2PT, :decimal
          column :total_o_PCT_PTS_2PT_MR, :decimal
          column :mean_o_PCT_PTS_2PT_MR, :decimal
          column :median_o_PCT_PTS_2PT_MR, :decimal
          column :total_o_PCT_PTS_3PT, :decimal
          column :mean_o_PCT_PTS_3PT, :decimal
          column :median_o_PCT_PTS_3PT, :decimal
          column :total_o_PCT_PTS_FB, :decimal
          column :mean_o_PCT_PTS_FB, :decimal
          column :median_o_PCT_PTS_FB, :decimal
          column :total_o_PCT_PTS_FT, :decimal
          column :mean_o_PCT_PTS_FT, :decimal
          column :median_o_PCT_PTS_FT, :decimal
          column :total_o_PCT_PTS_OFF_TOV, :decimal
          column :mean_o_PCT_PTS_OFF_TOV, :decimal
          column :median_o_PCT_PTS_OFF_TOV, :decimal
          column :total_o_PCT_PTS_PAINT, :decimal
          column :mean_o_PCT_PTS_PAINT, :decimal
          column :median_o_PCT_PTS_PAINT, :decimal
          column :total_o_AST_2PM, :decimal
          column :mean_o_AST_2PM, :decimal
          column :median_o_AST_2PM, :decimal
          column :total_o_PCT_AST_2PM, :decimal
          column :mean_o_PCT_AST_2PM, :decimal
          column :median_o_PCT_AST_2PM, :decimal
          column :total_o_UAST_2PM, :decimal
          column :mean_o_UAST_2PM, :decimal
          column :median_o_UAST_2PM, :decimal
          column :total_o_PCT_UAST_2PM, :decimal
          column :mean_o_PCT_UAST_2PM, :decimal
          column :median_o_PCT_UAST_2PM, :decimal
          column :total_o_AST_3PM, :decimal
          column :mean_o_AST_3PM, :decimal
          column :median_o_AST_3PM, :decimal
          column :total_o_PCT_AST_3PM, :decimal
          column :mean_o_PCT_AST_3PM, :decimal
          column :median_o_PCT_AST_3PM, :decimal
          column :total_o_UAST_3PM, :decimal
          column :mean_o_UAST_3PM, :decimal
          column :median_o_UAST_3PM, :decimal
          column :total_o_PCT_UAST_3PM, :decimal
          column :mean_o_PCT_UAST_3PM, :decimal
          column :median_o_PCT_UAST_3PM, :decimal
          column :total_o_AST_FGM, :decimal
          column :mean_o_AST_FGM, :decimal
          column :median_o_AST_FGM, :decimal
          column :total_o_PCT_AST_FGM, :decimal
          column :mean_o_PCT_AST_FGM, :decimal
          column :median_o_PCT_AST_FGM, :decimal
          column :total_o_UAST_FGM, :decimal
          column :mean_o_UAST_FGM, :decimal
          column :median_o_UAST_FGM, :decimal
          column :total_o_PCT_UAST_FGM, :decimal
          column :mean_o_PCT_UAST_FGM, :decimal
          column :median_o_PCT_UAST_FGM, :decimal

          column :o_PCT_AST_2PM, :decimal
          column :o_PCT_UAST_2PM, :decimal
          column :o_PCT_AST_3PM, :decimal
          column :o_PCT_UAST_3PM, :decimal
          column :o_PCT_AST_FGM, :decimal
          column :o_PCT_UAST_FGM, :decimal
          column :o_PCT_FGA_2PT, :decimal
          column :o_PCT_PTS_2PT, :decimal
          column :o_PCT_PTS_3PT, :decimal
          column :o_PCT_PTS_FB, :decimal
          column :o_PCT_PTS_FT, :decimal
          column :o_PCT_PTS_OFF_TOV, :decimal
          column :o_PCT_PTS_PAINT, :decimal
          #tracking
          column :total_o_DIST, :decimal
          column :mean_o_DIST, :decimal
          column :median_o_DIST, :decimal
          column :total_o_ORBC, :decimal
          column :mean_o_ORBC, :decimal
          column :median_o_ORBC, :decimal
          column :total_o_DRBC, :decimal
          column :mean_o_DRBC, :decimal
          column :median_o_DRBC, :decimal
          column :total_o_RBC, :decimal
          column :mean_o_RBC, :decimal
          column :median_o_RBC, :decimal
          column :total_o_TCHS, :decimal
          column :mean_o_TCHS, :decimal
          column :median_o_TCHS, :decimal
          column :total_o_SAST, :decimal
          column :mean_o_SAST, :decimal
          column :median_o_SAST, :decimal
          column :total_o_FTAST, :decimal
          column :mean_o_FTAST, :decimal
          column :median_o_FTAST, :decimal
          column :total_o_PASS, :decimal
          column :mean_o_PASS, :decimal
          column :median_o_PASS, :decimal
          column :total_o_CFGM, :decimal
          column :mean_o_CFGM, :decimal
          column :median_o_CFGM, :decimal
          column :total_o_CFGA, :decimal
          column :mean_o_CFGA, :decimal
          column :median_o_CFGA, :decimal
          column :total_o_CFG_PCT, :decimal
          column :mean_o_CFG_PCT, :decimal
          column :median_o_CFG_PCT, :decimal
          column :total_o_UFGM, :decimal
          column :mean_o_UFGM, :decimal
          column :median_o_UFGM, :decimal
          column :total_o_UFGA, :decimal
          column :mean_o_UFGA, :decimal
          column :median_o_UFGA, :decimal
          column :total_o_UFG_PCT, :decimal
          column :mean_o_UFG_PCT, :decimal
          column :median_o_UFG_PCT, :decimal
          column :total_o_DFGM, :decimal
          column :mean_o_DFGM, :decimal
          column :median_o_DFGM, :decimal
          column :total_o_DFGA, :decimal
          column :mean_o_DFGA, :decimal
          column :median_o_DFGA, :decimal
          column :total_o_DFG_PCT, :decimal
          column :mean_o_DFG_PCT, :decimal
          column :median_o_DFG_PCT, :decimal
          #these are aggregate stats
          column :o_TS_PCT, :decimal
          column :o_EFG_PCT, :decimal
          column :o_PCT_FGA_3PT, :decimal
          column :o_FTA_RATE, :decimal
          column :total_o_possessions, :decimal
          column :o_OREB_PCT, :decimal
          column :o_DREB_PCT, :decimal
          column :o_REB_PCT, :decimal
          column :o_AST_PCT, :decimal
          column :o_PCT_STL, :decimal
          column :o_PCT_BLK, :decimal
          column :o_TO_PCT, :decimal
          column :o_USG_PCT, :decimal
          column :o_OFF_RATING, :decimal
          column :o_DEF_RATING, :decimal
          column :o_NET_RATING, :decimal
          column :o_AST_TO, :decimal
          column :o_AST_RATIO, :decimal
          column :o_TO_RATIO, :decimal
          column :o_PACE, :decimal
          column :o_PIE, :decimal
        end

        season_str = season.gsub(/-/,"_") + "_regularseason"

        season_start = Date.parse( seasons_h[season][0] )
        season_end = Date.parse( seasons_h[season][1] )

#0021400307_advanced_PlayerStats 
#GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,START_POSITION,COMMENT,MIN,OFF_RATING,DEF_RATING,NET_RATING,AST_PCT,AST_TO,AST_RATIO,OREB_PCT,DREB_PCT,REB_PCT,TM_TO_PCT,EFG_PCT,TS_PCT,USG_PCT,PACE,PIE
#0021400307_advanced_TeamStats 
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,OFF_RATING,DEF_RATING,NET_RATING,AST_PCT,AST_TO,AST_RATIO,OREB_PCT,DREB_PCT,REB_PCT,TM_TO_PCT,EFG_PCT,TS_PCT,USG_PCT,PACE,PIE
#0021400307_fourfactors_sqlPlayersFourFactors 
#GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,START_POSITION,COMMENT,MIN,EFG_PCT,FTA_RATE,TM_TO_PCT,OREB_PCT,OPP_EFG_PCT,OPP_FTA_RATE,OPP_TO_PCT,OPP_OREB_PCT
#0021400307_fourfactors_sqlTeamsFourFactors 
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,EFG_PCT,FTA_RATE,TM_TO_PCT,OREB_PCT,OPP_EFG_PCT,OPP_FTA_RATE,OPP_TO_PCT,OPP_OREB_PCT
#0021400307_misc_sqlPlayersMisc 
#GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,START_POSITION,COMMENT,MIN,PTS_OFF_TO,PTS_2ND_CHANCE,PTS_FB,PTS_PAINT,OPP_PTS_OFF_TO,OPP_PTS_2ND_CHANCE,OPP_PTS_FB,OPP_PTS_PAINT,BLK,BLKA,PF,PFD
#0021400307_misc_sqlTeamsMisc
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,PTS_OFF_TO,PTS_2ND_CHANCE,PTS_FB,PTS_PAINT,OPP_PTS_OFF_TO,OPP_PTS_2ND_CHANCE,OPP_PTS_FB,OPP_PTS_PAINT,BLK,BLKA,PF,PFD
#0021400307_playertrack_PlayerTrack
#GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,START_POSITION,COMMENT,MIN,SPD,DIST,ORBC,DRBC,RBC,TCHS,SAST,FTAST,PASS,AST,CFGM,CFGA,CFG_PCT,UFGM,UFGA,UFG_PCT,FG_PCT,DFGM,DFGA,DFG_PCT
#0021400307_playertrack_PlayerTrackTeam
#GAME_ID,TEAM_ID,TEAM_NICKNAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,DIST,ORBC,DRBC,RBC,TCHS,SAST,FTAST,PASS,AST,CFGM,CFGA,CFG_PCT,UFGM,UFGA,UFG_PCT,FG_PCT,DFGM,DFGA,DFG_PCT
#0021400307_scoring_sqlPlayersScoring
#GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,START_POSITION,COMMENT,MIN,PCT_FGA_2PT,PCT_FGA_3PT,PCT_PTS_2PT,PCT_PTS_2PT_MR,PCT_PTS_3PT,PCT_PTS_FB,PCT_PTS_FT,PCT_PTS_OFF_TO,PCT_PTS_PAINT,PCT_AST_2PM,PCT_UAST_2PM,PCT_AST_3PM,PCT_UAST_3PM,PCT_AST_FGM,PCT_UAST_FGM
#0021400307_scoring_sqlTeamsScoring
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,PCT_FGA_2PT,PCT_FGA_3PT,PCT_PTS_2PT,PCT_PTS_2PT_MR,PCT_PTS_3PT,PCT_PTS_FB,PCT_PTS_FT,PCT_PTS_OFF_TO,PCT_PTS_PAINT,PCT_AST_2PM,PCT_UAST_2PM,PCT_AST_3PM,PCT_UAST_3PM,PCT_AST_FGM,PCT_UAST_FGM
#0021400307_traditional_PlayerStats
#GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,START_POSITION,COMMENT,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,FTM,FTA,FT_PCT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS,PLUS_MINUS
#0021400307_traditional_TeamStarterBenchStats
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,STARTERS_BENCH,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,FTM,FTA,FT_PCT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS
#0021400307_traditional_TeamStats
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,FTM,FTA,FT_PCT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS,PLUS_MINUS
#0021400307_usage_sqlPlayersUsage
#GAME_ID,TEAM_ID,TEAM_ABBREVIATION,TEAM_CITY,PLAYER_ID,PLAYER_NAME,START_POSITION,COMMENT,MIN,USG_PCT,PCT_FGM,PCT_FGA,PCT_FG3M,PCT_FG3A,PCT_FTM,PCT_FTA,PCT_OREB,PCT_DREB,PCT_REB,PCT_AST,PCT_TO,PCT_STL,PCT_BLK,PCT_BLKA,PCT_PF,PCT_PFD,PCT_PTS
#0021400307_usage_sqlTeamsUsage
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,USG_PCT,PCT_FGM,PCT_FGA,PCT_FG3M,PCT_FG3A,PCT_FTM,PCT_FTA,PCT_OREB,PCT_DREB,PCT_REB,PCT_AST,PCT_TO,PCT_STL,PCT_BLK,PCT_BLKA,PCT_PF,PCT_PFD,PCT_PTS



#0021400307_traditional_TeamStarterBenchStats
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,STARTERS_BENCH,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,FTM,FTA,FT_PCT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS
#0021400307_traditional_TeamStats
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,FGM,FGA,FG_PCT,FG3M,FG3A,FG3_PCT,FTM,FTA,FT_PCT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS,PLUS_MINUS
#0021400307_advanced_TeamStats 
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,OFF_RATING,DEF_RATING,NET_RATING,AST_PCT,AST_TOV,AST_RATIO,OREB_PCT,DREB_PCT,REB_PCT,TM_TOV_PCT,EFG_PCT,TS_PCT,USG_PCT,PACE,PIE
#0021400307_fourfactors_sqlTeamsFourFactors 
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,EFG_PCT,FTA_RATE,TM_TOV_PCT,OREB_PCT,OPP_EFG_PCT,OPP_FTA_RATE,OPP_TOV_PCT,OPP_OREB_PCT
#0021400307_misc_sqlTeamsMisc
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,PTS_OFF_TOV,PTS_2ND_CHANCE,PTS_FB,PTS_PAINT,OPP_PTS_OFF_TOV,OPP_PTS_2ND_CHANCE,OPP_PTS_FB,OPP_PTS_PAINT,BLK,BLKA,PF,PFD
#0021400307_playertrack_PlayerTrackTeam
#GAME_ID,TEAM_ID,TEAM_NICKNAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,DIST,ORBC,DRBC,RBC,TCHS,SAST,FTAST,PASS,AST,CFGM,CFGA,CFG_PCT,UFGM,UFGA,UFG_PCT,FG_PCT,DFGM,DFGA,DFG_PCT
#0021400307_scoring_sqlTeamsScoring
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,PCT_FGA_2PT,PCT_FGA_3PT,PCT_PTS_2PT,PCT_PTS_2PT_MR,PCT_PTS_3PT,PCT_PTS_FB,PCT_PTS_FT,PCT_PTS_OFF_TOV,PCT_PTS_PAINT,PCT_AST_2PM,PCT_UAST_2PM,PCT_AST_3PM,PCT_UAST_3PM,PCT_AST_FGM,PCT_UAST_FGM
#0021400307_usage_sqlTeamsUsage
#GAME_ID,TEAM_ID,TEAM_NAME,TEAM_ABBREVIATION,TEAM_CITY,MIN,USG_PCT,PCT_FGM,PCT_FGA,PCT_FG3M,PCT_FG3A,PCT_FTM,PCT_FTA,PCT_OREB,PCT_DREB,PCT_REB,PCT_AST,PCT_TOV,PCT_STL,PCT_BLK,PCT_BLKA,PCT_PF,PCT_PFD,PCT_PTS
#http://stats.nba.com/stats/leaguegamelog?Counter=1000&Direction=DESC&LeagueID=00&PlayerOrTeam=T&Season=2014-15&SeasonType=Regular+Season&Sorter=PTS

          #DB[season_str.to_sym].where(:Player => name).where(:Position => "PG").order(:GAME_ID).entries.each_with_index |boxscore,boxscore_index|
        teams = DB[ :"#{type}_#{team_tables[6]}" ].where(:SEASON => season).distinct.select(:TEAM_ABBREVIATION).entries
        players = DB[ :"#{type}_#{player_tables[5]}" ].where(:SEASON => season).distinct.select(:PLAYER_NAME).entries
        binding.pry
        bTeam = false
        bTeam = true 
        teams = teams + players
        #players.each{|team|
        teams.each{|team|
          if team[:PLAYER_NAME]
            bTeam = false
          end
          cur_date = season_start

          last_boxscore = Hash.new

          games_played = 0; games_started = 0; total_wins = 0; total_losses = 0; total_ties = 0; total_time_played = Duration.new; h_mins_played = Hash.new; h_FGM = Hash.new; h_FGA = Hash.new; h_FG_PCT = Hash.new; h_FG3M = Hash.new; h_FG3A = Hash.new; h_FG3_PCT = Hash.new; h_FTM = Hash.new; h_FTA = Hash.new; h_FT_PCT = Hash.new; h_OREB = Hash.new; h_DREB = Hash.new; h_REB = Hash.new; h_AST = Hash.new; h_STL = Hash.new; h_BLK = Hash.new; h_TO = Hash.new; h_PF = Hash.new; h_PTS = Hash.new; h_plus_minus = Hash.new; h_TS_PCT = Hash.new; h_EFG_PCT = Hash.new; h_PCT_FGA_3PT = Hash.new; h_FTA_RATE = Hash.new; h_OREB_PCT = Hash.new; h_DREB_PCT = Hash.new; h_REB_PCT = Hash.new; h_AST_PCT = Hash.new; h_PCT_STL = Hash.new; h_PCT_BLK = Hash.new; h_TO_PCT = Hash.new; h_USG_PCT = Hash.new; h_OFF_RATING = Hash.new; h_DEF_RATING = Hash.new; h_NET_RATING = Hash.new; h_AST_TO = Hash.new; h_AST_RATIO = Hash.new; h_PACE = Hash.new; h_PIE = Hash.new; h_PTS_OFF_TOV = Hash.new; h_PTS_2ND_CHANCE = Hash.new; h_PTS_FB = Hash.new; h_PTS_PAINT = Hash.new; h_DIST = Hash.new; h_ORBC = Hash.new; h_DRBC = Hash.new; h_RBC = Hash.new; h_TCHS = Hash.new; h_SAST = Hash.new; h_FTAST = Hash.new; h_PASS = Hash.new; h_AST = Hash.new; h_CFGM = Hash.new; h_CFGA = Hash.new; h_CFG_PCT = Hash.new; h_UFGM = Hash.new; h_UFGA = Hash.new; h_UFG_PCT = Hash.new; h_DFGM = Hash.new; h_DFGA = Hash.new; h_DFG_PCT = Hash.new; h_PCT_FGA_2PT = Hash.new; h_PCT_PTS_2PT = Hash.new; h_PCT_PTS_2PT_MR = Hash.new; h_PCT_PTS_3PT = Hash.new; h_PCT_PTS_FB = Hash.new; h_PCT_PTS_FT = Hash.new; h_PCT_PTS_OFF_TOV = Hash.new; h_PCT_PTS_PAINT = Hash.new; h_AST_2PM = Hash.new; h_PCT_AST_2PM = Hash.new; h_UAST_2PM = Hash.new; h_PCT_UAST_2PM = Hash.new; h_AST_3PM = Hash.new; h_PCT_AST_3PM = Hash.new; h_UAST_3PM = Hash.new; h_PCT_UAST_3PM = Hash.new; h_AST_FGM = Hash.new; h_PCT_AST_FGM = Hash.new; h_UAST_FGM = Hash.new; h_PCT_UAST_FGM = Hash.new

            total_FGM = 0; total_FGA = 0; total_FG_PCT = 0; total_FG3M = 0; total_FG3A = 0; total_FG3_PCT = 0; total_FTM = 0; total_FTA = 0; total_FTA = 0; total_FT_PCT = 0; total_OREB = 0; total_DREB = 0; total_REB = 0; total_AST = 0; total_STL = 0; total_BLK = 0; total_TO = 0; total_PF = 0; total_PTS = 0; total_plus_minus = 0; total_TS_PCT = 0; total_EFG_PCT = 0; total_PCT_FGA_3PT = 0; total_FTA_RATE = 0; total_OREB_PCT = 0; total_DREB_PCT = 0; total_REB_PCT = 0; total_AST_PCT = 0; total_PCT_STL = 0; total_PCT_BLK = 0; total_TO_PCT = 0; total_USG_PCT = 0; total_OFF_RATING = 0; total_DEF_RATING = 0; total_NET_RATING = 0; total_AST_TO = 0; total_AST_RATIO = 0; total_PACE = 0; total_PIE = 0; total_PTS_OFF_TOV = 0; total_PTS_2ND_CHANCE = 0; total_PTS_FB = 0; total_PTS_PAINT = 0; total_DIST = 0; total_ORBC = 0;total_DRBC = 0;total_RBC = 0;total_TCHS = 0;total_SAST = 0;total_FTAST = 0;total_PASS = 0;total_AST = 0;total_CFGM = 0;total_CFGA = 0;total_CFG_PCT = 0;total_UFGM = 0;total_UFGA = 0;total_UFG_PCT = 0;total_DFGM = 0;total_DFGA = 0; total_DFG_PCT = 0; total_PCT_FGA_2PT = 0; total_PCT_PTS_2PT = 0; total_PCT_PTS_2PT_MR = 0; total_PCT_PTS_3PT = 0; total_PCT_PTS_FB = 0; total_PCT_PTS_FT = 0; total_PCT_PTS_OFF_TOV = 0; total_PCT_PTS_PAINT = 0; total_AST_2PM = 0; total_PCT_AST_2PM = 0; total_UAST_2PM = 0; total_PCT_UAST_2PM = 0; total_AST_3PM = 0; total_PCT_AST_3PM = 0; total_UAST_3PM = 0; total_PCT_UAST_3PM = 0; total_AST_FGM = 0; total_PCT_AST_FGM = 0; total_UAST_FGM = 0; total_PCT_UAST_FGM = 0;

            h_o_FGM = Hash.new; h_o_FGA = Hash.new; h_o_FG_PCT = Hash.new; h_o_FG3M = Hash.new; h_o_FG3A = Hash.new; h_o_FG3_PCT = Hash.new; h_o_FTM = Hash.new; h_o_FTA = Hash.new; h_o_FT_PCT = Hash.new; h_o_OREB = Hash.new; h_o_DREB = Hash.new; h_o_REB = Hash.new; h_o_AST = Hash.new; h_o_STL = Hash.new; h_o_BLK = Hash.new; h_o_TO = Hash.new; h_oPF = Hash.new; h_oPTS = Hash.new; h_o_plus_minus = Hash.new; h_o_TS_PCT = Hash.new; h_o_EFG_PCT = Hash.new; h_o_PCT_FGA_3PT = Hash.new; h_o_FTA_RATE = Hash.new; h_o_OREB_PCT = Hash.new; h_o_DREB_PCT = Hash.new; h_o_REB_PCT = Hash.new; h_o_AST_PCT = Hash.new; h_o_PCT_STL = Hash.new; h_o_PCT_BLK = Hash.new; h_o_TO_PCT = Hash.new; h_o_USG_PCT = Hash.new; h_o_OFF_RATING = Hash.new; h_o_DEF_RATING = Hash.new; h_o_NET_RATING = Hash.new; h_o_AST_TO = Hash.new; h_o_AST_RATIO = Hash.new; h_o_PACE = Hash.new; h_o_PIE = Hash.new; h_o_PTS_OFF_TOV = Hash.new; h_o_PTS_2ND_CHANCE = Hash.new; h_o_PTS_FB = Hash.new; h_o_PTS_PAINT = Hash.new; h_o_DIST = Hash.new; h_o_ORBC = Hash.new; h_o_DRBC = Hash.new; h_o_RBC = Hash.new; h_o_TCHS = Hash.new; h_o_SAST = Hash.new; h_o_FTAST = Hash.new; h_o_PASS = Hash.new; h_o_AST = Hash.new; h_o_CFGM = Hash.new; h_o_CFGA = Hash.new; h_o_CFG_PCT = Hash.new; h_o_UFGM = Hash.new; h_o_UFGA = Hash.new; h_o_UFG_PCT = Hash.new; h_o_DFGM = Hash.new; h_o_DFGA = Hash.new; h_o_DFG_PCT = Hash.new; h_o_PCT_FGA_2PT = Hash.new; h_o_PCT_PTS_2PT = Hash.new; h_o_PCT_PTS_2PT_MR = Hash.new; h_o_PCT_PTS_3PT = Hash.new; h_o_PCT_PTS_FB = Hash.new; h_o_PCT_PTS_FT = Hash.new; h_o_PCT_PTS_OFF_TOV = Hash.new; h_o_PCT_PTS_PAINT = Hash.new; h_o_AST_2PM = Hash.new; h_o_PCT_AST_2PM = Hash.new; h_o_UAST_2PM = Hash.new; h_o_PCT_UAST_2PM = Hash.new; h_o_AST_3PM = Hash.new; h_o_PCT_AST_3PM = Hash.new; h_o_UAST_3PM = Hash.new; h_o_PCT_UAST_3PM = Hash.new; h_o_AST_FGM = Hash.new; h_o_PCT_AST_FGM = Hash.new; h_o_UAST_FGM = Hash.new; h_o_PCT_UAST_FGM = Hash.new

            total_o_FGM = 0; total_o_FGA = 0; total_o_FG_PCT = 0; total_o_FG3M = 0; total_o_FG3A = 0; total_o_FG3_PCT = 0; total_o_FTM = 0; total_o_FTA = 0; total_o_FT_PCT = 0; total_o_OREB = 0; total_o_DREB = 0; total_o_REB = 0; total_o_AST = 0; total_o_STL = 0; total_o_BLK = 0; total_o_TO = 0; total_o_PF = 0; total_o_PTS = 0; total_o_plus_minus = 0; total_o_TS_PCT = 0; total_o_EFG_PCT = 0; total_o_PCT_FGA_3PT = 0; total_o_FTA_RATE = 0; total_o_OREB_PCT = 0; total_o_DREB_PCT = 0; total_o_REB_PCT = 0; total_o_AST_PCT = 0; total_o_PCT_STL = 0; total_o_PCT_BLK = 0; total_o_TO_PCT = 0; total_o_USG_PCT = 0; total_o_OFF_RATING = 0; total_o_DEF_RATING = 0; total_o_NET_RATING = 0; total_o_AST_TO = 0; total_o_AST_RATIO = 0; total_o_PACE = 0; total_o_PIE = 0; total_o_PTS_OFF_TOV = 0; total_o_PTS_2ND_CHANCE = 0; total_o_PTS_FB = 0; total_o_PTS_PAINT = 0; total_o_DIST = 0; total_o_ORBC = 0;total_o_DRBC = 0;total_o_RBC = 0;total_o_TCHS = 0;total_o_SAST = 0;total_o_FTAST = 0;total_o_PASS = 0;total_o_AST = 0;total_o_CFGM = 0;total_o_CFGA = 0;total_o_CFG_PCT = 0;total_o_UFGM = 0;total_o_UFGA = 0;total_o_UFG_PCT = 0;total_o_DFGM = 0;total_o_DFGA = 0;total_o_DFG_PCT = 0; total_o_PCT_FGA_2PT = 0; total_o_PCT_PTS_2PT = 0; total_o_PCT_PTS_2PT_MR = 0; total_o_PCT_PTS_3PT = 0; total_o_PCT_PTS_FB = 0; total_o_PCT_PTS_FT = 0; total_o_PCT_PTS_OFF_TOV = 0; total_o_PCT_PTS_PAINT = 0; total_o_AST_2PM = 0; total_o_PCT_AST_2PM = 0; total_o_UAST_2PM = 0; total_o_PCT_UAST_2PM = 0; total_o_AST_3PM = 0; total_o_PCT_AST_3PM = 0; total_o_UAST_3PM = 0; total_o_PCT_UAST_3PM = 0; total_o_AST_FGM = 0; total_o_PCT_AST_FGM = 0; total_o_UAST_FGM = 0; total_o_PCT_UAST_FGM = 0;

  #player_tables = [ "advanced_PlayerStats", "fourfactors_sqlPlayersFourFactors", "misc_sqlPlayersMisc", "playertrack_PlayerTrack", "scoring_sqlPlayersScoring", "traditional_PlayerStats", "usage_sqlPlayersUsage" ]
            if true == bTeam
              boxscores = DB[ :"#{type}_traditional_TeamStats" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).where(:SEASON => season).order(:GAME_ID).entries
            else
              boxscores = DB[ :"#{type}_traditional_PlayerStats" ].where(:PLAYER_NAME => team[:PLAYER_NAME]).where(:SEASON => season).order(:GAME_ID).entries
            end

            num_boxscores = boxscores.size
            boxscores.each_with_index{|boxscore_traditional,i|
              p "#{i} / #{num_boxscores}"

              if true == bTeam
                gamelog = DB[ :"#{type}_gamelogs" ].where(:TEAM_ID => boxscore_traditional[:TEAM_ID]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_advanced = DB[ :"#{type}_advanced_TeamStats" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_fourfactors = DB[ :"#{type}_fourfactors_sqlTeamsFourFactors" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_scoring = DB[ :"#{type}_scoring_sqlTeamsScoring" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_usage = DB[ :"#{type}_usage_sqlTeamsUsage" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_misc = DB[ :"#{type}_misc_sqlTeamsMisc" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_tracking = DB[ :"#{type}_playertrack_PlayerTrackTeam" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_traditional_starters = DB[ :"#{type}_traditional_TeamStarterBenchStats" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_traditional_bench = DB[ :"#{type}_traditional_TeamStarterBenchStats" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[1]
              else
                gamelog = DB[ :"#{type}_player_gamelogs" ].where(:PLAYER_ID => boxscore_traditional[:PLAYER_ID]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_advanced = DB[ :"#{type}_advanced_PlayerStats" ].where(:PLAYER_NAME => team[:PLAYER_NAME]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_fourfactors = DB[ :"#{type}_fourfactors_sqlPlayersFourFactors" ].where(:PLAYER_NAME => team[:PLAYER_NAME]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_scoring = DB[ :"#{type}_scoring_sqlPlayersScoring" ].where(:PLAYER_NAME => team[:PLAYER_NAME]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_usage = DB[ :"#{type}_usage_sqlPlayersUsage" ].where(:PLAYER_NAME => team[:PLAYER_NAME]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_misc = DB[ :"#{type}_misc_sqlPlayersMisc" ].where(:PLAYER_NAME => team[:PLAYER_NAME]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                boxscore_tracking = DB[ :"#{type}_playertrack_PlayerTrack" ].where(:PLAYER_NAME => team[:PLAYER_NAME]).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
              end

              row = Hash.new
              row[:team_abbreviation] = boxscore_traditional[:TEAM_ABBREVIATION]
              if false == bTeam
                row[:player_name] = team[:PLAYER_NAME]
              end
              row[:date] = Date.parse( boxscore_traditional[:DATE] )
              row[:date_of_data] = Date.parse( boxscore_traditional[:DATE] )

=begin
#jlk - we need to calculate splits for: home/away games, back-to-backs, lots of rest, 3 in 4 nights, 5 in 7, etc., recent performance (last 3g, 5g, 10g)
              if 2 == gamelog[:MATCHUP].split("@").size
                if gamelog[:MATCHUP].split("@")[0].match /boxscore_traditional[:TEAM_NAME]/

              elsif 2 == gamelog[:MATCHUP].split("vs.").size
              else
                binding.pry
                p "more than 2 arguments"
              end
=end

              #For dates without games, just put a stub entry pointing to the last valid boxscore date, so we don't repeat data
              #cur_date + 1 b/c we are going to update cur_date in this iteration
              last_boxscore_date = last_boxscore[:date]

              while cur_date + 1 < row[:date]
                cur_date = cur_date + 1

                last_boxscore[:date] = cur_date
                last_boxscore[:date_of_data] = last_boxscore_date

                database[tablename].insert(last_boxscore.to_hash)
              end

              time_played = Duration.new( :minutes => gamelog[:MIN].split(":")[0], :seconds => gamelog[:MIN].split(":")[1] )
              total_time_played = total_time_played + time_played
              games_played = games_played + 1

              row[:games_played] = games_played

              if true == bTeam
                if gamelog[:WL] == "W"
                  total_wins = total_wins + 1
                  row[:wins] = total_wins
                  row[:losses] = total_losses
                elsif gamelog[:WL] == "L"
                  total_losses = total_losses + 1
                  row[:losses] = total_losses
                  row[:wins] = total_wins
                else
                  binding.pry
                  total_ties = total_ties + 1
                  row[:ties] = total_ties
                  row[:wins] = total_wins
                  row[:losses] = total_losses
                end

                row[:win_pct] = total_wins / (games_played - total_ties)
              end

              row[:total_mins_played] = total_time_played.total_minutes
              row[:mean_mins_played] = games_played == 0 ? 0 : (total_time_played.total_minutes / games_played)
              h_mins_played[ boxscore_traditional[:GAME_ID] ] = time_played
              row[:median_mins_played] = median( h_mins_played ).strftime("%H:%M:%S")

              if "0021400072" == boxscore_traditional[:GAME_ID] || "0021400748" == boxscore_traditional[:GAME_ID] || "0021400842" == boxscore_traditional[:GAME_ID]
                #binding.pry
                p "overtime game"
              end
              total_FGM, row[:mean_FGM] = mean( gamelog[:FGM].to_f, total_FGM, games_played )
              h_FGM[ boxscore_traditional[:GAME_ID] ] = gamelog[:FGM].to_f
              row[:median_FGM] = median( h_FGM )
              row[:total_FGM] = total_FGM

              total_FGA, row[:mean_FGA] = mean( gamelog[:FGA].to_f, total_FGA, games_played )
              h_FGA[ boxscore_traditional[:GAME_ID] ] = gamelog[:FGA].to_f
              row[:median_FGA] = median( h_FGA )
              row[:total_FGA] = total_FGA

              total_FG_PCT, row[:mean_FG_PCT] = mean( gamelog[:FG_PCT].to_f, total_FG_PCT, games_played )
              h_FG_PCT[ boxscore_traditional[:GAME_ID] ] = gamelog[:FG_PCT].to_f
              row[:median_FG_PCT] = median( h_FG_PCT )
              row[:total_FG_PCT] = total_FG_PCT
              row[:FG_PCT] = total_FGM / total_FGA

              total_FG3M, row[:mean_FG3M] = mean( gamelog[:FG3M].to_f, total_FG3M, games_played )
              h_FG3M[ boxscore_traditional[:GAME_ID] ] = gamelog[:FG3M].to_f
              row[:median_FG3M] = median( h_FG3M )
              row[:total_FG3M] = total_FG3M

              total_FG3A, row[:mean_FG3A] = mean( gamelog[:FG3A].to_f, total_FG3A, games_played )
              h_FG3A[ boxscore_traditional[:GAME_ID] ] = gamelog[:FG3A].to_f
              row[:median_FG3A] = median( h_FG3A )
              row[:total_FG3A] = total_FG3A

              total_FG3_PCT, row[:mean_FG3_PCT] = mean( gamelog[:FG3_PCT].to_f, total_FG3_PCT, games_played )
              h_FG3_PCT[ boxscore_traditional[:GAME_ID] ] = gamelog[:FG3_PCT].to_f
              row[:median_FG3_PCT] = median( h_FG3_PCT )
              row[:total_FG3_PCT] = total_FG3_PCT
              if 0 == total_FG3A
                row[:FG3_PCT] = 0
              else
                row[:FG3_PCT] = total_FG3M / total_FG3A
              end

              total_FTM, row[:mean_FTM] = mean( gamelog[:FTM].to_f, total_FTM, games_played )
              h_FTM[ boxscore_traditional[:GAME_ID] ] = gamelog[:FTM].to_f
              row[:median_FTM] = median( h_FTM )
              row[:total_FTM] = total_FTM

              total_FTA, row[:mean_FTA] = mean( gamelog[:FTA].to_f, total_FTA, games_played )
              h_FTA[ boxscore_traditional[:GAME_ID] ] = gamelog[:FTA].to_f
              row[:median_FTA] = median( h_FTA )
              row[:total_FTA] = total_FTA

              total_FT_PCT, row[:mean_FT_PCT] = mean( gamelog[:FT_PCT].to_f, total_FT_PCT, games_played )
              h_FT_PCT[ boxscore_traditional[:GAME_ID] ] = gamelog[:FT_PCT].to_f
              row[:median_FT_PCT] = median( h_FT_PCT )
              row[:total_FT_PCT] = total_FT_PCT
              if 0 == total_FTA
                row[:FT_PCT] = 0
              else
                row[:FT_PCT] = total_FTM / total_FTA
              end

              total_OREB, row[:mean_OREB] = mean( gamelog[:OREB].to_f, total_OREB, games_played )
              h_OREB[ boxscore_traditional[:GAME_ID] ] = gamelog[:OREB].to_f
              row[:median_OREB] = median( h_OREB )
              row[:total_OREB] = total_OREB

              total_DREB, row[:mean_DREB] = mean( gamelog[:DREB].to_f, total_DREB, games_played )
              h_DREB[ boxscore_traditional[:GAME_ID] ] = gamelog[:DREB].to_f
              row[:median_DREB] = median( h_DREB )
              row[:total_DREB] = total_DREB

              total_REB, row[:mean_REB] = mean( gamelog[:REB].to_f, total_REB, games_played )
              h_REB[ boxscore_traditional[:GAME_ID] ] = gamelog[:REB].to_f
              row[:median_REB] = median( h_REB )
              row[:total_REB] = total_REB

              total_AST, row[:mean_AST] = mean( gamelog[:AST].to_f, total_AST, games_played )
              h_AST[ boxscore_traditional[:GAME_ID] ] = gamelog[:AST].to_f
              row[:median_AST] = median( h_AST )
              row[:total_AST] = total_AST

              total_STL, row[:mean_STL] = mean( gamelog[:STL].to_f, total_STL, games_played )
              h_STL[ boxscore_traditional[:GAME_ID] ] = gamelog[:STL].to_f
              row[:median_STL] = median( h_STL )
              row[:total_STL] = total_STL

              total_BLK, row[:mean_BLK] = mean( gamelog[:BLK].to_f, total_BLK, games_played )
              h_BLK[ boxscore_traditional[:GAME_ID] ] = gamelog[:BLK].to_f
              row[:median_BLK] = median( h_BLK )
              row[:total_BLK] = total_BLK

              if boxscore_traditional[:"TO"].to_f != gamelog[:"TOV"].to_f
                #p "box TO: #{boxscore_traditional[:"TO"]} gamelog TO: #{gamelog[:"TOV"]}"
              end

              total_TO, row[:mean_TO] = mean( gamelog[:"TOV"].to_f, total_TO, games_played )
              h_TO[ boxscore_traditional[:GAME_ID] ] = gamelog[:"TOV"].to_f
              row[:median_TO] = median( h_TO )
              row[:total_TO] = total_TO

              total_PF, row[:mean_PF] = mean( gamelog[:PF].to_f, total_PF, games_played )
              h_PF[ boxscore_traditional[:GAME_ID] ] = gamelog[:PF].to_f
              row[:median_PF] = median( h_PF )
              row[:total_PF] = total_PF

              total_PTS, row[:mean_PTS] = mean( gamelog[:PTS].to_f, total_PTS, games_played )
              h_PTS[ boxscore_traditional[:GAME_ID] ] = gamelog[:PTS].to_f
              row[:median_PTS] = median( h_PTS )
              row[:total_PTS] = total_PTS

              total_plus_minus, row[:mean_plus_minus] = mean( gamelog[:PLUS_MINUS].to_f, total_plus_minus, games_played )
              h_plus_minus[ boxscore_traditional[:GAME_ID] ] = gamelog[:PLUS_MINUS].to_f
              row[:median_plus_minus] = median( h_plus_minus )
              row[:total_plus_minus] = total_plus_minus

              #Need to figure out how to calculate these based on raw data
              tsa = total_FGA + 0.44 * total_FTA
              if 0 == tsa
                row[:TS_PCT] = 0
              else
                row[:TS_PCT] = (total_PTS / ( 2 * tsa )).round(3)
              end

              total_TS_PCT, row[:mean_TS_PCT] = mean( boxscore_advanced[:"TS_PCT"].to_f, total_TS_PCT, games_played )
              h_TS_PCT[ boxscore_traditional[:GAME_ID] ] = boxscore_advanced[:"TS_PCT"].to_f
              row[:median_TS_PCT] = median( h_TS_PCT )
              row[:total_TS_PCT] = total_TS_PCT

              if 0 == total_FGA
                row[:EFG_PCT] = 0
              else
                row[:EFG_PCT] = ((total_FGM + 0.5 * total_FG3M) / total_FGA).round(3)
              end

              total_EFG_PCT, row[:mean_EFG_PCT] = mean( boxscore_advanced[:"EFG_PCT"].to_f, total_EFG_PCT, games_played )
              h_EFG_PCT[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"EFG_PCT"].to_f
              row[:median_EFG_PCT] = median( h_EFG_PCT )
              row[:total_EFG_PCT] = total_EFG_PCT

              if 0 == total_FGA
                row[:PCT_FGA_3PT] = 0
              else
                row[:PCT_FGA_3PT] = (total_FG3A / total_FGA).round(3)
              end

              total_PCT_FGA_3PT, row[:mean_PCT_FGA_3PT] = mean( boxscore_scoring[:"PCT_FGA_3PT"].to_f, total_PCT_FGA_3PT, games_played )
              h_PCT_FGA_3PT[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_FGA_3PT"].to_f
              row[:median_PCT_FGA_3PT] = median( h_PCT_FGA_3PT )
              row[:total_PCT_FGA_3PT] = total_PCT_FGA_3PT

              if 0 == total_FGA
                row[:FTA_RATE] = 0
              else
                row[:FTA_RATE] = (total_FTA / total_FGA).round(3)
              end

              total_FTA_RATE, row[:mean_FTA_RATE] = mean( boxscore_fourfactors[:"FTA_RATE"].to_f, total_FTA_RATE, games_played )
              h_FTA_RATE[ boxscore_fourfactors[:GAME_ID] ] = boxscore_fourfactors[:"FTA_RATE"].to_f
              row[:median_FTA_RATE] = median( h_FTA_RATE )
              row[:total_FTA_RATE] = total_FTA_RATE

              total_OREB_PCT, row[:mean_OREB_PCT] = mean( boxscore_advanced[:"OREB_PCT"].to_f, total_OREB_PCT, games_played )
              h_OREB_PCT[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"OREB_PCT"].to_f
              row[:median_OREB_PCT] = median( h_OREB_PCT )
              row[:total_OREB_PCT] = total_OREB_PCT

              total_DREB_PCT, row[:mean_DREB_PCT] = mean( boxscore_advanced[:"DREB_PCT"].to_f, total_DREB_PCT, games_played )
              h_DREB_PCT[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"DREB_PCT"].to_f
              row[:median_DREB_PCT] = median( h_DREB_PCT )
              row[:total_DREB_PCT] = total_DREB_PCT

              total_REB_PCT, row[:mean_REB_PCT] = mean( boxscore_advanced[:"REB_PCT"].to_f, total_REB_PCT, games_played )
              h_REB_PCT[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"REB_PCT"].to_f
              row[:median_REB_PCT] = median( h_REB_PCT )
              row[:total_REB_PCT] = total_REB_PCT

              total_AST_PCT, row[:mean_AST_PCT] = mean( boxscore_advanced[:"AST_PCT"].to_f, total_AST_PCT, games_played )
              h_AST_PCT[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"AST_PCT"].to_f
              row[:median_AST_PCT] = median( h_AST_PCT )
              row[:total_AST_PCT] = total_AST_PCT

              total_PCT_STL, row[:mean_PCT_STL] = mean( boxscore_usage[:"PCT_STL"].to_f, total_PCT_STL, games_played )
              h_PCT_STL[ boxscore_advanced[:GAME_ID] ] = boxscore_usage[:"PCT_STL"].to_f
              row[:median_PCT_STL] = median( h_PCT_STL )
              row[:total_PCT_STL] = total_PCT_STL

              total_PCT_BLK, row[:mean_PCT_BLK] = mean( boxscore_usage[:"PCT_BLK"].to_f, total_PCT_BLK, games_played )
              h_PCT_BLK[ boxscore_advanced[:GAME_ID] ] = boxscore_usage[:"PCT_BLK"].to_f
              row[:median_PCT_BLK] = median( h_PCT_BLK )
              row[:total_PCT_BLK] = total_PCT_BLK

              total_TO_PCT, row[:mean_TO_PCT] = mean( boxscore_advanced[:"TM_TO_PCT"].to_f, total_TO_PCT, games_played )
              h_TO_PCT[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"TM_TO_PCT"].to_f
              row[:median_TO_PCT] = median( h_TO_PCT )
              row[:total_TO_PCT] = total_TO_PCT

              total_USG_PCT, row[:mean_USG_PCT] = mean( boxscore_advanced[:"USG_PCT"].to_f, total_USG_PCT, games_played )
              h_USG_PCT[ boxscore_fourfactors[:GAME_ID] ] = boxscore_advanced[:"USG_PCT"].to_f
              row[:median_USG_PCT] = median( h_USG_PCT )
              row[:total_USG_PCT] = total_USG_PCT

              total_OFF_RATING, row[:mean_OFF_RATING] = mean( boxscore_advanced[:"OFF_RATING"].to_f, total_OFF_RATING, games_played )
              h_OFF_RATING[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"OFF_RATING"].to_f
              row[:median_OFF_RATING] = median( h_OFF_RATING )
              row[:total_OFF_RATING] = total_OFF_RATING

              total_DEF_RATING, row[:mean_DEF_RATING] = mean( boxscore_advanced[:"DEF_RATING"].to_f, total_DEF_RATING, games_played )
              h_DEF_RATING[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"DEF_RATING"].to_f
              row[:median_DEF_RATING] = median( h_DEF_RATING )
              row[:total_DEF_RATING] = total_DEF_RATING

              total_NET_RATING, row[:mean_NET_RATING] = mean( boxscore_advanced[:"NET_RATING"].to_f, total_NET_RATING, games_played )
              h_NET_RATING[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"NET_RATING"].to_f
              row[:median_NET_RATING] = median( h_NET_RATING )
              row[:total_NET_RATING] = total_NET_RATING

              total_AST_TO, row[:mean_AST_TO] = mean( boxscore_advanced[:"AST_TOV"].to_f, total_AST_TO, games_played )
              h_AST_TO[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"AST_TOV"].to_f
              row[:median_AST_TO] = median( h_AST_TO )
              row[:total_AST_TO] = total_AST_TO

              total_AST_RATIO, row[:mean_AST_RATIO] = mean( boxscore_advanced[:"AST_RATIO"].to_f, total_AST_RATIO, games_played )
              h_AST_RATIO[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"AST_RATIO"].to_f
              row[:median_AST_RATIO] = median( h_AST_RATIO )
              row[:total_AST_RATIO] = total_AST_RATIO

              total_PACE, row[:mean_PACE] = mean( boxscore_advanced[:"PACE"].to_f, total_PACE, games_played )
              h_PACE[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"PACE"].to_f
              row[:median_PACE] = median( h_PACE )
              row[:total_PACE] = total_PACE

              total_PIE, row[:mean_PIE] = mean( boxscore_advanced[:"PIE"].to_f, total_PIE, games_played )
              h_PIE[ boxscore_advanced[:GAME_ID] ] = boxscore_advanced[:"PIE"].to_f
              row[:median_PIE] = median( h_PIE )
              row[:total_PIE] = total_PIE

              total_PTS_OFF_TOV, row[:mean_PTS_OFF_TOV] = mean( boxscore_misc[:"PTS_OFF_TOV"].to_f, total_PTS_OFF_TOV, games_played )
              h_PTS_OFF_TOV[ boxscore_misc[:GAME_ID] ] = boxscore_misc[:"PTS_OFF_TOV"].to_f
              row[:median_PTS_OFF_TOV] = median( h_PTS_OFF_TOV )
              row[:total_PTS_OFF_TOV] = total_PTS_OFF_TOV

              total_PTS_2ND_CHANCE, row[:mean_PTS_2ND_CHANCE] = mean( boxscore_misc[:"PTS_2ND_CHANCE"].to_f, total_PTS_2ND_CHANCE, games_played )
              h_PTS_2ND_CHANCE[ boxscore_misc[:GAME_ID] ] = boxscore_misc[:"PTS_2ND_CHANCE"].to_f
              row[:median_PTS_2ND_CHANCE] = median( h_PTS_2ND_CHANCE )
              row[:total_PTS_2ND_CHANCE] = total_PTS_2ND_CHANCE

              total_PTS_FB, row[:mean_PTS_FB] = mean( boxscore_misc[:"PTS_FB"].to_f, total_PTS_FB, games_played )
              h_PTS_FB[ boxscore_misc[:GAME_ID] ] = boxscore_misc[:"PTS_FB"].to_f
              row[:median_PTS_FB] = median( h_PTS_FB )
              row[:total_PTS_FB] = total_PTS_FB

              total_PTS_PAINT, row[:mean_PTS_PAINT] = mean( boxscore_misc[:"PTS_PAINT"].to_f, total_PTS_PAINT, games_played )
              h_PTS_PAINT[ boxscore_misc[:GAME_ID] ] = boxscore_misc[:"PTS_PAINT"].to_f
              row[:median_PTS_PAINT] = median( h_PTS_PAINT )
              row[:total_PTS_PAINT] = total_PTS_PAINT

              #scoring
              if 0 == total_FGA
                row[:PCT_FGA_2PT] = 0
              else
                total_FG2A = total_FGA - total_FG3A
                row[:PCT_FGA_2PT] = (total_FG2A / total_FGA).round(3)
              end

              total_PCT_FGA_2PT, row[:mean_PCT_FGA_2PT] = mean( boxscore_scoring[:"PCT_FGA_2PT"].to_f, total_PCT_FGA_2PT, games_played )
              h_PCT_FGA_2PT[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_FGA_2PT"].to_f
              row[:median_PCT_FGA_2PT] = median( h_PCT_FGA_2PT )
              row[:total_PCT_FGA_2PT] = total_PCT_FGA_2PT

              if 0 == total_PTS
                row[:PCT_PTS_2PT] = 0
              else
                total_PTS_2PT = total_PTS - total_FG3M * 3 - total_FTM
                row[:PCT_PTS_2PT] = (total_PTS_2PT / total_PTS).round(3)
              end

              total_PCT_PTS_2PT, row[:mean_PCT_PTS_2PT] = mean( boxscore_scoring[:"PCT_PTS_2PT"].to_f, total_PCT_PTS_2PT, games_played )
              h_PCT_PTS_2PT[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_PTS_2PT"].to_f
              row[:median_PCT_PTS_2PT] = median( h_PCT_PTS_2PT )
              row[:total_PCT_PTS_2PT] = total_PCT_PTS_2PT

              total_PCT_PTS_2PT_MR, row[:mean_PCT_PTS_2PT_MR] = mean( boxscore_scoring[:"PCT_PTS_2PT_MR"].to_f, total_PCT_PTS_2PT_MR, games_played )
              h_PCT_PTS_2PT_MR[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_PTS_2PT_MR"].to_f
              row[:median_PCT_PTS_2PT_MR] = median( h_PCT_PTS_2PT_MR )
              row[:total_PCT_PTS_2PT_MR] = total_PCT_PTS_2PT_MR

              if 0 == total_PTS
                row[:PCT_PTS_3PT] = 0
              else
                total_PTS_3PT = total_FG3M * 3
                row[:PCT_PTS_3PT] = (total_PTS_3PT / total_PTS).round(3)
              end

              total_PCT_PTS_3PT, row[:mean_PCT_PTS_3PT] = mean( boxscore_scoring[:"PCT_PTS_3PT"].to_f, total_PCT_PTS_3PT, games_played )
              h_PCT_PTS_3PT[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_PTS_3PT"].to_f
              row[:median_PCT_PTS_3PT] = median( h_PCT_PTS_3PT )
              row[:total_PCT_PTS_3PT] = total_PCT_PTS_3PT

              if 0 == total_PTS
                row[:PCT_PTS_FB] = 0
              else
                row[:PCT_PTS_FB] = (total_PTS_FB / total_PTS).round(3)
              end

              total_PCT_PTS_FB, row[:mean_PCT_PTS_FB] = mean( boxscore_scoring[:"PCT_PTS_FB"].to_f, total_PCT_PTS_FB, games_played )
              h_PCT_PTS_FB[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_PTS_FB"].to_f
              row[:median_PCT_PTS_FB] = median( h_PCT_PTS_FB )
              row[:total_PCT_PTS_FB] = total_PCT_PTS_FB

              if 0 == total_PTS
                row[:PCT_PTS_FT] = 0
              else
                row[:PCT_PTS_FT] = (total_FTM / total_PTS).round(3)
              end

              total_PCT_PTS_FT, row[:mean_PCT_PTS_FT] = mean( boxscore_scoring[:"PCT_PTS_FT"].to_f, total_PCT_PTS_FT, games_played )
              h_PCT_PTS_FT[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_PTS_FT"].to_f
              row[:median_PCT_PTS_FT] = median( h_PCT_PTS_FT )
              row[:total_PCT_PTS_FT] = total_PCT_PTS_FT

              if 0 == total_PTS
                row[:PCT_PTS_OFF_TOV] = 0
              else
                row[:PCT_PTS_OFF_TOV] = (total_PTS_OFF_TOV / total_PTS).round(3)
              end

              total_PCT_PTS_OFF_TOV, row[:mean_PCT_PTS_OFF_TOV] = mean( boxscore_scoring[:"PCT_PTS_OFF_TOV"].to_f, total_PCT_PTS_OFF_TOV, games_played )
              h_PCT_PTS_OFF_TOV[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_PTS_OFF_TOV"].to_f
              row[:median_PCT_PTS_OFF_TOV] = median( h_PCT_PTS_OFF_TOV )
              row[:total_PCT_PTS_OFF_TOV] = total_PCT_PTS_OFF_TOV

              if 0 == total_PTS
                row[:PCT_PTS_PAINT] = 0
              else
                row[:PCT_PTS_PAINT] = (total_PTS_PAINT / total_PTS).round(3)
              end

              total_PCT_PTS_PAINT, row[:mean_PCT_PTS_PAINT] = mean( boxscore_scoring[:"PCT_PTS_PAINT"].to_f, total_PCT_PTS_PAINT, games_played )
              h_PCT_PTS_PAINT[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_PTS_PAINT"].to_f
              row[:median_PCT_PTS_PAINT] = median( h_PCT_PTS_PAINT )
              row[:total_PCT_PTS_PAINT] = total_PCT_PTS_PAINT

              boxscore_FG2M = gamelog[:"FGM"].to_f - gamelog[:"FG3M"].to_f
              boxscore_AST_2PM = (boxscore_scoring[:"PCT_AST_2PM"].to_f * boxscore_FG2M).round.to_f

              total_AST_2PM, row[:mean_AST_2PM] = mean( boxscore_AST_2PM, total_AST_2PM, games_played )
              h_AST_2PM[ boxscore_scoring[:GAME_ID] ] = boxscore_AST_2PM
              row[:median_AST_2PM] = median( h_AST_2PM )
              row[:total_AST_2PM] = total_AST_2PM

              total_PCT_AST_2PM, row[:mean_PCT_AST_2PM] = mean( boxscore_scoring[:"PCT_AST_2PM"].to_f, total_PCT_AST_2PM, games_played )
              h_PCT_AST_2PM[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_AST_2PM"].to_f
              row[:median_PCT_AST_2PM] = median( h_PCT_AST_2PM )
              row[:total_PCT_AST_2PM] = total_PCT_AST_2PM
              if 0 == (total_FGM - total_FG3M)
                row[:PCT_AST_2PM] = 0
              else
                row[:PCT_AST_2PM] = total_AST_2PM / ( total_FGM - total_FG3M ).to_f
              end

              boxscore_UAST_2PM = boxscore_FG2M - boxscore_AST_2PM

              total_UAST_2PM, row[:mean_UAST_2PM] = mean( boxscore_UAST_2PM, total_UAST_2PM, games_played )
              h_UAST_2PM[ boxscore_scoring[:GAME_ID] ] = boxscore_UAST_2PM
              row[:median_UAST_2PM] = median( h_UAST_2PM )
              row[:total_UAST_2PM] = total_UAST_2PM
              if 0 == ( total_FGM - total_FG3M ).to_f
                row[:PCT_UAST_2PM] = 0
              else
                row[:PCT_UAST_2PM] = total_UAST_2PM / ( total_FGM - total_FG3M ).to_f
              end
              

              total_PCT_UAST_2PM, row[:mean_PCT_UAST_2PM] = mean( boxscore_scoring[:"PCT_UAST_2PM"].to_f, total_PCT_UAST_2PM, games_played )
              h_PCT_UAST_2PM[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_UAST_2PM"].to_f
              row[:median_PCT_UAST_2PM] = median( h_PCT_UAST_2PM )
              row[:total_PCT_UAST_2PM] = total_PCT_UAST_2PM

              boxscore_AST_3PM = (boxscore_scoring[:"PCT_AST_3PM"].to_f * gamelog[:"FG3M"].to_f).round.to_f

              total_AST_3PM, row[:mean_AST_3PM] = mean( boxscore_AST_3PM, total_AST_3PM, games_played )
              h_AST_3PM[ boxscore_scoring[:GAME_ID] ] = boxscore_AST_3PM
              row[:median_AST_3PM] = median( h_AST_3PM )
              row[:total_AST_3PM] = total_AST_3PM

              total_PCT_AST_3PM, row[:mean_PCT_AST_3PM] = mean( boxscore_scoring[:"PCT_AST_3PM"].to_f, total_PCT_AST_3PM, games_played )
              h_PCT_AST_3PM[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_AST_3PM"].to_f
              row[:median_PCT_AST_3PM] = median( h_PCT_AST_3PM )
              row[:total_PCT_AST_3PM] = total_PCT_AST_3PM
              if 0 == total_FG3M
                row[:PCT_AST_3PM] = 0
              else
                row[:PCT_AST_3PM] = total_AST_3PM / total_FG3M
              end

              boxscore_UAST_3PM = gamelog[:"FG3M"].to_f - boxscore_AST_3PM

              total_UAST_3PM, row[:mean_UAST_3PM] = mean( boxscore_UAST_3PM, total_UAST_3PM, games_played )
              h_UAST_3PM[ boxscore_scoring[:GAME_ID] ] = boxscore_UAST_3PM
              row[:median_UAST_3PM] = median( h_UAST_3PM )
              row[:total_UAST_3PM] = total_UAST_3PM

              total_PCT_UAST_3PM, row[:mean_PCT_UAST_3PM] = mean( boxscore_scoring[:"PCT_UAST_3PM"].to_f, total_PCT_UAST_3PM, games_played )
              h_PCT_UAST_3PM[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_UAST_3PM"].to_f
              row[:median_PCT_UAST_3PM] = median( h_PCT_UAST_3PM )
              row[:total_PCT_UAST_3PM] = total_PCT_UAST_3PM
              if 0 == total_FG3M
                row[:PCT_UAST_3PM] = 0
              else
                row[:PCT_UAST_3PM] = total_UAST_3PM / total_FG3M
              end

              boxscore_AST_FGM = (gamelog[:"FGM"].to_f * boxscore_scoring[:"PCT_AST_FGM"].to_f).round.to_f

              total_AST_FGM, row[:mean_AST_FGM] = mean( boxscore_AST_FGM, total_AST_FGM, games_played )
              h_AST_FGM[ boxscore_scoring[:GAME_ID] ] = boxscore_AST_FGM
              row[:median_AST_FGM] = median( h_AST_FGM )
              row[:total_AST_FGM] = total_AST_FGM

              total_PCT_AST_FGM, row[:mean_PCT_AST_FGM] = mean( boxscore_scoring[:"PCT_AST_FGM"].to_f, total_PCT_AST_FGM, games_played )
              h_PCT_AST_FGM[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_AST_FGM"].to_f
              row[:median_PCT_AST_FGM] = median( h_PCT_AST_FGM )
              row[:total_PCT_AST_FGM] = total_PCT_AST_FGM
              if 0 == total_FGM
                row[:PCT_AST_FGM] = 0
              else
                row[:PCT_AST_FGM] = total_AST_FGM / total_FGM
              end

              boxscore_UAST_FGM = gamelog[:"FGM"].to_f - boxscore_AST_FGM
              
              total_UAST_FGM, row[:mean_UAST_FGM] = mean( boxscore_UAST_FGM, total_UAST_FGM, games_played )
              h_UAST_FGM[ boxscore_scoring[:GAME_ID] ] = boxscore_UAST_FGM
              row[:median_UAST_FGM] = median( h_UAST_FGM )
              row[:total_UAST_FGM] = total_UAST_FGM

              total_PCT_UAST_FGM, row[:mean_PCT_UAST_FGM] = mean( boxscore_scoring[:"PCT_UAST_FGM"].to_f, total_PCT_UAST_FGM, games_played )
              h_PCT_UAST_FGM[ boxscore_scoring[:GAME_ID] ] = boxscore_scoring[:"PCT_UAST_FGM"].to_f
              row[:median_PCT_UAST_FGM] = median( h_PCT_UAST_FGM )
              row[:total_PCT_UAST_FGM] = total_PCT_UAST_FGM
              if 0 == total_FGM
                row[:PCT_UAST_FGM] = 0
              else
                row[:PCT_UAST_FGM] = total_UAST_FGM / total_FGM
              end

              #tracking
              total_DIST, row[:mean_DIST] = mean( boxscore_tracking[:"DIST"].to_f, total_DIST, games_played )
              h_DIST[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"DIST"].to_f
              row[:median_DIST] = median( h_DIST )
              row[:total_DIST] = total_DIST

              total_ORBC, row[:mean_ORBC] = mean( boxscore_tracking[:"ORBC"].to_f, total_ORBC, games_played )
              h_ORBC[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"ORBC"].to_f
              row[:median_ORBC] = median( h_ORBC )
              row[:total_ORBC] = total_ORBC

              total_RBC, row[:mean_RBC] = mean( boxscore_tracking[:"RBC"].to_f, total_RBC, games_played )
              h_RBC[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"RBC"].to_f
              row[:median_RBC] = median( h_RBC )
              row[:total_RBC] = total_RBC

              total_SAST, row[:mean_SAST] = mean( boxscore_tracking[:"SAST"].to_f, total_SAST, games_played )
              h_SAST[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"SAST"].to_f
              row[:median_SAST] = median( h_SAST )
              row[:total_SAST] = total_SAST

              total_PASS, row[:mean_PASS] = mean( boxscore_tracking[:"PASS"].to_f, total_PASS, games_played )
              h_PASS[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"PASS"].to_f
              row[:median_PASS] = median( h_PASS )
              row[:total_PASS] = total_PASS

              total_CFGM, row[:mean_CFGM] = mean( boxscore_tracking[:"CFGM"].to_f, total_CFGM, games_played )
              h_CFGM[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"CFGM"].to_f
              row[:median_CFGM] = median( h_CFGM )
              row[:total_CFGM] = total_CFGM

              total_CFG_PCT, row[:mean_CFG_PCT] = mean( boxscore_tracking[:"CFG_PCT"].to_f, total_CFG_PCT, games_played )
              h_CFG_PCT[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"CFG_PCT"].to_f
              row[:median_CFG_PCT] = median( h_CFG_PCT )
              row[:total_CFG_PCT] = total_CFG_PCT

              total_UFGA, row[:mean_UFGA] = mean( boxscore_tracking[:"UFGA"].to_f, total_UFGA, games_played )
              h_UFGA[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"UFGA"].to_f
              row[:median_UFGA] = median( h_UFGA )
              row[:total_UFGA] = total_UFGA

              total_DFGA, row[:mean_DFGA] = mean( boxscore_tracking[:"DFGA"].to_f, total_DFGA, games_played )
              h_DFGA[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"DFGA"].to_f
              row[:median_DFGA] = median( h_DFGA )
              row[:total_DFGA] = total_DFGA

              total_DFG_PCT, row[:mean_DFG_PCT] = mean( boxscore_tracking[:"DFG_PCT"].to_f, total_DFG_PCT, games_played )
              h_DFG_PCT[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"DFG_PCT"].to_f
              row[:median_DFG_PCT] = median( h_DFG_PCT )
              row[:total_DFG_PCT] = total_DFG_PCT


              ##OPPONENT STATS if this is a team boxscore
              opponent = ""

              #jlk - This is hacky.  Should be using DB at this opint, not CSV files
              #binding.pry
              file = File.open(season + "/" + type + "/" + boxscore_traditional[:GAME_ID].to_s + "_traditional_TeamStats.csv")
              arr_of_arrs = CSV.parse( file )
              #if arr_of_arrs[1][4] == row[:team_abbreviation]
              if arr_of_arrs[1][3] == boxscore_traditional[:TEAM_ID]
                opponent_abbreviation = arr_of_arrs[2][5]
              #elsif arr_of_arrs[2][4] == row[:team_abbreviation]
              elsif arr_of_arrs[2][3] == boxscore_traditional[:TEAM_ID]
                opponent_abbreviation = arr_of_arrs[1][5]
              else
                binding.pry
                p "error"
              end

              if true == bTeam
                o_boxscore_traditional = DB[ :"#{type}_traditional_TeamStats" ].where(:TEAM_ABBREVIATION => opponent_abbreviation).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                o_gamelog = DB[ :"#{type}_gamelogs" ].where(:TEAM_ID => o_boxscore_traditional[:TEAM_ID]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                o_boxscore_advanced = DB[ :"#{type}_advanced_TeamStats" ].where(:TEAM_ABBREVIATION => opponent_abbreviation).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                o_boxscore_fourfactors = DB[ :"#{type}_fourfactors_sqlTeamsFourFactors" ].where(:TEAM_ABBREVIATION => opponent_abbreviation).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                o_boxscore_scoring = DB[ :"#{type}_scoring_sqlTeamsScoring" ].where(:TEAM_ABBREVIATION => opponent_abbreviation).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                o_boxscore_usage = DB[ :"#{type}_usage_sqlTeamsUsage" ].where(:TEAM_ABBREVIATION => opponent_abbreviation).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                o_boxscore_misc = DB[ :"#{type}_misc_sqlTeamsMisc" ].where(:TEAM_ABBREVIATION => opponent_abbreviation).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                o_boxscore_tracking = DB[ :"#{type}_playertrack_PlayerTrackTeam" ].where(:TEAM_ABBREVIATION => opponent_abbreviation).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                o_boxscore_traditional_starters = DB[ :"#{type}_traditional_TeamStarterBenchStats" ].where(:TEAM_ABBREVIATION => opponent_abbreviation).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
                o_boxscore_traditional_bench = DB[ :"#{type}_traditional_TeamStarterBenchStats" ].where(:TEAM_ABBREVIATION => opponent_abbreviation).where(:SEASON => season).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[1]

                total_o_FGM, row[:mean_o_FGM] = mean( o_gamelog[:FGM].to_f, total_o_FGM, games_played )
                h_o_FGM[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:FGM].to_f
                row[:median_o_FGM] = median( h_o_FGM )
                row[:total_o_FGM] = total_o_FGM

                total_o_FGA, row[:mean_o_FGA] = mean( o_gamelog[:FGA].to_f, total_o_FGA, games_played )
                h_o_FGA[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:FGA].to_f
                row[:median_o_FGA] = median( h_o_FGA )
                row[:total_o_FGA] = total_o_FGA

                total_o_FG_PCT, row[:mean_o_FG_PCT] = mean( o_gamelog[:FG_PCT].to_f, total_o_FG_PCT, games_played )
                h_o_FG_PCT[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:FG_PCT].to_f
                row[:median_o_FG_PCT] = median( h_o_FG_PCT )
                row[:total_o_FG_PCT] = total_o_FG_PCT
                if 0 == total_o_FGA
                  row[:o_FG_PCT] = 0
                else
                  row[:o_FG_PCT] = total_o_FGM / total_o_FGA
                end

                total_o_FG3M, row[:mean_o_FG3M] = mean( o_gamelog[:FG3M].to_f, total_o_FG3M, games_played )
                h_o_FG3M[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:FG3M].to_f
                row[:median_o_FG3M] = median( h_o_FG3M )
                row[:total_o_FG3M] = total_o_FG3M

                total_o_FG3A, row[:mean_o_FG3A] = mean( o_gamelog[:FG3A].to_f, total_o_FG3A, games_played )
                h_o_FG3A[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:FG3A].to_f
                row[:median_o_FG3A] = median( h_o_FG3A )
                row[:total_o_FG3A] = total_o_FG3A

                total_o_FG3_PCT, row[:mean_o_FG3_PCT] = mean( o_gamelog[:FG3_PCT].to_f, total_o_FG3_PCT, games_played )
                h_o_FG3_PCT[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:FG3_PCT].to_f
                row[:median_o_FG3_PCT] = median( h_o_FG3_PCT )
                row[:total_o_FG3_PCT] = total_o_FG3_PCT
                if 0 == total_o_FG3A
                  row[:o_FG3_PCT] = 0
                else
                  row[:o_FG3_PCT] = total_o_FG3M / total_o_FG3A
                end

                total_o_FTM, row[:mean_o_FTM] = mean( o_gamelog[:FTM].to_f, total_o_FTM, games_played )
                h_o_FTM[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:FTM].to_f
                row[:median_o_FTM] = median( h_o_FTM )
                row[:total_o_FTM] = total_o_FTM

                total_o_FTA, row[:mean_o_FTA] = mean( o_gamelog[:FTA].to_f, total_o_FTA, games_played )
                h_o_FTA[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:FTA].to_f
                row[:median_o_FTA] = median( h_o_FTA )
                row[:total_o_FTA] = total_o_FTA

                total_o_FT_PCT, row[:mean_o_FT_PCT] = mean( o_gamelog[:FT_PCT].to_f, total_o_FT_PCT, games_played )
                h_o_FT_PCT[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:FT_PCT].to_f
                row[:median_o_FT_PCT] = median( h_o_FT_PCT )
                row[:total_o_FT_PCT] = total_o_FT_PCT
                if 0 == total_o_FTA
                  row[:o_FT_PCT] = 0
                else
                  row[:o_FT_PCT] = total_o_FTM / total_o_FTA
                end

                total_o_OREB, row[:mean_o_OREB] = mean( o_gamelog[:OREB].to_f, total_o_OREB, games_played )
                h_o_OREB[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:OREB].to_f
                row[:median_o_OREB] = median( h_o_OREB )
                row[:total_o_OREB] = total_o_OREB

                total_o_DREB, row[:mean_o_DREB] = mean( o_gamelog[:DREB].to_f, total_o_DREB, games_played )
                h_o_DREB[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:DREB].to_f
                row[:median_o_DREB] = median( h_o_DREB )
                row[:total_o_DREB] = total_o_DREB

                total_o_REB, row[:mean_o_REB] = mean( o_gamelog[:REB].to_f, total_o_REB, games_played )
                h_o_REB[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:REB].to_f
                row[:median_o_REB] = median( h_o_REB )
                row[:total_o_REB] = total_o_REB

                total_o_AST, row[:mean_o_AST] = mean( o_gamelog[:AST].to_f, total_o_AST, games_played )
                h_o_AST[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:AST].to_f
                row[:median_o_AST] = median( h_o_AST )
                row[:total_o_AST] = total_o_AST

                total_o_STL, row[:mean_o_STL] = mean( o_gamelog[:STL].to_f, total_o_STL, games_played )
                h_o_STL[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:STL].to_f
                row[:median_o_STL] = median( h_o_STL )
                row[:total_o_STL] = total_o_STL

                total_o_BLK, row[:mean_o_BLK] = mean( o_gamelog[:BLK].to_f, total_o_BLK, games_played )
                h_o_BLK[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:BLK].to_f
                row[:median_o_BLK] = median( h_o_BLK )
                row[:total_o_BLK] = total_o_BLK

                if o_boxscore_traditional[:"TO"].to_f != o_gamelog[:"TOV"].to_f
                  #p "box TO: #{o_boxscore_traditional[:"TO"]} gamelog TO: #{o_gamelog[:"TOV"]}"
                end

                total_o_TO, row[:mean_o_TO] = mean( o_gamelog[:"TOV"].to_f, total_o_TO, games_played )
                h_o_TO[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:"TOV"].to_f
                row[:median_o_TO] = median( h_o_TO )
                row[:total_o_TO] = total_o_TO

                total_o_PF, row[:mean_o_PF] = mean( o_gamelog[:PF].to_f, total_o_PF, games_played )
                h_oPF[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:PF].to_f
                row[:median_o_PF] = median( h_oPF )
                row[:total_o_PF] = total_o_PF

                total_o_PTS, row[:mean_o_PTS] = mean( o_gamelog[:PTS].to_f, total_o_PTS, games_played )
                h_oPTS[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:PTS].to_f
                row[:median_o_PTS] = median( h_oPTS )
                row[:total_o_PTS] = total_o_PTS

                total_o_plus_minus, row[:mean_o_plus_minus] = mean( o_gamelog[:PLUS_MINUS].to_f, total_o_plus_minus, games_played )
                h_o_plus_minus[ o_boxscore_traditional[:GAME_ID] ] = o_gamelog[:PLUS_MINUS].to_f
                row[:median_o_plus_minus] = median( h_o_plus_minus )
                row[:total_o_plus_minus] = total_o_plus_minus

                #Need to figure out how to calculate these based on raw data
                tsa = total_o_FGA + 0.44 * total_o_FTA
                if 0 == tsa
                  row[:o_TS_PCT] = 0
                else
                  row[:o_TS_PCT] = (total_o_PTS / ( 2 * tsa )).round(3)
                end

                total_o_TS_PCT, row[:mean_o_TS_PCT] = mean( o_boxscore_advanced[:"TS_PCT"].to_f, total_o_TS_PCT, games_played )
                h_o_TS_PCT[ o_boxscore_traditional[:GAME_ID] ] = o_boxscore_advanced[:"TS_PCT"].to_f
                row[:median_o_TS_PCT] = median( h_o_TS_PCT )
                row[:total_o_TS_PCT] = total_o_TS_PCT

                if 0 == total_o_FGA
                  row[:o_EFG_PCT] = 0
                else
                  row[:o_EFG_PCT] = ((total_o_FGM + 0.5 * total_o_FG3M) / total_o_FGA).round(3)
                end

                total_o_EFG_PCT, row[:mean_o_EFG_PCT] = mean( o_boxscore_advanced[:"EFG_PCT"].to_f, total_o_EFG_PCT, games_played )
                h_o_EFG_PCT[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"EFG_PCT"].to_f
                row[:median_o_EFG_PCT] = median( h_o_EFG_PCT )
                row[:total_o_EFG_PCT] = total_o_EFG_PCT

                if 0 == total_o_FGA
                  row[:o_PCT_FGA_3PT] = 0
                else
                  row[:o_PCT_FGA_3PT] = (total_o_FG3A / total_o_FGA).round(3)
                end

                total_o_PCT_FGA_3PT, row[:mean_o_PCT_FGA_3PT] = mean( o_boxscore_scoring[:"PCT_FGA_3PT"].to_f, total_o_PCT_FGA_3PT, games_played )
                h_o_PCT_FGA_3PT[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_FGA_3PT"].to_f
                row[:median_o_PCT_FGA_3PT] = median( h_o_PCT_FGA_3PT )
                row[:total_o_PCT_FGA_3PT] = total_o_PCT_FGA_3PT

                if 0 == total_o_FGA
                  row[:o_FTA_RATE] = 0
                else
                  row[:o_FTA_RATE] = (total_o_FTA / total_o_FGA).round(3)
                end

                total_o_FTA_RATE, row[:mean_o_FTA_RATE] = mean( o_boxscore_fourfactors[:"FTA_RATE"].to_f, total_o_FTA_RATE, games_played )
                h_o_FTA_RATE[ o_boxscore_fourfactors[:GAME_ID] ] = o_boxscore_fourfactors[:"FTA_RATE"].to_f
                row[:median_o_FTA_RATE] = median( h_o_FTA_RATE )
                row[:total_o_FTA_RATE] = total_o_FTA_RATE

                total_o_OREB_PCT, row[:mean_o_OREB_PCT] = mean( o_boxscore_advanced[:"OREB_PCT"].to_f, total_o_OREB_PCT, games_played )
                h_o_OREB_PCT[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"OREB_PCT"].to_f
                row[:median_o_OREB_PCT] = median( h_o_OREB_PCT )
                row[:total_o_OREB_PCT] = total_o_OREB_PCT

                total_o_DREB_PCT, row[:mean_o_DREB_PCT] = mean( o_boxscore_advanced[:"DREB_PCT"].to_f, total_o_DREB_PCT, games_played )
                h_o_DREB_PCT[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"DREB_PCT"].to_f
                row[:median_o_DREB_PCT] = median( h_o_DREB_PCT )
                row[:total_o_DREB_PCT] = total_o_DREB_PCT

                total_o_REB_PCT, row[:mean_o_REB_PCT] = mean( o_boxscore_advanced[:"REB_PCT"].to_f, total_o_REB_PCT, games_played )
                h_o_REB_PCT[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"REB_PCT"].to_f
                row[:median_o_REB_PCT] = median( h_o_REB_PCT )
                row[:total_o_REB_PCT] = total_o_REB_PCT

                total_o_AST_PCT, row[:mean_o_AST_PCT] = mean( o_boxscore_advanced[:"AST_PCT"].to_f, total_o_AST_PCT, games_played )
                h_o_AST_PCT[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"AST_PCT"].to_f
                row[:median_o_AST_PCT] = median( h_o_AST_PCT )
                row[:total_o_AST_PCT] = total_o_AST_PCT

                total_o_PCT_STL, row[:mean_o_PCT_STL] = mean( o_boxscore_usage[:"PCT_STL"].to_f, total_o_PCT_STL, games_played )
                h_o_PCT_STL[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_usage[:"PCT_STL"].to_f
                row[:median_o_PCT_STL] = median( h_o_PCT_STL )
                row[:total_o_PCT_STL] = total_o_PCT_STL

                total_o_PCT_BLK, row[:mean_o_PCT_BLK] = mean( o_boxscore_usage[:"PCT_BLK"].to_f, total_o_PCT_BLK, games_played )
                h_o_PCT_BLK[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_usage[:"PCT_BLK"].to_f
                row[:median_o_PCT_BLK] = median( h_o_PCT_BLK )
                row[:total_o_PCT_BLK] = total_o_PCT_BLK

                total_o_TO_PCT, row[:mean_o_TO_PCT] = mean( o_boxscore_advanced[:"TM_TO_PCT"].to_f, total_o_TO_PCT, games_played )
                h_o_TO_PCT[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"TM_TO_PCT"].to_f
                row[:median_o_TO_PCT] = median( h_o_TO_PCT )
                row[:total_o_TO_PCT] = total_o_TO_PCT

                total_o_USG_PCT, row[:mean_o_USG_PCT] = mean( o_boxscore_advanced[:"USG_PCT"].to_f, total_o_USG_PCT, games_played )
                h_o_USG_PCT[ o_boxscore_fourfactors[:GAME_ID] ] = o_boxscore_advanced[:"USG_PCT"].to_f
                row[:median_o_USG_PCT] = median( h_o_USG_PCT )
                row[:total_o_USG_PCT] = total_o_USG_PCT

                total_o_OFF_RATING, row[:mean_o_OFF_RATING] = mean( o_boxscore_advanced[:"OFF_RATING"].to_f, total_o_OFF_RATING, games_played )
                h_o_OFF_RATING[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"OFF_RATING"].to_f
                row[:median_o_OFF_RATING] = median( h_o_OFF_RATING )
                row[:total_o_OFF_RATING] = total_o_OFF_RATING

                total_o_DEF_RATING, row[:mean_o_DEF_RATING] = mean( o_boxscore_advanced[:"DEF_RATING"].to_f, total_o_DEF_RATING, games_played )
                h_o_DEF_RATING[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"DEF_RATING"].to_f
                row[:median_o_DEF_RATING] = median( h_o_DEF_RATING )
                row[:total_o_DEF_RATING] = total_o_DEF_RATING

                total_o_NET_RATING, row[:mean_o_NET_RATING] = mean( o_boxscore_advanced[:"NET_RATING"].to_f, total_o_NET_RATING, games_played )
                h_o_NET_RATING[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"NET_RATING"].to_f
                row[:median_o_NET_RATING] = median( h_o_NET_RATING )
                row[:total_o_NET_RATING] = total_o_NET_RATING

                total_o_AST_TO, row[:mean_o_AST_TO] = mean( o_boxscore_advanced[:"AST_TOV"].to_f, total_o_AST_TO, games_played )
                h_o_AST_TO[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"AST_TOV"].to_f
                row[:median_o_AST_TO] = median( h_o_AST_TO )
                row[:total_o_AST_TO] = total_o_AST_TO

                total_o_AST_RATIO, row[:mean_o_AST_RATIO] = mean( o_boxscore_advanced[:"AST_RATIO"].to_f, total_o_AST_RATIO, games_played )
                h_o_AST_RATIO[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"AST_RATIO"].to_f
                row[:median_o_AST_RATIO] = median( h_o_AST_RATIO )
                row[:total_o_AST_RATIO] = total_o_AST_RATIO

                total_o_PACE, row[:mean_o_PACE] = mean( o_boxscore_advanced[:"PACE"].to_f, total_o_PACE, games_played )
                h_o_PACE[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"PACE"].to_f
                row[:median_o_PACE] = median( h_o_PACE )
                row[:total_o_PACE] = total_o_PACE

                total_o_PIE, row[:mean_o_PIE] = mean( o_boxscore_advanced[:"PIE"].to_f, total_o_PIE, games_played )
                h_o_PIE[ o_boxscore_advanced[:GAME_ID] ] = o_boxscore_advanced[:"PIE"].to_f
                row[:median_o_PIE] = median( h_o_PIE )
                row[:total_o_PIE] = total_o_PIE

                if true == bTeam
                  if o_boxscore_misc[:"PTS_OFF_TOV"].to_f != boxscore_misc[:"OPP_PTS_OFF_TOV"].to_f
                    binding.pry
                    p "error"
                  end
                end
                total_o_PTS_OFF_TOV, row[:mean_o_PTS_OFF_TOV] = mean( o_boxscore_misc[:"PTS_OFF_TOV"].to_f, total_o_PTS_OFF_TOV, games_played )
                h_o_PTS_OFF_TOV[ o_boxscore_misc[:GAME_ID] ] = o_boxscore_misc[:"PTS_OFF_TOV"].to_f
                row[:median_o_PTS_OFF_TOV] = median( h_o_PTS_OFF_TOV )
                row[:total_o_PTS_OFF_TOV] = total_o_PTS_OFF_TOV

                if true == bTeam
                  if o_boxscore_misc[:"PTS_2ND_CHANCE"].to_f != boxscore_misc[:"OPP_PTS_2ND_CHANCE"].to_f
                    binding.pry
                    p "error"
                  end
                end
                total_o_PTS_2ND_CHANCE, row[:mean_o_PTS_2ND_CHANCE] = mean( o_boxscore_misc[:"PTS_2ND_CHANCE"].to_f, total_o_PTS_2ND_CHANCE, games_played )
                h_o_PTS_2ND_CHANCE[ o_boxscore_misc[:GAME_ID] ] = o_boxscore_misc[:"PTS_2ND_CHANCE"].to_f
                row[:median_o_PTS_2ND_CHANCE] = median( h_o_PTS_2ND_CHANCE )
                row[:total_o_PTS_2ND_CHANCE] = total_o_PTS_2ND_CHANCE

                if true == bTeam
                  if o_boxscore_misc[:"PTS_FB"].to_f != boxscore_misc[:"OPP_PTS_FB"].to_f
                    binding.pry
                    p "error"
                  end
                end
                total_o_PTS_FB, row[:mean_o_PTS_FB] = mean( o_boxscore_misc[:"PTS_FB"].to_f, total_o_PTS_FB, games_played )
                h_o_PTS_FB[ o_boxscore_misc[:GAME_ID] ] = o_boxscore_misc[:"PTS_FB"].to_f
                row[:median_o_PTS_FB] = median( h_o_PTS_FB )
                row[:total_o_PTS_FB] = total_o_PTS_FB

                if true == bTeam
                  if o_boxscore_misc[:"PTS_PAINT"].to_f != boxscore_misc[:"OPP_PTS_PAINT"].to_f
                    binding.pry
                    p "error"
                  end
                end
                total_o_PTS_PAINT, row[:mean_o_PTS_PAINT] = mean( o_boxscore_misc[:"PTS_PAINT"].to_f, total_o_PTS_PAINT, games_played )
                h_o_PTS_PAINT[ o_boxscore_misc[:GAME_ID] ] = o_boxscore_misc[:"PTS_PAINT"].to_f
                row[:median_o_PTS_PAINT] = median( h_o_PTS_PAINT )
                row[:total_o_PTS_PAINT] = total_o_PTS_PAINT

                #scoring
                if 0 == total_o_FGA
                  row[:o_PCT_FGA_2PT] = 0
                else
                  total_o_FG2A = total_o_FGA - total_o_FG3A
                  row[:o_PCT_FGA_2PT] = (total_o_FG2A / total_o_FGA).round(3)
                end

                total_o_PCT_FGA_2PT, row[:mean_o_PCT_FGA_2PT] = mean( o_boxscore_scoring[:"PCT_FGA_2PT"].to_f, total_o_PCT_FGA_2PT, games_played )
                h_o_PCT_FGA_2PT[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_FGA_2PT"].to_f
                row[:median_o_PCT_FGA_2PT] = median( h_o_PCT_FGA_2PT )
                row[:total_o_PCT_FGA_2PT] = total_o_PCT_FGA_2PT

                if 0 == total_o_PTS
                  row[:o_PCT_PTS_2PT] = 0
                else
                  total_o_PTS_2PT = total_o_PTS - total_o_FG3M * 3 - total_o_FTM
                  row[:o_PCT_PTS_2PT] = (total_o_PTS_2PT / total_o_PTS).round(3)
                end

                total_o_PCT_PTS_2PT, row[:mean_o_PCT_PTS_2PT] = mean( o_boxscore_scoring[:"PCT_PTS_2PT"].to_f, total_o_PCT_PTS_2PT, games_played )
                h_o_PCT_PTS_2PT[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_PTS_2PT"].to_f
                row[:median_o_PCT_PTS_2PT] = median( h_o_PCT_PTS_2PT )
                row[:total_o_PCT_PTS_2PT] = total_o_PCT_PTS_2PT

                total_o_PCT_PTS_2PT_MR, row[:mean_o_PCT_PTS_2PT_MR] = mean( o_boxscore_scoring[:"PCT_PTS_2PT_MR"].to_f, total_o_PCT_PTS_2PT_MR, games_played )
                h_o_PCT_PTS_2PT_MR[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_PTS_2PT_MR"].to_f
                row[:median_o_PCT_PTS_2PT_MR] = median( h_o_PCT_PTS_2PT_MR )
                row[:total_o_PCT_PTS_2PT_MR] = total_o_PCT_PTS_2PT_MR

                if 0 == total_o_PTS
                  row[:o_PCT_PTS_3PT] = 0
                else
                  total_o_PTS_3PT = total_o_FG3M * 3
                  row[:o_PCT_PTS_3PT] = (total_o_PTS_3PT / total_o_PTS).round(3)
                end

                total_o_PCT_PTS_3PT, row[:mean_o_PCT_PTS_3PT] = mean( o_boxscore_scoring[:"PCT_PTS_3PT"].to_f, total_o_PCT_PTS_3PT, games_played )
                h_o_PCT_PTS_3PT[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_PTS_3PT"].to_f
                row[:median_o_PCT_PTS_3PT] = median( h_o_PCT_PTS_3PT )
                row[:total_o_PCT_PTS_3PT] = total_o_PCT_PTS_3PT

                if 0 == total_o_PTS
                  row[:o_PCT_PTS_FB] = 0
                else
                  row[:o_PCT_PTS_FB] = (total_o_PTS_FB / total_o_PTS).round(3)
                end

                total_o_PCT_PTS_FB, row[:mean_o_PCT_PTS_FB] = mean( o_boxscore_scoring[:"PCT_PTS_FB"].to_f, total_o_PCT_PTS_FB, games_played )
                h_o_PCT_PTS_FB[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_PTS_FB"].to_f
                row[:median_o_PCT_PTS_FB] = median( h_o_PCT_PTS_FB )
                row[:total_o_PCT_PTS_FB] = total_o_PCT_PTS_FB

                if 0 == total_o_PTS
                  row[:o_PCT_PTS_FT] = 0
                else
                  row[:o_PCT_PTS_FT] = (total_o_FTM / total_o_PTS).round(3)
                end

                total_o_PCT_PTS_FT, row[:mean_o_PCT_PTS_FT] = mean( o_boxscore_scoring[:"PCT_PTS_FT"].to_f, total_o_PCT_PTS_FT, games_played )
                h_o_PCT_PTS_FT[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_PTS_FT"].to_f
                row[:median_o_PCT_PTS_FT] = median( h_o_PCT_PTS_FT )
                row[:total_o_PCT_PTS_FT] = total_o_PCT_PTS_FT

                if 0 == total_o_PTS
                  row[:o_PCT_PTS_OFF_TOV] = 0
                else
                  row[:o_PCT_PTS_OFF_TOV] = (total_o_PTS_OFF_TOV / total_o_PTS).round(3)
                end

                total_o_PCT_PTS_OFF_TOV, row[:mean_o_PCT_PTS_OFF_TOV] = mean( o_boxscore_scoring[:"PCT_PTS_OFF_TOV"].to_f, total_o_PCT_PTS_OFF_TOV, games_played )
                h_o_PCT_PTS_OFF_TOV[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_PTS_OFF_TOV"].to_f
                row[:median_o_PCT_PTS_OFF_TOV] = median( h_o_PCT_PTS_OFF_TOV )
                row[:total_o_PCT_PTS_OFF_TOV] = total_o_PCT_PTS_OFF_TOV

                if 0 == total_o_PTS
                  row[:o_PCT_PTS_PAINT] = 0
                else
                  row[:o_PCT_PTS_PAINT] = (total_o_PTS_PAINT / total_o_PTS).round(3)
                end

                total_o_PCT_PTS_PAINT, row[:mean_o_PCT_PTS_PAINT] = mean( o_boxscore_scoring[:"PCT_PTS_PAINT"].to_f, total_o_PCT_PTS_PAINT, games_played )
                h_o_PCT_PTS_PAINT[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_PTS_PAINT"].to_f
                row[:median_o_PCT_PTS_PAINT] = median( h_o_PCT_PTS_PAINT )
                row[:total_o_PCT_PTS_PAINT] = total_o_PCT_PTS_PAINT

                o_boxscore_FG2M = o_gamelog[:"FGM"].to_f - o_gamelog[:"FG3M"].to_f
                o_boxscore_AST_2PM = (o_boxscore_scoring[:"PCT_AST_2PM"].to_f * o_boxscore_FG2M).round.to_f

                total_o_AST_2PM, row[:mean_o_AST_2PM] = mean( o_boxscore_AST_2PM, total_o_AST_2PM, games_played )
                h_o_AST_2PM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_AST_2PM
                row[:median_o_AST_2PM] = median( h_o_AST_2PM )
                row[:total_o_AST_2PM] = total_o_AST_2PM

                total_o_PCT_AST_2PM, row[:mean_o_PCT_AST_2PM] = mean( o_boxscore_scoring[:"PCT_AST_2PM"].to_f, total_o_PCT_AST_2PM, games_played )
                h_o_PCT_AST_2PM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_AST_2PM"].to_f
                row[:median_o_PCT_AST_2PM] = median( h_o_PCT_AST_2PM )
                row[:total_o_PCT_AST_2PM] = total_o_PCT_AST_2PM
                if 0 == ( total_o_FGM - total_o_FG3M ).to_f
                  row[:o_PCT_AST_2PM] = 0
                else
                  row[:o_PCT_AST_2PM] = total_o_AST_2PM / ( total_o_FGM - total_o_FG3M ).to_f
                end

                o_boxscore_UAST_2PM = o_boxscore_FG2M - o_boxscore_AST_2PM

                total_o_UAST_2PM, row[:mean_o_UAST_2PM] = mean( o_boxscore_UAST_2PM, total_o_UAST_2PM, games_played )
                h_o_UAST_2PM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_UAST_2PM
                row[:median_o_UAST_2PM] = median( h_o_UAST_2PM )
                row[:total_o_UAST_2PM] = total_o_UAST_2PM
                if 0 == ( total_o_FGM - total_o_FG3M ).to_f
                  row[:o_PCT_UAST_2PM] = 0
                else
                  row[:o_PCT_UAST_2PM] = total_o_UAST_2PM / ( total_o_FGM - total_o_FG3M ).to_f
                end

                total_o_PCT_UAST_2PM, row[:mean_o_PCT_UAST_2PM] = mean( o_boxscore_scoring[:"PCT_UAST_2PM"].to_f, total_o_PCT_UAST_2PM, games_played )
                h_o_PCT_UAST_2PM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_UAST_2PM"].to_f
                row[:median_o_PCT_UAST_2PM] = median( h_o_PCT_UAST_2PM )
                row[:total_o_PCT_UAST_2PM] = total_o_PCT_UAST_2PM

                o_boxscore_AST_3PM = (o_boxscore_scoring[:"PCT_AST_3PM"].to_f * o_gamelog[:"FG3M"].to_f).round.to_f

                total_o_AST_3PM, row[:mean_o_AST_3PM] = mean( o_boxscore_AST_3PM, total_o_AST_3PM, games_played )
                h_o_AST_3PM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_AST_3PM
                row[:median_o_AST_3PM] = median( h_o_AST_3PM )
                row[:total_o_AST_3PM] = total_o_AST_3PM
                if 0 == total_FG3M
                  row[:o_PCT_AST_3PM] = 0
                else
                  row[:o_PCT_AST_3PM] = total_AST_3PM / total_FG3M
                end

                total_o_PCT_AST_3PM, row[:mean_o_PCT_AST_3PM] = mean( o_boxscore_scoring[:"PCT_AST_3PM"].to_f, total_o_PCT_AST_3PM, games_played )
                h_o_PCT_AST_3PM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_AST_3PM"].to_f
                row[:median_o_PCT_AST_3PM] = median( h_o_PCT_AST_3PM )
                row[:total_o_PCT_AST_3PM] = total_o_PCT_AST_3PM

                o_boxscore_UAST_3PM = o_gamelog[:"FG3M"].to_f - o_boxscore_AST_3PM

                total_o_UAST_3PM, row[:mean_o_UAST_3PM] = mean( o_boxscore_UAST_3PM, total_o_UAST_3PM, games_played )
                h_o_UAST_3PM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_UAST_3PM
                row[:median_o_UAST_3PM] = median( h_o_UAST_3PM )
                row[:total_o_UAST_3PM] = total_o_UAST_3PM
                if 0 == total_o_FG3M
                  row[:o_PCT_UAST_3PM] = 0
                else
                  row[:o_PCT_UAST_3PM] = total_o_UAST_3PM / total_o_FG3M
                end

                total_o_PCT_UAST_3PM, row[:mean_o_PCT_UAST_3PM] = mean( o_boxscore_scoring[:"PCT_UAST_3PM"].to_f, total_o_PCT_UAST_3PM, games_played )
                h_o_PCT_UAST_3PM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_UAST_3PM"].to_f
                row[:median_o_PCT_UAST_3PM] = median( h_o_PCT_UAST_3PM )
                row[:total_o_PCT_UAST_3PM] = total_o_PCT_UAST_3PM

                o_boxscore_AST_FGM = (o_gamelog[:"FGM"].to_f * o_boxscore_scoring[:"PCT_AST_FGM"].to_f).round.to_f

                total_o_AST_FGM, row[:mean_o_AST_FGM] = mean( o_boxscore_AST_FGM, total_o_AST_FGM, games_played )
                h_o_AST_FGM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_AST_FGM
                row[:median_o_AST_FGM] = median( h_o_AST_FGM )
                row[:total_o_AST_FGM] = total_o_AST_FGM

                total_o_PCT_AST_FGM, row[:mean_o_PCT_AST_FGM] = mean( o_boxscore_scoring[:"PCT_AST_FGM"].to_f, total_o_PCT_AST_FGM, games_played )
                h_o_PCT_AST_FGM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_AST_FGM"].to_f
                row[:median_o_PCT_AST_FGM] = median( h_o_PCT_AST_FGM )
                row[:total_o_PCT_AST_FGM] = total_o_PCT_AST_FGM
                if 0 == total_o_FGM
                  row[:o_PCT_AST_FGM] = 0
                else
                  row[:o_PCT_AST_FGM] = total_o_AST_FGM / total_o_FGM
                end

                o_boxscore_UAST_FGM = o_gamelog[:"FGM"].to_f - o_boxscore_AST_FGM
                
                total_o_UAST_FGM, row[:mean_o_UAST_FGM] = mean( o_boxscore_UAST_FGM, total_o_UAST_FGM, games_played )
                h_o_UAST_FGM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_UAST_FGM
                row[:median_o_UAST_FGM] = median( h_o_UAST_FGM )
                row[:total_o_UAST_FGM] = total_o_UAST_FGM

                total_o_PCT_UAST_FGM, row[:mean_o_PCT_UAST_FGM] = mean( o_boxscore_scoring[:"PCT_UAST_FGM"].to_f, total_o_PCT_UAST_FGM, games_played )
                h_o_PCT_UAST_FGM[ o_boxscore_scoring[:GAME_ID] ] = o_boxscore_scoring[:"PCT_UAST_FGM"].to_f
                row[:median_o_PCT_UAST_FGM] = median( h_o_PCT_UAST_FGM )
                row[:total_o_PCT_UAST_FGM] = total_o_PCT_UAST_FGM
                if 0 == total_o_FGM
                  row[:o_PCT_UAST_FGM] = 0
                else
                  row[:o_PCT_UAST_FGM] = total_o_UAST_FGM / total_o_FGM
                end

                #tracking
                total_o_DIST, row[:mean_o_DIST] = mean( o_boxscore_tracking[:"DIST"].to_f, total_o_DIST, games_played )
                h_o_DIST[ o_boxscore_tracking[:GAME_ID] ] = o_boxscore_tracking[:"DIST"].to_f
                row[:median_o_DIST] = median( h_o_DIST )
                row[:total_o_DIST] = total_o_DIST

                total_o_ORBC, row[:mean_o_ORBC] = mean( o_boxscore_tracking[:"ORBC"].to_f, total_o_ORBC, games_played )
                h_o_ORBC[ o_boxscore_tracking[:GAME_ID] ] = o_boxscore_tracking[:"ORBC"].to_f
                row[:median_o_ORBC] = median( h_o_ORBC )
                row[:total_o_ORBC] = total_o_ORBC

                total_o_RBC, row[:mean_o_RBC] = mean( o_boxscore_tracking[:"RBC"].to_f, total_o_RBC, games_played )
                h_o_RBC[ o_boxscore_tracking[:GAME_ID] ] = o_boxscore_tracking[:"RBC"].to_f
                row[:median_o_RBC] = median( h_o_RBC )
                row[:total_o_RBC] = total_o_RBC

                total_o_SAST, row[:mean_o_SAST] = mean( o_boxscore_tracking[:"SAST"].to_f, total_o_SAST, games_played )
                h_o_SAST[ o_boxscore_tracking[:GAME_ID] ] = o_boxscore_tracking[:"SAST"].to_f
                row[:median_o_SAST] = median( h_o_SAST )
                row[:total_o_SAST] = total_o_SAST

                total_o_PASS, row[:mean_o_PASS] = mean( o_boxscore_tracking[:"PASS"].to_f, total_o_PASS, games_played )
                h_o_PASS[ o_boxscore_tracking[:GAME_ID] ] = o_boxscore_tracking[:"PASS"].to_f
                row[:median_o_PASS] = median( h_o_PASS )
                row[:total_o_PASS] = total_o_PASS

                total_o_CFGM, row[:mean_o_CFGM] = mean( o_boxscore_tracking[:"CFGM"].to_f, total_o_CFGM, games_played )
                h_o_CFGM[ o_boxscore_tracking[:GAME_ID] ] = o_boxscore_tracking[:"CFGM"].to_f
                row[:median_o_CFGM] = median( h_o_CFGM )
                row[:total_o_CFGM] = total_o_CFGM

                total_o_CFG_PCT, row[:mean_o_CFG_PCT] = mean( o_boxscore_tracking[:"CFG_PCT"].to_f, total_o_CFG_PCT, games_played )
                h_o_CFG_PCT[ o_boxscore_tracking[:GAME_ID] ] = o_boxscore_tracking[:"CFG_PCT"].to_f
                row[:median_o_CFG_PCT] = median( h_o_CFG_PCT )
                row[:total_o_CFG_PCT] = total_o_CFG_PCT

                total_o_UFGA, row[:mean_o_UFGA] = mean( o_boxscore_tracking[:"UFGA"].to_f, total_o_UFGA, games_played )
                h_o_UFGA[ o_boxscore_tracking[:GAME_ID] ] = o_boxscore_tracking[:"UFGA"].to_f
                row[:median_o_UFGA] = median( h_o_UFGA )
                row[:total_o_UFGA] = total_o_UFGA

                total_o_DFGA, row[:mean_o_DFGA] = mean( o_boxscore_tracking[:"DFGA"].to_f, total_o_DFGA, games_played )
                h_o_DFGA[ o_boxscore_tracking[:GAME_ID] ] = o_boxscore_tracking[:"DFGA"].to_f
                row[:median_o_DFGA] = median( h_o_DFGA )
                row[:total_o_DFGA] = total_o_DFGA

                total_o_DFG_PCT, row[:mean_o_DFG_PCT] = mean( boxscore_tracking[:"DFG_PCT"].to_f, total_o_DFG_PCT, games_played )
                h_o_DFG_PCT[ boxscore_tracking[:GAME_ID] ] = boxscore_tracking[:"DFG_PCT"].to_f
                row[:median_o_DFG_PCT] = median( h_o_DFG_PCT )
                row[:total_o_DFG_PCT] = total_o_DFG_PCT
              end

            #DERIVED STATS
            #team = boxscore[:Team]

            if true == bTeam
              team_daily_averages = row
              team_minutes_played = team_daily_averages[:total_mins_played] * 5

              poss = team_daily_averages[:total_FGA] + 0.4 * team_daily_averages[:total_FTA] - 1.07 * (team_daily_averages[:total_OREB] / (team_daily_averages[:total_OREB] + team_daily_averages[:total_o_DREB])) * (team_daily_averages[:total_FGA] - team_daily_averages[:total_FGM]) + team_daily_averages[:total_TO] 

              o_poss = team_daily_averages[:total_o_FGA] + 0.4 * team_daily_averages[:total_o_FTA] - 1.07 * (team_daily_averages[:total_o_OREB] / (team_daily_averages[:total_o_OREB] + team_daily_averages[:total_DREB])) * (team_daily_averages[:total_o_FGA] - team_daily_averages[:total_o_FGM]) + team_daily_averages[:total_o_TO]
 
              avg_poss = (poss + o_poss) / 2

              poss_approx = row[:total_FGA] + row[:total_TO] - row[:total_OREB] + 0.4 * row[:total_FTA]

              o_poss_approx = row[:total_o_FGA] + row[:total_o_TO] - row[:total_o_OREB] + 0.4 * row[:total_o_FTA]

              poss_approx_avg = (poss_approx + o_poss_approx) / 2

              sb_poss = 0.5 * (row[:total_FGA] + row[:total_TO] - row[:total_OREB] + 0.475 * row[:total_FTA])
              o_sb_poss = 0.5 * (row[:total_o_FGA] + row[:total_o_TO] - row[:total_o_OREB] + 0.475 * row[:total_o_FTA])
              sb_poss_avg = sb_poss + o_sb_poss

              nba_poss = gamelog[:FGA].to_f + gamelog[:TOV].to_f - gamelog[:OREB].to_f + 0.436 * gamelog[:FTA].to_f
              trad_nba_poss = boxscore_traditional[:FGA].to_f + boxscore_traditional[:TO].to_f - boxscore_traditional[:OREB].to_f + 0.436 * boxscore_traditional[:FTA].to_f
              trad_nba_poss2 = boxscore_traditional[:FGA].to_f + gamelog[:TOV].to_f - boxscore_traditional[:OREB].to_f + 0.436 * boxscore_traditional[:FTA].to_f
              o_nba_poss = o_gamelog[:FGA].to_f + o_gamelog[:TOV].to_f - o_gamelog[:OREB].to_f + 0.436 * o_gamelog[:FTA].to_f
              nba_poss_avg = (nba_poss + o_nba_poss) / 2

              #team_daily_averages[:total_possessions] = 0.5 * ((team_daily_averages[:total_FGA] + 0.4 * team_daily_averages[:total_FTA] - 1.07 * (team_daily_averages[:total_OREB] / (team_daily_averages[:total_OREB] + team_daily_averages[:total_o_DREB])) * (team_daily_averages[:total_FGA] - team_daily_averages[:total_FGM]) + team_daily_averages[:total_TO]) + (team_daily_averages[:total_o_FGA] + 0.4 * team_daily_averages[:total_o_FTA] - 1.07 * (team_daily_averages[:total_o_OREB] / (team_daily_averages[:total_o_OREB] + team_daily_averages[:total_DREB])) * (team_daily_averages[:total_o_FGA] - team_daily_averages[:total_o_FGM]) + team_daily_averages[:total_o_TO]))

              #team_daily_averages[:total_o_possessions] = 0.5 * ((team_daily_averages[:total_o_FGA] + 0.4 * team_daily_averages[:total_o_FTA] - 1.07 * (team_daily_averages[:total_o_OREB] / (team_daily_averages[:total_o_OREB] + team_daily_averages[:total_DREB])) * (team_daily_averages[:total_o_FGA] - team_daily_averages[:total_o_FGM]) + team_daily_averages[:total_o_TO]) + (team_daily_averages[:total_FGA] + 0.4 * team_daily_averages[:total_FTA] - 1.07 * (team_daily_averages[:total_OREB] / (team_daily_averages[:total_OREB] + team_daily_averages[:total_o_DREB])) * (team_daily_averages[:total_FGA] - team_daily_averages[:total_FGM]) + team_daily_averages[:total_TO]))
              team_daily_averages[:total_possessions] = total_FGA + total_TO - total_OREB + 0.436 * total_FTA
              team_daily_averages[:total_o_possessions] = total_o_FGA + total_o_TO - total_o_OREB + 0.436 * total_o_FTA

              ortg = Array.new
              ortg[0] = gamelog[:PTS].to_f / row[:total_possessions]
              ortg[1] = gamelog[:PTS].to_f / poss
              ortg[2] = gamelog[:PTS].to_f / avg_poss
              ortg[3] = gamelog[:PTS].to_f / poss_approx
              ortg[4] = gamelog[:PTS].to_f / poss_approx_avg
              ortg[5] = gamelog[:PTS].to_f / sb_poss_avg
              ortg[6] = gamelog[:PTS].to_f / nba_poss
              ortg[7] = gamelog[:PTS].to_f / nba_poss_avg

              ortg_trad = boxscore_traditional[:PTS].to_f / trad_nba_poss
              ortg_trad_fix = boxscore_traditional[:PTS].to_f / trad_nba_poss2

              o_ortg = o_gamelog[:PTS].to_f / o_nba_poss

              if gamelog[:MIN].to_f > 240
                p "game_id: #{gamelog[:GAME_ID]}"
                p "gamelog overtime mins: #{gamelog[:MIN]}"
              end

              if ( 100*ortg[6] - boxscore_advanced[:OFF_RATING].to_f ).abs >= 0.2
                #binding.pry
                p "off error: calc: #{100*ortg[6]} actual: #{boxscore_advanced[:OFF_RATING]}"
              elsif ( 100*o_ortg - boxscore_advanced[:DEF_RATING].to_f ).abs >= 0.2
                #binding.pry
                p "def error: calc: #{100*o_ortg} actual: #{boxscore_advanced[:DEF_RATING]}"
              else
                #p "#{boxscore_traditional[:GAME_ID]} ok"
              end

            else
              team_daily_averages = DB[tablename].where(:team_abbreviation => gamelog[:TEAM_ABBREVIATION]).where(:date => row[:date]).entries[0]
              row[:total_possessions] = total_FGA + total_TO - total_OREB + 0.436 * total_FTA
              team_minutes_played = team_daily_averages[:total_mins_played]
            end

            row[:OREB_PCT] = 100 * ( total_OREB * ( team_minutes_played / 5 ) ) / ( total_time_played.total_minutes * ( team_daily_averages[:total_OREB] + team_daily_averages[:total_o_DREB] ) )
            row[:DREB_PCT] = 100 * ( total_DREB * ( team_minutes_played / 5 ) ) / ( total_time_played.total_minutes * ( team_daily_averages[:total_DREB] + team_daily_averages[:total_o_OREB] ) )
            row[:REB_PCT] = 100 * ( total_REB * ( team_minutes_played / 5 ) ) / ( total_time_played.total_minutes * ( team_daily_averages[:total_REB] + team_daily_averages[:total_o_REB] ) )
            #100 * AST / (((MP / (Tm MP / 5)) * Tm FG) - FG)

            if true == bTeam
              row[:AST_PCT] = ( 100 * total_AST ) / ( team_daily_averages[:total_FGM] )
            else
              row[:AST_PCT] = ( 100 * total_AST ) / (((total_time_played.total_minutes / (team_minutes_played/5)) * team_daily_averages[:total_FGM]) - total_FGM)
            end

            row[:PCT_STL] = 100 * (total_STL * (team_minutes_played / 5)) / (total_time_played.total_minutes * team_daily_averages[:total_o_possessions])
            row[:PCT_BLK] = 100 * (total_BLK * (team_minutes_played / 5)) / (total_time_played.total_minutes * (team_daily_averages[:total_o_FGA] - team_daily_averages[:total_o_FG3A])) 

            row[:TO_PCT] = 100 * total_TO / row[:total_possessions]

            row[:USG_PCT] = 100 * ((total_FGA + 0.44 * total_FTA + total_TO) * (team_minutes_played / 5)) / (total_time_played.total_minutes * (team_daily_averages[:total_FGA] + 0.44 * team_daily_averages[:total_FTA] + team_daily_averages[:total_TO]))

            if true == bTeam
              #wikipedia: 100 x Pts / (Tm FGA + .40 x Tm FTA - 1.07 x (Tm OREB / (Tm OREB + Tm DREB)) x (Tm FGA - Tm FGM) + Tm TO)
              row[:OFF_RATING] = (100 * total_PTS) / row[:total_possessions]
              row[:DEF_RATING] = (100 * total_o_PTS) / row[:total_o_possessions]
              row[:NET_RATING] = row[:OFF_RATING] - row[:DEF_RATING]
              if 0 == total_TO
                row[:AST_TO] = -1 #sentinel value for n/a
              else
                row[:AST_TO] = total_AST / total_TO
              end
              row[:AST_RATIO] = total_AST / row[:total_possessions]
              row[:TO_RATIO] = total_TO / row[:total_possessions]
              row[:PACE] = row[:total_possessions] / ( total_time_played.total_minutes / (5.0*48.0) )
              row[:PIE] = (total_PTS + total_FGM + total_FTM - total_FGA - total_FTA + total_DREB + (0.5 * total_OREB) + total_AST + total_STL + (0.5 * total_BLK) - total_PF - total_TO) / (total_PTS + total_o_PTS + total_FGM + total_o_FGM + total_FTM + total_o_FTM - total_FGA - total_o_FGA - total_FTA - total_o_FTA + total_DREB + total_o_DREB + (0.5 * (total_OREB + total_o_OREB) ) + total_AST + total_o_AST + total_STL + total_o_STL + (0.5 * (total_BLK + total_o_BLK) ) - total_PF - total_o_PF - total_TO - total_o_TO)

            else
              qAST = ((total_time_played.total_minutes / (team_minutes_played / 5)) * (1.14 * ((team_daily_averages[:total_AST] - total_AST) / team_daily_averages[:total_FGM]))) + ((((team_daily_averages[:total_AST]/ team_minutes_played) * total_time_played.total_minutes * 5 - total_AST) / ((team_daily_averages[:total_FGM] / team_minutes_played) * total_time_played.total_minutes * 5 - total_FGM)) * (1 - (total_time_played.total_minutes/ (team_minutes_played / 5))))
              if 0 == total_FGA
                fG_Part = 0
                pProd_FG_Part = 0
              else
                fG_Part = total_FGM * (1 - 0.5 * ((total_PTS - total_FTM) / (2 * total_FGA)) * qAST)
                pProd_FG_Part = 2 * (total_FGM + 0.5 * total_FG3M) * (1 - 0.5 * ((total_PTS - total_FTM) / (2 * total_FGA)) * qAST)
              end

              aST_Part = 0.5 * (((team_daily_averages[:total_PTS] - team_daily_averages[:total_FTM]) - (total_PTS - total_FTM)) / (2 * (team_daily_averages[:total_FGA] - total_FGA))) * total_AST

              if 0 == total_FTA 
                fT_Part = 0
                fTxPoss = 0
              else
                fT_Part = (1-(1-(total_FTM/total_FTA))**2)*0.4*total_FTA
                fTxPoss = ((1 - (total_FTM / total_FTA))**2) * 0.4 * total_FTA
              end

              team_Scoring_Poss = team_daily_averages[:total_FGM] + (1 - (1 - (team_daily_averages[:total_FTM] / team_daily_averages[:total_FTA]))**2) * team_daily_averages[:total_FTA] * 0.4
              team_PlayP = team_Scoring_Poss / (team_daily_averages[:total_FGA] + team_daily_averages[:total_FTA] * 0.4 + team_daily_averages[:total_TO])
              team_OREB_PCT = team_daily_averages[:total_OREB] / (team_daily_averages[:total_OREB] + (team_daily_averages[:total_OREB] - team_daily_averages[:total_o_OREB]))
              team_OREB_Weight = ((1 - team_OREB_PCT) * team_PlayP) / ((1 - team_OREB_PCT) * team_PlayP + team_OREB_PCT * (1 - team_PlayP))
              oREB_Part = total_OREB * team_OREB_Weight * team_PlayP

              scPoss = (fG_Part + aST_Part + fT_Part) * (1 - (team_daily_averages[:total_OREB] / team_Scoring_Poss) * team_OREB_Weight * team_PlayP) + oREB_Part

              fGxPoss = (total_FGA - total_FGM) * (1 - 1.07 * team_OREB_PCT)
                      
              totPoss = scPoss + fGxPoss + fTxPoss + total_TO

              pProd_AST_Part = 2 * ((team_daily_averages[:total_FGM] - total_FGM + 0.5 * (team_daily_averages[:total_FG3M] - total_FG3M)) / (team_daily_averages[:total_FGM] - total_FGM)) * 0.5 * (((team_daily_averages[:total_PTS] - team_daily_averages[:total_FTM]) - (total_PTS - total_FTM)) / (2 * (team_daily_averages[:total_FGA] - total_FGA))) * total_AST
              pProd_OREB_Part = total_OREB * team_OREB_Weight * team_PlayP * (team_daily_averages[:total_PTS] / (team_daily_averages[:total_FGM] + (1 - (1 - (team_daily_averages[:total_FTM] / team_daily_averages[:total_FTA]))**2) * 0.4 * team_daily_averages[:total_FTA]))

              pProd = (pProd_FG_Part + pProd_AST_Part + total_FTM) * (1 - (team_daily_averages[:total_OREB] / team_Scoring_Poss) * team_OREB_Weight * team_PlayP) + pProd_OREB_Part

              row[:OFF_RATING] = 100 * (pProd / totPoss)
              row[:floorP] = scPoss / totPoss
              
              #Defensive Rating
              dORP = team_daily_averages[:total_o_OREB] / (team_daily_averages[:total_o_OREB] + team_daily_averages[:total_DREB])
              dFGP = team_daily_averages[:total_o_FGM] / team_daily_averages[:total_o_FGA]
              fMwt = (dFGP * (1 - dORP)) / (dFGP * (1 - dORP) + (1 - dFGP) * dORP)
              stops1 = total_STL + total_BLK * fMwt * (1 - 1.07 * dORP) + total_DREB * (1 - fMwt)
              stops2 = (((team_daily_averages[:total_o_FGA]- team_daily_averages[:total_o_FGM]- team_daily_averages[:total_BLK]) / team_minutes_played) * fMwt * (1 - 1.07 * dORP) + ((team_daily_averages[:total_o_TO] - team_daily_averages[:total_STL]) / team_minutes_played)) * total_time_played.total_minutes + (total_PF / team_daily_averages[:total_PF]) * 0.4 * team_daily_averages[:total_o_FTA] * (1 - (team_daily_averages[:total_o_FTM] / team_daily_averages[:total_o_FTA]))**2

              stops = stops1 + stops2
              #jlk - assume opponent_MP is the same as team_MP
              stopP = (stops * team_minutes_played) / (team_daily_averages[:total_possessions] * total_time_played.total_minutes)
              team_Defensive_Rating = 100 * (team_daily_averages[:total_o_PTS] / team_daily_averages[:total_possessions])
              d_Pts_per_ScPoss = team_daily_averages[:total_o_PTS] / (team_daily_averages[:total_o_FGM] + (1 - (1 - (team_daily_averages[:total_o_FTM] / team_daily_averages[:total_o_FTA]))**2) * team_daily_averages[:total_o_FTA]*0.4)
              row[:DEF_RATING] = team_Defensive_Rating + 0.2 * (100 * d_Pts_per_ScPoss * (1 - stopP) - team_Defensive_Rating)
            end

            #Opponent DERIVED STATS

            if true == bTeam
              row[:o_OREB_PCT] = 100 * ( total_o_OREB * ( team_minutes_played / 5 ) ) / ( total_time_played.total_minutes * ( team_daily_averages[:total_o_OREB] + team_daily_averages[:total_DREB] ) )
              row[:o_DREB_PCT] = 100 * ( total_o_DREB * ( team_minutes_played / 5 ) ) / ( total_time_played.total_minutes * ( team_daily_averages[:total_o_DREB] + team_daily_averages[:total_OREB] ) )
              row[:o_REB_PCT] = 100 * ( total_o_REB * ( team_minutes_played / 5 ) ) / ( total_time_played.total_minutes * ( team_daily_averages[:total_o_REB] + team_daily_averages[:total_REB] ) )
              #100 * AST / (((MP / (Tm MP / 5)) * Tm FGM) - FGM)
              if true == bTeam
                row[:o_AST_PCT] = ( 100 * total_o_AST ) / ( team_daily_averages[:total_o_FGM] )
              else
                row[:o_AST_PCT] = ( 100 * total_o_AST ) / (((total_time_played.total_minutes / (team_minutes_played/5)) * team_daily_averages[:total_o_FGM]) - total_o_FGM)
              end
              row[:o_PCT_STL] = 100 * (total_o_STL * (team_minutes_played / 5)) / (total_time_played.total_minutes * team_daily_averages[:total_possessions])
              row[:o_PCT_BLK] = 100 * (total_o_BLK * (team_minutes_played / 5)) / (total_time_played.total_minutes * (team_daily_averages[:total_FGA] - team_daily_averages[:total_FG3A])) 

              row[:o_TO_PCT] = 100 * total_o_TO / row[:total_o_possessions]

              row[:o_USG_PCT] = 100 * ((total_o_FGA + 0.44 * total_o_FTA + total_o_TO) * (team_minutes_played / 5)) / (total_time_played.total_minutes * (team_daily_averages[:total_o_FGA] + 0.44 * team_daily_averages[:total_o_FTA] + team_daily_averages[:total_o_TO]))

              if true == bTeam
                #wikipedia: 100 x Pts / (Tm FGA + .40 x Tm FTA - 1.07 x (Tm OREB / (Tm OREB + Tm DREB)) x (Tm FGA - Tm FGM) + Tm TO)
                row[:o_OFF_RATING] = (100 * total_o_PTS) / row[:total_o_possessions]
                row[:o_DEF_RATING] = (100 * total_PTS) / row[:total_o_possessions]
                row[:o_NET_RATING] = row[:o_OFF_RATING] - row[:o_DEF_RATING]
                row[:o_AST_TO] = total_o_AST / total_o_TO
                row[:o_AST_RATIO] = total_o_AST / row[:total_o_possessions]
                row[:o_TO_RATIO] = total_o_TO / row[:total_o_possessions]
                row[:o_PACE] = row[:total_o_possessions] / ( total_time_played.total_minutes.to_f / (5.0*48.0) )
                row[:o_PIE] = (total_o_PTS + total_o_FGM + total_o_FTM - total_o_FGA - total_o_FTA + total_o_DREB + (0.5 * total_o_OREB) + total_o_AST + total_o_STL + (0.5 * total_o_BLK) - total_o_PF - total_o_TO) / (total_PTS + total_o_PTS + total_FGM + total_o_FGM + total_FTM + total_o_FTM - total_FGA - total_o_FGA - total_FTA - total_o_FTA + total_DREB + total_o_DREB + (0.5 * (total_OREB + total_o_OREB) ) + total_AST + total_o_AST + total_STL + total_o_STL + (0.5 * (total_BLK + total_o_BLK) ) - total_PF - total_o_PF - total_TO - total_o_TO)
              else
                qAST = ((total_time_played.total_minutes / (team_minutes_played / 5)) * (1.14 * ((team_daily_averages[:total_o_AST] - total_o_AST) / team_daily_averages[:total_o_FGM]))) + ((((team_daily_averages[:total_o_AST]/ team_minutes_played) * total_time_played.total_minutes * 5 - total_o_AST) / ((team_daily_averages[:total_o_FGM] / team_minutes_played) * total_time_played.total_minutes * 5 - total_o_FGM)) * (1 - (total_time_played.total_minutes/ (team_minutes_played / 5))))
                if 0 == total_o_FGA
                  fG_Part = 0
                  pProd_FG_Part = 0
                else
                  fG_Part = total_o_FGM * (1 - 0.5 * ((total_o_PTS - total_o_FTM) / (2 * total_o_FGA)) * qAST)
                  pProd_FG_Part = 2 * (total_o_FGM + 0.5 * total_o_FG3M) * (1 - 0.5 * ((total_o_PTS - total_o_FTM) / (2 * total_o_FGA)) * qAST)
                end
                aST_Part = 0.5 * (((team_daily_averages[:total_o_PTS] - team_daily_averages[:total_o_FTM]) - (total_o_PTS - total_o_FTM)) / (2 * (team_daily_averages[:total_o_FGA] - total_o_FGA))) * total_o_AST

                if 0 == total_o_FTA 
                  fT_Part = 0
                  fTxPoss = 0
                else
                  fT_Part = (1-(1-(total_o_FTM/total_o_FTA))**2)*0.4*total_o_FTA
                  fTxPoss = ((1 - (total_o_FTM / total_o_FTA))**2) * 0.4 * total_o_FTA
                end

                team_Scoring_Poss = team_daily_averages[:total_o_FGM] + (1 - (1 - (team_daily_averages[:total_o_FTM] / team_daily_averages[:total_o_FTA]))**2) * team_daily_averages[:total_o_FTA] * 0.4
                team_PlayP = team_Scoring_Poss / (team_daily_averages[:total_o_FGA] + team_daily_averages[:total_o_FTA] * 0.4 + team_daily_averages[:total_o_TO])
                team_OREB_PCT = team_daily_averages[:total_o_OREB] / (team_daily_averages[:total_o_OREB] + (team_daily_averages[:total_REB] - team_daily_averages[:total_OREB]))
                team_OREB_Weight = ((1 - team_OREB_PCT) * team_PlayP) / ((1 - team_OREB_PCT) * team_PlayP + team_OREB_PCT * (1 - team_PlayP))
                oREB_Part = total_o_OREB * team_OREB_Weight * team_PlayP

                scPoss = (fG_Part + aST_Part + fT_Part) * (1 - (team_daily_averages[:total_o_OREB] / team_Scoring_Poss) * team_OREB_Weight * team_PlayP) + oREB_Part

                fGxPoss = (total_o_FGA - total_o_FGM) * (1 - 1.07 * team_OREB_PCT)
                        
                totPoss = scPoss + fGxPoss + fTxPoss + total_o_TO

                pProd_AST_Part = 2 * ((team_daily_averages[:total_o_FGM] - total_o_FGM + 0.5 * (team_daily_averages[:total_o_FG3M] - total_o_FG3M)) / (team_daily_averages[:total_o_FGM] - total_o_FGM)) * 0.5 * (((team_daily_averages[:total_o_PTS] - team_daily_averages[:total_o_FTM]) - (total_o_PTS - total_o_FTM)) / (2 * (team_daily_averages[:total_o_FGA] - total_o_FGA))) * total_o_AST
                pProd_OREB_Part = total_o_OREB * team_OREB_Weight * team_PlayP * (team_daily_averages[:total_o_PTS] / (team_daily_averages[:total_o_FGM] + (1 - (1 - (team_daily_averages[:total_o_FTM] / team_daily_averages[:total_o_FTA]))**2) * 0.4 * team_daily_averages[:total_o_FTA]))

                pProd = (pProd_FG_Part + pProd_AST_Part + total_o_FTM) * (1 - (team_daily_averages[:total_o_OREB] / team_Scoring_Poss) * team_OREB_Weight * team_PlayP) + pProd_OREB_Part

                row[:o_OFF_RATING] = 100 * (pProd / totPoss)
                row[:ofloorP] = scPoss / totPoss
                
                #Defensive Rating
                dORP = team_daily_averages[:total_OREB] / (team_daily_averages[:total_OREB] + team_daily_averages[:total_o_DREB])
                dFGP = team_daily_averages[:total_FGM] / team_daily_averages[:total_FGA]
                fMwt = (dFGP * (1 - dORP)) / (dFGP * (1 - dORP) + (1 - dFGP) * dORP)
                stops1 = total_o_STL + total_BLK * fMwt * (1 - 1.07 * dORP) + total_o_DREB * (1 - fMwt)
                stops2 = (((team_daily_averages[:total_FGA]- team_daily_averages[:total_FGM]- team_daily_averages[:total_BLK]) / team_minutes_played) * fMwt * (1 - 1.07 * dORP) + ((team_daily_averages[:total_TO] - team_daily_averages[:total_o_STL]) / team_minutes_played)) * total_time_played.total_minutes + (total_o_PF / team_daily_averages[:total_o_PF]) * 0.4 * team_daily_averages[:total_FTA] * (1 - (team_daily_averages[:total_FTM] / team_daily_averages[:total_FTA]))**2

                stops = stops1 + stops2
                #jlk - assume opponent_MP is the same as team_MP
                stopP = (stops * team_minutes_played) / (team_daily_averages[:total_o_possessions] * total_time_played.total_minutes)
                team_Defensive_Rating = 100 * (team_daily_averages[:total_PTS] / team_daily_averages[:total_o_possessions])
                d_Pts_per_ScPoss = team_daily_averages[:total_PTS] / (team_daily_averages[:total_FGM] + (1 - (1 - (team_daily_averages[:total_FTM] / team_daily_averages[:total_FTA]))**2) * team_daily_averages[:total_FTA]*0.4)
                row[:o_DEF_RATING] = team_Defensive_Rating + 0.2 * (100 * d_Pts_per_ScPoss * (1 - stopP) - team_Defensive_Rating)
              end
            end
            #todo: calculate opponent derived stats
            #clean up code
            #check all columns right for 1 game for team
            #check all columns right for 1 game for player
            #check all columns right for 1 season for team
            #check all columns right for 1 season for player
            #Data validation: caleb
            #splits for: home/away games, back-to-backs, lots of rest, 3 in 4 nights, 5 in 7, etc., recent performance (last 3g, 5g, 10g)
            #parse salary and historical data from fanduel
            #html rendering
            #automated scripts
            #write objective functions for all major stats
            #test framework for psat seasons: cross validation and stuff
            #tag players based on position and height and weight
            #test framework for current season w/ salary info
            #investigate questions and quantify insights!
            #get data and update db every day!
            #reconcile w/ kerry's real world data
            #use R functions for machine learing, linear regressions

            last_boxscore = row
            cur_date = row[:date]

            begin
              database[tablename].insert(row.to_hash)
            rescue StandardError => e
              binding.pry
              p "hi"
            end
            #p "boxscore #{boxscore_index} saved"
            if (num_boxscores - 1) == i
              binding.pry
              p "hi"
            end
          }

          last_boxscore_date = last_boxscore[:date]
          while cur_date < season_end
            cur_date = cur_date + 1

            last_boxscore[:date] = cur_date
            last_boxscore[:date_of_data] = last_boxscore_date

            database[tablename].insert(last_boxscore.to_hash)
          end
          p "team #{team} done"
        }
        p "type: #{type}"
      }
      p "season: #{season}"
    }
#=end
end

def leaders( season_sym, category, limit, date )
  DB[season_sym].select(category).select(:Player).where(:date => date).where(:Position => "PG").order(category).entries
end
binding.pry

launchSqliteConsole() if OPTIONS[:sqlite_console] 
launchConsole(DB) if OPTIONS[:irb_console] || ! OPTIONS[:output]

__END__
"year","name","percent","sex"
1880,"John",0.081541,"boy"
1880,"William",0.080511,"boy"
1880,"James",0.050057,"boy"
1880,"Charles",0.045167,"boy"
1880,"George",0.043292,"boy"
1880,"Frank",0.02738,"boy"
1880,"Joseph",0.022229,"boy"
1880,"Thomas",0.021401,"boy"
1880,"Henry",0.020641,"boy"
