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
require 'json'
require 'net/http'
require 'thread'
require 'monitor'
require './data_types'
require 'socket'
#require './get_betting_lines_old'

seasons_h = { 
  #"season", "day 1", "reg season end + 1", "playoffs end + 1"
  #"2018-19" => [ "2018-10-16", "2019-4-11", "2019-6-19" ],
  #"2013-14" => [ "2013-10-29", "2014-4-17", "2014-6-16" ],
  #"2011-12" => [ "2011-12-25", "2012-4-27", "2012-6-22" ],
  #"2017-18" => [ "2017-10-17", "2018-4-12", "2018-6-18" ],
  "2010-11" => [ "2010-10-26", "2011-4-14", "2011-6-13" ],
=begin
  "2016-17" => [ "2016-10-25", "2017-4-13", "2017-6-13" ],
  "2015-16" => [ "2015-10-27", "2016-4-14", "2016-6-20" ],
  "2014-15" => [ "2014-10-28", "2015-4-16", "2015-6-17" ],
#=begin
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
#=end
=end
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
  opt :num_threads,  "number of threads", :type => :integer
  opt :player_skip, "player to skip to", :type => :integer
  opt :player_id_skip, "player id to skip to", :type => :string
  opt :day_skip, "day to skip to", :type => :string
  opt :season_range, "season range", :type => :string
  opt :entity, "team or player", :type => :string
  opt :stat_type, "tracking or daily", :type => :string
  opt :season, "season", :type => :string
  opt :thread_index, "thread index", :type => :integer
  opt :calculate_averages, "calculate averages", :type => :integer
  opt :serverIP, "server IP", :type => :string
  opt :cookie, "cookie (web)", :type => :string
  opt :includetodaygames, "include today's games", :type => :string
  opt :startyesterday, "start yest", :type => :string
  opt :fanduelsalaryfile, "fanduel salary file", :type => :string
end

def getDatabase(filename)
  puts "Connecting to sqlite://#{filename}"
  database = Sequel.sqlite(filename)
  #database = Sequel.connect("jdbc:sqlite:#{filename}")
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
  database.drop_table? tablename
  database.create_table tablename do
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
  database.drop_table? tablename
  database.create_table tablename do
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

PLAYER_SKIP = nil; NUM_THREADS = nil; THREAD_INDEX = nil;SERVERIP = nil;

if OPTIONS[:output]
  DB_PATH = OPTIONS[:output]
  OPTIONS[:season] = OPTIONS[:output].split(".")[0].gsub(/_part\d/,"").gsub("_","-")
else
  DB_TMP = Tempfile.new(['csv2sqlite','.sqlite3'])
  DB_PATH = DB_TMP.path
end

if OPTIONS[:season]
  season_index = -1
  seasons_h.keys.each_with_index{|season,i|
    if season == OPTIONS[:season]
      season_index = i
      break
    end
  }
  if -1 == season_index
    p "error 1"
    exit
  else
    p "season index is #{season_index}"
  end
  SEASON = OPTIONS[:season]
  seasons_h = { SEASON => seasons_h[SEASON] }
end

if OPTIONS[:num_threads]
  NUM_THREADS = OPTIONS[:num_threads]
  if !OPTIONS[:thread_index]
    hostname = Socket.gethostname
    THREAD_INDEX = hostname.split("-").last.to_i % 30
  end
end

if OPTIONS[:thread_index]
  THREAD_INDEX = OPTIONS[:thread_index]
end

if OPTIONS[:serverIP]
  SERVERIP = OPTIONS[:serverIP]
end

if OPTIONS[:player_skip]
  PLAYER_SKIP = OPTIONS[:player_skip]
  p "PLAYER_SKIP: #{PLAYER_SKIP}"
else
  p "no PLAYER_SKIP"
end

if OPTIONS[:player_id_skip]
  player_id_skip = OPTIONS[:player_id_skip]
  p "player_id_skip: #{player_id_skip}"
else
  p "no player_id_skip"
end

team_skip = PLAYER_SKIP
team_id_skip = player_id_skip

if OPTIONS[:day_skip]
  day_skip = Date.parse( OPTIONS[:day_skip] )
  p "day_skip: #{day_skip}"
  test_index = -1
  seasons_a.each_with_index{|season,index|
    season_dates = season[1]
    if (day_skip >= Date.parse(season_dates[0])) and (day_skip < Date.parse(season_dates[1]))
      test_index = index
      p "season index is: #{test_index}"
      break
    end

    if "no" != season_dates[2]
      if (day_skip >= Date.parse(season_dates[1])) and (day_skip < Date.parse(season_dates[2]))
        test_index = index
        p "season index is: #{test_index}"
      end
    end
  }
  if -1 != test_index
    season_index = test_index
  end
else
  p "no day_skip"
end

if OPTIONS[:season_range]
  season_range = OPTIONS[:season_range]
  s_range = season_range.split("...").map{|d| Integer(d)}
end

if OPTIONS[:entity]
  entity = OPTIONS[:entity]
end

if OPTIONS[:stat_type]
  stat_type = OPTIONS[:stat_type]
end

def getPreviousSeason( season )
  beg_season_i = season.split("-")[0].to_i
  end_season_i = season.split("-")[1].to_i

  beg_season_i = beg_season_i - 1
  if 0 == end_season_i
    beg_season_i.to_s + "_99"
  else
    end_season_i = end_season_i - 1
    beg_season_i.to_s + "_" + end_season_i.to_s
  end
end
database = getDatabase(DB_PATH)
DB1 = nil; DB2 = nil; DB3 = nil;
prev1_season = nil; prev2_season = nil; prev3_season = nil;

prev1_season = getPreviousSeason( SEASON )
if seasons_h[ prev1_season ] and File.exists?("#{prev1_season}.db")
  DB1 = getDatabase("#{prev1_season}.db")

  prev2_season = getPreviousSeason( prev1_season )
  if seasons_h[ prev2_season ] and File.exists?("#{prev2_season}.db")
    DB2 = getDatabase(DB_PATH)

    prev3_season = getPreviousSeason( prev2_season )
    if seasons_h[ prev3_season ] and File.exists?("#{prev3_season}.db")
      DB3 = getDatabase(DB_PATH)
    end
  end
end

def divide( num, den )
  if 0 == den
    return 0.0
  else
    return num / den
  end
end

def median( hash )
  begin
    array = hash.sort_by{|k,v| v}
    len = array.length
    center = len / 2
    med = len % 2 ? array[center][1].to_f : ( array[center][1].to_f + array[center+1][1].to_f ) / 2.to_f
  rescue StandardError => e
    binding.pry
    p "hi"
  end
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

def addSecondsPlayed( database, season, type, season_start_date_str, season_end_date_str )
  average_types = [ "opponent vs starter PG", "opponent vs starter SG", "opponent vs starter SF", "opponent vs starter PF", "opponent vs starter C" ]

  average_types.each{|type_text|
    p "average_type: #{type_text}"

    date = Date.parse( season_start_date_str )
    season_end_date = Date.parse( season_end_date_str )

    while date < season_end_date
      p "date: #{date.to_s}"

      date_str = date.strftime("%Y-%m-%d")

      tablename = "_" + season + " " + type + " daily averages"
      tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
      team_averages_on_date = database[:"#{tablename}"].select_all.where(:date => date_str, :average_type => type_text).order(:team_abbreviation).entries
      team_averages_on_date = team_averages_on_date.uniq{|team_average| team_average[:team_abbreviation]}

      total_seconds = 0
      team_averages_on_date.each_with_index{|team_average,team_index|
        total_seconds = total_seconds + team_average[:seconds_played_mean].to_f
      }
      num_teams = team_averages_on_date.size
      
      row = Hash.new
      row[:team_mean_SECONDS] = divide( total_seconds, num_teams )
      average_type = type_text.gsub(/'/,"")

      #write to DB
      begin
        new_tablename = "_#{season}_#{type}_daily_team_averages"
        new_tablename = new_tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
        rows_updated = database[new_tablename].select_all.where(:date => date, :average_type => average_type).update( row )
      rescue StandardError => e
        binding.pry
        p "hi"
      end

      date = date + 1
    end
  }
end

def calculateDailyTeamAverages( database, season, type, season_start_date_str, season_end_date_str )

  new_tablename = "_#{season}_#{type}_daily_team_averages"
  new_tablename = new_tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

  options = { :headers    => true,
              :header_converters => nil,
              :converters => :nil  }

  if !database.table_exists? new_tablename
    puts "Dropping and re-creating table #{new_tablename}"
    database.drop_table? new_tablename
    database.create_table new_tablename do
      # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
      # primary_key :id
      # Float :price
      column :date, :text
      column :average_type, :text
      column :team_mean_pace, :text
      column :team_mean_def_rtg, :text
      column :team_mean_off_rtg, :text
      column :team_mean_PTS, :text
      column :team_mean_SECONDS, :text
      column :team_mean_FG_PCT, :text
      column :team_mean_OREB, :text
      column :team_mean_OREB_PCT, :text
      column :team_mean_DREB, :text
      column :team_mean_DREB_PCT, :text
      column :team_mean_STL, :text
      column :team_mean_PCT_STL, :text
      column :team_mean_BLK, :text
      column :team_mean_PCT_BLK, :text
      column :team_mean_AST, :text
      column :team_mean_AST_PCT, :text
      column :team_mean_AST_RATIO, :text
      column :team_mean_TOV, :text
      column :team_mean_TOV_PCT, :text

      column :team_mean_FG3A, :text
      column :team_mean_FG3M, :text
      column :team_mean_FG3_PCT, :text
      column :team_mean_PF, :text
      column :team_mean_PFD, :text
      column :team_mean_FTA, :text
      column :team_mean_TS_PCT, :text
      column :team_mean_pct_pts_2pt, :text
      column :team_mean_pct_pts_2pt_mr, :text
      column :team_mean_pct_pts_3pt, :text
      column :team_mean_pct_pts_ft, :text
      column :team_mean_PTS_OFF_TOV, :text
      column :team_mean_PTS_2ND_CHANCE, :text
      column :team_mean_PTS_FB, :text
      column :team_mean_PTS_PAINT, :text
      column :team_mean_PCT_CFGA, :text
      column :team_mean_PCT_CFGM, :text
      column :team_mean_CFG_PCT, :text
      column :team_mean_PCT_UFGA, :text
      column :team_mean_PCT_UFGM, :text
      column :team_mean_UFG_PCT, :text
    end
  end
  
  tablename = "_" + season + " " + type + " daily averages"
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

  #average_types = [ "null", "opponent vs starter PG", "opponent vs starter SG", "opponent vs starter SF", "opponent vs starter PF", "opponent vs starter C" ]
  average_types = [ nil, "opponent vs starter PG", "opponent vs starter SG", "opponent vs starter SF", "opponent vs starter PF", "opponent vs starter C" ]

  average_types.each{|type_text|
    p "average_type: #{type_text}"

    date = Date.parse( season_start_date_str )
    season_end_date = Date.parse( season_end_date_str )

    while date < season_end_date
      p "date: #{date.to_s}"

    date_str = date.strftime("%Y-%m-%d")
    team_averages_on_date = nil

    if type_text
      team_averages_on_date = database[:"#{tablename}"].select_all.where(:date => date_str, :average_type => type_text).order(:team_abbreviation).entries
    else
      team_averages_on_date = database[:"#{tablename}"].select_all.where(:date => date_str, :player_name => nil, :average_type => type_text).order(:team_abbreviation).entries
    end
      team_averages_on_date = team_averages_on_date.uniq{|team_average| team_average[:team_abbreviation]}

      total_pace = 0

      total_def_rtg = 0
      total_off_rtg = 0

      total_PTS = 0
      total_SECONDS = 0
      total_FG_PCT = 0
      total_DREB = 0
      total_DREB_PCT = 0
      total_OREB = 0
      total_OREB_PCT = 0
      total_STL = 0
      total_PCT_STL = 0
      total_AST = 0
      total_AST_PCT = 0
      total_AST_RATIO = 0
      total_BLK = 0
      total_PCT_BLK = 0
      total_TOV = 0
      total_TOV_PCT = 0

      total_FG3A = 0
      total_FG3M = 0
      total_PF = 0
      total_PFD = 0
      total_FTA = 0
      total_TS_PCT = 0
      total_PCT_PTS_2PT = 0
      total_PCT_PTS_2PT_MR = 0
      total_PCT_PTS_3PT = 0
      total_PCT_PTS_FT = 0
      total_PTS_OFF_TOV = 0
      total_PTS_2ND_CHANCE = 0
      total_PTS_FB = 0
      total_PTS_PAINT = 0

      total_CFGM = 0
      total_CFGA = 0
      total_UFGM = 0
      total_UFGA = 0

      total_seconds = 0

      team_averages_on_date.each_with_index{|team_average,team_index|

        total_pace = total_pace + team_average[:PACE].to_f
        total_def_rtg = total_def_rtg + team_average[:DEF_RATING].to_f
        total_off_rtg = total_off_rtg + team_average[:OFF_RATING].to_f

        total_PTS = total_PTS + team_average[:PTS_mean].to_f
        total_PTS = total_SECONDS + team_average[:seconds_played_mean].to_f
        total_FG_PCT = total_FG_PCT + team_average[:FG_PCT].to_f
        total_OREB = total_OREB + team_average[:OREB_mean].to_f
        total_DREB = total_DREB + team_average[:DREB_mean].to_f

        total_OREB_PCT = total_OREB_PCT + team_average[:OREB_PCT].to_f
        total_DREB_PCT = total_DREB_PCT + team_average[:DREB_PCT].to_f

        total_AST = total_AST + team_average[:AST_mean].to_f
        total_AST_PCT = total_AST_PCT + team_average[:AST_PCT].to_f
        total_AST_RATIO = total_AST_RATIO + team_average[:AST_RATIO].to_f
        total_BLK = total_BLK + team_average[:BLK_mean].to_f
        total_PCT_BLK = total_PCT_BLK + team_average[:PCT_BLK].to_f
        total_STL = total_STL + team_average[:STL_mean].to_f
        total_PCT_STL = total_PCT_STL + team_average[:PCT_STL].to_f
        total_TOV = total_TOV + team_average[:TOV_mean].to_f
        total_TOV_PCT = total_TOV_PCT + team_average[:TO_PCT].to_f

        total_FG3A = total_FG3A + team_average[:FG3A_mean].to_f
        total_FG3M = total_FG3M + team_average[:FG3M_mean].to_f
        total_PF = total_PF + team_average[:PF_mean].to_f
        total_PFD = total_PFD + team_average[:PFD_mean].to_f
        total_FTA = total_FTA + team_average[:FTA_mean].to_f
        total_TS_PCT = total_TS_PCT + team_average[:TS_PCT].to_f
        total_PCT_PTS_2PT = total_PCT_PTS_2PT + team_average[:PCT_PTS_2PT].to_f
        total_PCT_PTS_2PT_MR = total_PCT_PTS_2PT_MR + team_average[:PCT_PTS_2PT_MR].to_f
        total_PCT_PTS_3PT = total_PCT_PTS_3PT + team_average[:PCT_PTS_3PT].to_f
        total_PCT_PTS_FT = total_PCT_PTS_FT + team_average[:PCT_PTS_FT].to_f
        total_PTS_OFF_TOV = total_PTS_OFF_TOV + team_average[:PTS_OFF_TOV_mean].to_f
        total_PTS_2ND_CHANCE = total_PTS_2ND_CHANCE + team_average[:PTS_2ND_CHANCE_mean].to_f
        total_PTS_FB = total_PTS_FB + team_average[:PTS_FB_mean].to_f
        total_PTS_PAINT = total_PTS_PAINT + team_average[:PTS_PAINT_mean].to_f
        total_CFGA = total_CFGA + team_average[:CFGA_mean].to_f
        total_CFGM = total_CFGM + team_average[:CFGM_mean].to_f
        total_UFGA = total_UFGA + team_average[:UFGA_mean].to_f
        total_UFGM = total_UFGM + team_average[:UFGM_mean].to_f

        total_seconds = total_seconds + team_average[:seconds_played_mean].to_f
      }
      num_teams = team_averages_on_date.size
    
      row = Hash.new
      row[:date] = date_str
      row[:team_mean_PTS] = divide( total_PTS, num_teams )
      row[:team_mean_SECONDS] = divide( total_SECONDS, num_teams )
      row[:team_mean_FG_PCT] = divide( total_FG_PCT, num_teams )
      row[:team_mean_pace] = divide( total_pace, num_teams )
      row[:team_mean_def_rtg] = divide( total_def_rtg, num_teams )
      row[:team_mean_off_rtg] = divide( total_off_rtg, num_teams )
      row[:team_mean_OREB] = divide( total_OREB, num_teams )
      row[:team_mean_OREB_PCT] = divide( total_OREB_PCT, num_teams )
      row[:team_mean_DREB] = divide( total_DREB, num_teams )
      row[:team_mean_DREB_PCT] = divide( total_DREB_PCT, num_teams )
      row[:team_mean_STL] = divide( total_STL, num_teams )
      row[:team_mean_PCT_STL] = divide( total_PCT_STL, num_teams )
      row[:team_mean_BLK] = divide( total_BLK, num_teams )
      row[:team_mean_PCT_BLK] = divide( total_PCT_BLK, num_teams )
      row[:team_mean_AST] = divide( total_AST, num_teams )
      row[:team_mean_AST_PCT] = divide( total_AST_PCT, num_teams )
      row[:team_mean_AST_RATIO] = divide( total_AST_RATIO, num_teams )
      row[:team_mean_TOV] = divide( total_TOV, num_teams )
      row[:team_mean_TOV_PCT] = divide( total_TOV_PCT, num_teams )
      row[:team_mean_FG3A] = divide( total_FG3A, num_teams )
      row[:team_mean_FG3M] = divide( total_FG3M, num_teams )
      row[:team_mean_FG3_PCT] = divide( divide( total_FG3M, total_FG3A ), num_teams )
      row[:team_mean_PF] = divide( total_PF, num_teams )
      row[:team_mean_PFD] = divide( total_PFD, num_teams )
      row[:team_mean_FTA] = divide( total_FTA, num_teams )
      row[:team_mean_TS_PCT] = divide( total_TS_PCT, num_teams )
      row[:team_mean_PCT_PTS_2PT] = divide( total_PCT_PTS_2PT, num_teams )
      row[:team_mean_PCT_PTS_2PT_MR] = divide( total_PCT_PTS_2PT_MR, num_teams )
      row[:team_mean_PCT_PTS_3PT] = divide( total_PCT_PTS_3PT, num_teams )
      row[:team_mean_PCT_PTS_FT] = divide( total_PCT_PTS_FT, num_teams )
      row[:team_mean_PTS_OFF_TOV] = divide( total_PTS_OFF_TOV, num_teams )
      row[:team_mean_PTS_2ND_CHANCE] = divide( total_PTS_2ND_CHANCE, num_teams )
      row[:team_mean_PTS_FB] = divide( total_PTS_FB, num_teams )
      row[:team_mean_PTS_PAINT] = divide( total_PTS_PAINT, num_teams )
      #row[:team_mean_PCT_CFGA] = divide( PCT_CFGA, num_teams )
      #row[:team_mean_PCT_CFGM] = divide( PCT_CFGM, num_teams )
      #row[:team_mean_CFG_PCT] = divide( CFG_PCT, num_teams )
      row[:team_mean_PCT_CFGA] = divide( divide( total_CFGA, total_CFGA + total_UFGA ), num_teams )
      row[:team_mean_PCT_CFGM] = divide( divide( total_CFGM, total_CFGM + total_UFGM ), num_teams )
      row[:team_mean_CFG_PCT] = divide( divide( total_CFGM, total_CFGA ), num_teams )
      #row[:team_mean_PCT_UFGA] = divide( PCT_UFGA, num_teams )
      #row[:team_mean_PCT_UFGM] = divide( PCT_UFGM, num_teams )
      #row[:team_mean_UFG_PCT] = divide( UFG_PCT, num_teams )
      row[:team_mean_PCT_UFGA] = divide( divide( total_UFGA, total_UFGA + total_UFGA ), num_teams )
      row[:team_mean_PCT_UFGM] = divide( divide( total_UFGM, total_UFGM + total_UFGM ), num_teams )
      row[:team_mean_UFG_PCT] = divide( divide( total_UFGM, total_UFGA ), num_teams )

      row[:team_mean_SECONDS] = divide( total_seconds, num_teams )

      if nil == type_text
        row[:average_type] = nil
      else
        row[:average_type] = type_text.gsub(/'/,"")
      end

      #write to DB
      begin
        database[new_tablename].insert(row)
      rescue StandardError => e
        binding.pry
        p "hi"
      end

      date = date + 1
    end
    
  }
end

def bExtraRest( date, i, player_boxscores )
  bExtraRest = 0
  if i > 0 
    if ( player_boxscores[i-1][:DATE] and (date - Date.parse( player_boxscores[i-1][:DATE] )) > 2 ) or
      ( player_boxscores[i-1][:GAME_DATE] and (date - Date.parse( player_boxscores[i-1][:GAME_DATE] )) > 2 )
      bExtraRest = 1
    end
  else
    bExtraRest = 0
  end

  return bExtraRest 
end

def b3games4nights( date, i, player_boxscores )
  b3g4d = 0
  if i > 1 
    if ( player_boxscores[i-2][:DATE] and (date - Date.parse( player_boxscores[i-2][:DATE])) < 4 ) or
      ( player_boxscores[i-2][:GAME_DATE] and (date - Date.parse( player_boxscores[i-2][:GAME_DATE])) < 4 )
      b3g4d = 1
    end
  else
    b3g4d = 0
  end

  return b3g4d
end

def bGameYesterday( date, i, player_boxscores )
  bGameYesterday = 0
  if i > 0 
    if (player_boxscores[i-1][:DATE] and ( 1 == (date - Date.parse( player_boxscores[i-1][:DATE] ) ) ) ) or
      ( player_boxscores[i-1][:GAME_DATE] and ( 1 == (date - Date.parse( player_boxscores[i-1][:GAME_DATE] ) ) ) )
      bGameYesterday = 1
    end
  else
    bGameYesterday = 0
  end

  return bGameYesterday
end

#match up the team and player boxscore indices
def syncTeamAndPlayerBoxscores( i, player_boxscores, j, team_gamelogs )
  if (player_boxscores.size - 1) != i and (team_gamelogs.size - 1) != j
    player_game_date = Date.parse( player_boxscores[i][:DATE] )

    while player_game_date != Date.parse( team_gamelogs[j][:GAME_DATE] )
      j = j + 1
      if team_gamelogs.size == j
        binding.pry
        p "major problem reached end of array"
      end
    end
  end

  return j
end

def checkIfPlayerChangedTeams( database, season, type, i, player_boxscores, j, team_gamelogs, team_abbr )
  begin
  if player_boxscores[i][:TEAM_ABBREVIATION] != team_abbr #all_team_gamelogs[ team_abbr ][j][:TEAM_ABBREVIATION]
    team_abbr = player_boxscores[i][:TEAM_ABBREVIATION]
    p "player changed teams.  new team: #{team_abbr}"

    team_gamelogs = database[:"#{season.gsub(/-/,"_")}_#{type}_gamelogs"].select_all.where(:TEAM_ABBREVIATION => team_abbr).order(:Game_ID).entries
    j = 0
    j = syncTeamAndPlayerBoxscores( i, player_boxscores, j, team_gamelogs )
  end

  rescue StandardError => e
    binding.pry
    p 'hi'
  end
  return j, team_gamelogs, team_abbr
end

def bGameTomorrow( date, j, team_gamelogs )
  bGameTomorrow = 0

  if (team_gamelogs.size - 1) != j 
    if ( 1 == (Date.parse( team_gamelogs[j+1][:GAME_DATE] ) - date) ) 
      p "front_b2b"
      bGameTomorrow = 1
    end
  end

  return bGameTomorrow
end

def getPosition( database, season, date, player_id )
  position = ""
  if database.table_exists? :"fanduelinfo"
    fd_entries = database[:"fanduelinfo"].select_all.where(:date => date.strftime("%Y%m%d"), :player_id => player_id).entries
  end
  if fd_entries and fd_entries.size > 0
    position = fd_entries.first[:Pos]

    if "PG" != position and "SG" != position and "SF" != position and "PF" != position and "C" != position
      all_entries = database[:"fanduelinfo"].select_all.where(:player_id => player_id).entries

      hPositions = Hash.new
      all_entries.each{|entry|
        if "PG"==entry[:Pos] or "SG"==entry[:Pos] or "SF"==entry[:Pos] or "PF"==entry[:Pos] or "C"==entry[:Pos]
          hPositions[entry[:Pos]] = (hPositions[entry[:Pos]] ? hPositions[entry[:Pos]] : 0) + 1
        end
      }

      if hPositions.size > 0
        p "changed position from #{position} to #{hPositions.sort.last[0]} #{hPositions}"
        position = hPositions.sort.last[0]
      else
        binding.pry
        p "hi"
      end
    end
  elsif fd_entries
    fd_entries = database[:"fanduelinfo"].select_all.where(:player_id => player_id).entries
    if fd_entries.size > 0
      position = fd_entries.first[:Pos]
    else
      entries = database[:"fanduelinfo"].select_all.where(:date => date.strftime("%Y%m%d"), :player_id => player_id).entries
      if entries > 0
        position = entries[0][:Pos]
      else
        binding.pry
        p "error"
      end
    end
  end

  if "NA" == position or "" == position
    bio_entries = database[:"#{season.gsub(/-/,"_")}_bioinfo"].select_all.where(:player_id => player_id).entries
    if bio_entries and bio_entries.size > 0 and bio_entries.first[:pos]
      position = bio_entries.first[:pos]
    elsif bio_entries and bio_entries.size > 0 and bio_entries.first[:Pos]
      position = bio_entries.first[:Pos]
    else
      #jlk - binding.pry
      p "no bio for #{player_id}"
      p "no bio for #{player_id}"
      p "no bio for #{player_id}"
      p "no bio for #{player_id}"
      p "no bio for #{player_id}"
    end
  end

  return position
end

def getOpponentAbbrAndLocation( season_year, gamelog, boxscore )
  opponent_abbr = nil
  location = nil
  if 2 == gamelog[:MATCHUP].split("@").size and convertBBRTeamAbbr2( gamelog[:MATCHUP].split("@")[0].gsub(" ",""), season_year ).match( boxscore[:TEAM_ABBREVIATION] )
    location = "away"
  opponent_abbr = convertBBRTeamAbbr2( gamelog[:MATCHUP].split("@")[1].gsub(/\s/, ""), season_year )
  elsif 2 == gamelog[:MATCHUP].split("vs.").size and convertBBRTeamAbbr2( gamelog[:MATCHUP].split("vs.")[0].gsub(" ",""), season_year ).match( boxscore[:TEAM_ABBREVIATION] )
    location = "home"
  opponent_abbr = convertBBRTeamAbbr2( gamelog[:MATCHUP].split("vs.")[1].gsub(/\s/, ""), season_year )
  else
    binding.pry
    p "more than 2 arguments"
  end

  return opponent_abbr, location
end

def createVegasLinesTable( database, season, type )
  new_tablename = "_#{season}_#{type}_vegas_lines"
  new_tablename = new_tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

  options = { :headers    => true,
              :header_converters => nil,
              :converters => :nil  }

  if !database.table_exists? new_tablename
    puts "Dropping and re-creating table #{new_tablename}"
    database.drop_table? new_tablename
    database.create_table new_tablename do
      column :gameDate, :text
      column :nbaGameID, :text
      column :over_under_mean, :text
      column :point_spread_mean, :text
      column :est_vegas_team_PTS, :text
      column :est_vegas_opp_PTS, :text
      column :team_abbreviation, :text
      bookies = database[ :"#{type}_bettinglines"].distinct.select(:bookname).entries
      bookies.each{|bookie|
        book = bookie[:bookname]
        column :"over_under_#{book}", :text
        column :"point_spread_#{book}", :text
        column :"est_vegas_team_PTS_#{book}", :text
        column :"est_vegas_opp_PTS_#{book}", :text
      }
    end
  end
end
  
def calculateAverageOverUnder( database, season, type, overUnders, team_abbr )
  totalOverUnder = 0
  totalPointSpread = 0
  row = Hash.new

  begin
  overUnders.entries.each{|entry|
    totalPointSpread = totalPointSpread + entry[:point_spread].to_f
    totalOverUnder = totalOverUnder + entry[:over_under].to_f

    #linesHash[ entry[:bookname] ] = entry[:point_spread].to_f
    #ouHash[ entry[:bookname] ] = entry[:over_under].to_f

    homePts = (entry[:over_under].to_f - entry[:point_spread].to_f) / 2
    awayPts = homePts + entry[:point_spread].to_f

    row[:gameDate] = entry[:gameDate]
    row[:nbaGameID] = entry[:nbaGameID]

    if team_abbr == entry[:home]
      row[:"over_under_#{entry[:bookname]}"] = entry[:over_under].to_f
      row[:"point_spread_#{entry[:bookname]}"] = entry[:point_spread].to_f
      row[:"est_vegas_team_PTS_#{entry[:bookname]}"] = homePts
      row[:"est_vegas_opp_PTS_#{entry[:bookname]}"] = awayPts
    else
      row[:"over_under_#{entry[:bookname]}"] = entry[:over_under].to_f
      row[:"point_spread_#{entry[:bookname]}"] = -entry[:point_spread].to_f
      row[:"est_vegas_team_PTS_#{entry[:bookname]}"] = awayPts
      row[:"est_vegas_opp_PTS_#{entry[:bookname]}"] = homePts
    end
  }
  rescue StandardError => e
    binding.pry
    p 'hi'
  end
  avgPointSpread = divide( totalPointSpread, overUnders.entries.size )
  avgOverUnder = divide( totalOverUnder, overUnders.entries.size )

  homePts = ( avgOverUnder - avgPointSpread ) / 2
  awayPts = homePts + avgPointSpread
  if overUnders.entries.size > 0
    if team_abbr == overUnders.entries.first[:home]
      row[:over_under_mean] = avgOverUnder
      row[:point_spread_mean] = avgPointSpread
      row[:est_vegas_team_PTS] = homePts
      row[:est_vegas_opp_PTS] = awayPts
    else
      row[:over_under_mean] = avgOverUnder
      row[:point_spread_mean] = -avgPointSpread
      row[:est_vegas_team_PTS] = awayPts
      row[:est_vegas_opp_PTS] = homePts
    end

    row[:team_abbreviation] = team_abbr

    begin
      tablename = "_#{season}_#{type}_vegas_lines"
      tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
      rows_updated = database[ :"#{tablename}" ].insert( row )
    rescue StandardError => e
      binding.pry
      p 'hi'
    end
  else

  end

end

def calculateAveragePoints( database, seasons_h, season, type, player, season_start_date )
  #go through all games in the season, and predict the player's scoring output for each one based on a trailing formula
  #retrieve the league_average for the date we are requesting
  tablename = season + "_#{type}_traditional_TeamStats"
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
  teams = database[ :"#{tablename}" ].distinct.select(:TEAM_ID).entries

  tablename = "_" + season + " " + type + " daily averages"
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
  
  player_averages = database[:"#{tablename}"].select_all.where(:date => :date_of_data, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => nil).order(:DATE).entries

  player_averages_home = database[:"#{tablename}"].select_all.where(:date => :date_of_data, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "home").order(:DATE).entries
  player_averages_away = database[:"#{tablename}"].select_all.where(:date => :date_of_data, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "away").order(:DATE).entries
  player_averages_starter = database[:"#{tablename}"].select_all.where(:date => :date_of_data, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "starter").order(:DATE).entries

  player_averages_bench = database[:"#{tablename}"].select_all.where(:date => :date_of_data, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "bench").order(:DATE).entries

  #player_averages_prev = database[:"#{tablename}"].select_all.where(:date => :date_of_data, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => nil).order(:DATE).entries
  player_averages_prev2 = database[:"#{tablename}"].select_all.where(:date => :date_of_data, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "prev2").order(:DATE).entries
  player_averages_prev5 = database[:"#{tablename}"].select_all.where(:date => :date_of_data, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "prev5").order(:DATE).entries

  #jlk - reconcile player_name and player_id
  if "regularseason" == type
    season_type = "2"
  else
    season_type = "3"
  end
  season_year = season.split("-")[0]
  
  season_id = season_type + season_year

  tablename = "#{season}_#{type}_traditional_PlayerStats"
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
  player_boxscores = database[:"#{tablename}"].select_all.where(:PLAYER_NAME => player[:PLAYER_NAME]).order(:Game_ID).entries.reject{|box|(box[:MIN] == nil) or box[:GAME_ID].match(/^003/)}

  num_games = player_boxscores.size
  if player_boxscores.size != player_averages.size
    binding.pry
    p "error 2"
  end
  if 0 == player_boxscores.size
    return
  end

  j = 0

  tablename = "_" + season + " " + type + " daily averages"
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

  team_abbr = nil
  new_team_abbr = nil
  opponent_abbr = nil

  player_id = player_boxscores[0][:PLAYER_ID]

  p "starting player_boxscores"

  num_reg_games = 0;
  total_b2b_PTS = 0; total_b2b_OREB = 0; total_b2b_OREB_PCT = 0; total_b2b_DREB = 0; total_b2b_DREB_PCT = 0; total_b2b_STL = 0; total_b2b_AST = 0; total_b2b_BLK = 0; total_b2b_TOV = 0; total_b2b_SECONDS = 0; num_b2b_games = 0; total_non_b2b_PTS = 0; total_non_b2b_OREB = 0; total_non_b2b_OREB_PCT = 0; total_non_b2b_DREB = 0; total_non_b2b_DREB_PCT = 0; total_non_b2b_STL = 0; total_non_b2b_AST = 0; total_non_b2b_BLK = 0; total_non_b2b_TOV = 0; total_non_b2b_SECONDS = 0; num_non_b2b_games = 0; num_b2b_opp_games = 0; num_non_b2b_opp_games = 0; num_threeg4d_games = 0;num_extra_rest_games = 0;num_opp_threeg4d_games = 0;num_opp_extra_rest_games = 0;

  total_threeg4d_PTS = 0; total_threeg4d_OREB = 0; total_threeg4d_OREB_PCT = 0; total_threeg4d_DREB = 0; total_threeg4d_DREB_PCT = 0; total_threeg4d_STL = 0; total_threeg4d_AST = 0; total_threeg4d_BLK = 0; total_threeg4d_TOV = 0; total_threeg4d_SECONDS = 0; num_threeg4d_games = 0; total_extra_rest_PTS = 0; total_extra_rest_OREB = 0; total_extra_rest_OREB_PCT = 0; total_extra_rest_DREB = 0; total_extra_rest_DREB_PCT = 0; total_extra_rest_STL = 0; total_extra_rest_AST = 0; total_extra_rest_BLK = 0; total_extra_rest_TOV = 0; total_extra_rest_SECONDS = 0; num_extra_rest_games = 0
  total_opp_threeg4d_PTS = 0; total_opp_threeg4d_OREB = 0; total_opp_threeg4d_OREB_PCT = 0; total_opp_threeg4d_DREB = 0; total_opp_threeg4d_DREB_PCT = 0; total_opp_threeg4d_STL = 0; total_opp_threeg4d_AST = 0; total_opp_threeg4d_BLK = 0; total_opp_threeg4d_TOV = 0; total_opp_threeg4d_SECONDS = 0; num_opp_threeg4d_games = 0; total_opp_extra_rest_PTS = 0; total_opp_extra_rest_OREB = 0; total_opp_extra_rest_OREB_PCT = 0; total_opp_extra_rest_DREB = 0; total_opp_extra_rest_DREB_PCT = 0; total_opp_extra_rest_STL = 0; total_opp_extra_rest_AST = 0; total_opp_extra_rest_BLK = 0; total_opp_extra_rest_TOV = 0; total_opp_extra_rest_SECONDS = 0; num_opp_extra_rest_games = 0
  total_front_b2b_PTS = 0; total_front_b2b_OREB = 0; total_front_b2b_OREB_PCT = 0; total_front_b2b_DREB = 0; total_front_b2b_DREB_PCT = 0; total_front_b2b_STL = 0; total_front_b2b_AST = 0; total_front_b2b_BLK = 0; total_front_b2b_TOV = 0; total_front_b2b_SECONDS = 0; num_front_b2b_games = 0; total_non_front_b2b_PTS = 0; total_non_front_b2b_OREB = 0; total_non_front_b2b_OREB_PCT = 0; total_non_front_b2b_DREB = 0; total_non_front_b2b_DREB_PCT = 0; total_non_front_b2b_STL = 0; total_non_front_b2b_AST = 0; total_non_front_b2b_BLK = 0; total_non_front_b2b_TOV = 0; total_non_front_b2b_SECONDS = 0; num_non_front_b2b_games = 0; 
  total_opp_b2b_PTS = 0; total_opp_b2b_OREB = 0; total_opp_b2b_OREB_PCT = 0; total_opp_b2b_DREB = 0; total_opp_b2b_DREB_PCT = 0; total_opp_b2b_STL = 0; total_opp_b2b_AST = 0; total_opp_b2b_BLK = 0; total_opp_b2b_TOV = 0; total_opp_b2b_SECONDS = 0; num_opp_b2b_games = 0; 

  total_opp_front_b2b_PTS = 0; total_opp_front_b2b_OREB = 0; total_opp_front_b2b_OREB_PCT = 0; total_opp_front_b2b_DREB = 0; total_opp_front_b2b_DREB_PCT = 0; total_opp_front_b2b_STL = 0; total_opp_front_b2b_AST = 0; total_opp_front_b2b_BLK = 0; total_opp_front_b2b_TOV = 0; total_opp_front_b2b_SECONDS = 0; num_opp_front_b2b_games = 0; 
  total_opp_non_front_b2b_PTS = 0; total_opp_non_front_b2b_OREB = 0; total_opp_non_front_b2b_OREB_PCT = 0; total_opp_non_front_b2b_DREB = 0; total_opp_non_front_b2b_DREB_PCT = 0; total_opp_non_front_b2b_STL = 0; total_opp_non_front_b2b_AST = 0; total_opp_non_front_b2b_BLK = 0; total_opp_non_front_b2b_TOV = 0; total_opp_non_front_b2b_SECONDS= 0; num_opp_non_front_b2b_games = 0; 
  total_opp_non_b2b_PTS = 0; total_opp_non_b2b_OREB = 0; total_opp_non_b2b_OREB_PCT = 0; total_opp_non_b2b_DREB = 0; total_opp_non_b2b_DREB_PCT = 0; total_opp_non_b2b_STL = 0; total_opp_non_b2b_AST = 0; total_opp_non_b2b_BLK = 0; total_opp_non_b2b_TOV = 0; total_opp_non_b2b_SECONDS = 0; num_opp_non_b2b_games = 0; 

  team_averages = Array.new
  team_averages_home = Array.new
  team_averages_away = Array.new
  team_abbr = player_boxscores.first[:TEAM_ABBREVIATION]

  entries_vegas = database[:"_#{season.gsub(/-/,"_")}_#{type}_vegas_lines"].select_all.where(:team_abbreviation => team_abbr).entries
  vegas_averages = Hash.new
  entries_vegas.each{|entry|
    #use the date before the game
    date = Date.parse( entry[:gameDate] )
    date = date - 1

    vegas_averages[ date.to_s ] = entry
  }

  entries = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].select_all.where(:team_abbreviation => team_abbr, :player_name => nil, :average_type => nil).entries
  team_averages = Hash.new
  entries.each{|entry|
    team_averages[ entry[:date] ] = entry
  }

  entries_home = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].select_all.where(:team_abbreviation => team_abbr, :player_name => nil, :average_type => "home").entries
  team_averages_home = Hash.new
  entries_home.each{|entry|
    team_averages_home[ entry[:date] ] = entry
  }

  entries_away = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].select_all.where(:team_abbreviation => team_abbr, :player_name => nil, :average_type => "away").entries
  team_averages_away = Hash.new
  entries_away.each{|entry|
    team_averages_away[ entry[:date] ] = entry
  }

  team_gamelogs = database[:"#{season.gsub(/-/,"_")}_#{type}_gamelogs"].select_all.where(:TEAM_ABBREVIATION => team_abbr).order(:Game_ID).entries

  t0 = Time.now
  player_boxscores.each_with_index{|boxscore,i|
    #first game won't have a prediction
    t0_loop = Time.now

    begin
      date = Date.parse( boxscore[:DATE] )
      average_date = date - 1 #use previous day's averages

      starter = (boxscore[:START_POSITION] and "" != boxscore[:START_POSITION]) ? 1 : 0
      #jlk todo
      gamelog = database[ :"#{season.gsub(/-/,"_")}_#{type}_gamelogs" ].where(:TEAM_ABBREVIATION => boxscore[:TEAM_ABBREVIATION]).where(:GAME_ID => boxscore[:GAME_ID]).entries[0]
      opponent_abbr, location = getOpponentAbbrAndLocation( season_year, gamelog, player_boxscores[i] )

      entries = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].select_all.where(:team_abbreviation => opponent_abbr, :player_name => nil, :average_type => nil, :date => average_date.to_s).entries
      opp_average = entries[0]

      entries_home = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].select_all.where(:team_abbreviation => opponent_abbr, :player_name => nil, :average_type => "home", :date => average_date.to_s).entries
      opp_average_home = entries_home[0]

      entries_away = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].select_all.where(:team_abbreviation => opponent_abbr, :player_name => nil, :average_type => "away", :date => average_date.to_s).entries
      opp_average_away = entries_away[0]
      
      actual_SECONDS = Duration.new( :minutes => boxscore[:MIN].split(":")[0], :seconds => boxscore[:MIN].split(":")[1] ).total

      j, team_gamelogs, new_team_abbr = checkIfPlayerChangedTeams( database, season, type, i, player_boxscores, j, team_gamelogs, team_abbr )

      if new_team_abbr != team_abbr
        p "changed from #{team_abbr} to #{new_team_abbr}"
        team_abbr = new_team_abbr
        entries = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].select_all.where(:team_abbreviation => team_abbr, :player_name => nil, :average_type => nil).entries
        team_averages = Hash.new
        entries.each{|entry|
          team_averages[ entry[:date] ] = entry
        }

        entries_home = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].select_all.where(:team_abbreviation => team_abbr, :player_name => nil, :average_type => "home").entries
        team_averages_home = Hash.new
        entries_home.each{|entry|
          team_averages_home[ entry[:date] ] = entry
        }

        entries_away = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].select_all.where(:team_abbreviation => team_abbr, :player_name => nil, :average_type => "away").entries
        team_averages_away = Hash.new
        entries_away.each{|entry|
          team_averages_away[ entry[:date] ] = entry
        }
      end

      vegas_average = vegas_averages[ average_date.to_s ]
      team_average = team_averages[ average_date.to_s ]
      team_average_home = team_averages_home[ average_date.to_s ]
      team_average_away = team_averages_away[ average_date.to_s ]

      j = syncTeamAndPlayerBoxscores( i, player_boxscores, j, team_gamelogs )
      front_b2b = bGameTomorrow( date, j, team_gamelogs )
      b2b = bGameYesterday( date, i, player_boxscores )
      threeg4d = b3games4nights( date, i, player_boxscores )
      extra_rest = bExtraRest( date, i, player_boxscores ) #jlk todo - filter out comebacks from injury (negative!)

      opp_game_index = syncTeamAndPlayerBoxscores( i, player_boxscores, 0, team_gamelogs )
      opp_front_b2b = bGameTomorrow( date, opp_game_index, team_gamelogs )
      opp_b2b = bGameYesterday( date, opp_game_index, team_gamelogs )
      opp_threeg4d = b3games4nights( date, opp_game_index, team_gamelogs )
      opp_extra_rest = bExtraRest( date, opp_game_index, team_gamelogs )

      position = getPosition( database, season, date, player_id )
      league_average = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_team_averages"].where(:average_type => nil, :date => average_date.to_s).entries[0]

      league_average_v_position = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_team_averages"].where(:average_type => "opponent vs starter #{position}").where(:date => average_date.to_s).entries[0]
      opp_average_v_position = database[:"_#{season.gsub(/-/,"_")}_#{type}_daily_averages"].where(:opponent_against_abbr => opponent_abbr, :average_type => "opponent vs starter #{position}").where(:date => average_date.to_s).entries[0]

      if i > 0
        previous_average = player_averages[i-1]
        previous_average_home = player_averages_home[i-1]
        previous_average_away = player_averages_away[i-1]
        previous_average_starter = player_averages_starter[i-1]
        previous_average_bench = player_averages_bench[i-1]
        previous_average_prev2 = player_averages_prev2[i-1]
        previous_average_prev5 = player_averages_prev5[i-1]

        average_seconds = previous_average[:seconds_played_mean].to_f

        prev_PCT_CFGA = divide( previous_average[:CFGA_total].to_f,  (previous_average[:CFGA_total].to_f + previous_average[:UFGA_total].to_f) )
        prev_PCT_CFGM = divide( previous_average[:CFGM_total].to_f,  (previous_average[:CFGM_total].to_f + previous_average[:UFGM_total].to_f) )

        if opp_average
          opp_o_PCT_CFGA = divide( opp_average[:o_CFGA_total].to_f,  (opp_average[:o_CFGA_total].to_f + opp_average[:o_UFGA_total].to_f) )
          opp_o_PCT_CFGM = divide( opp_average[:o_CFGM_total].to_f,  (opp_average[:o_CFGM_total].to_f + opp_average[:o_UFGM_total].to_f) )
        end
        if opp_average_v_position
          opp_o_PCT_CFGA_v_position = divide( opp_average_v_position[:CFGA_total].to_f,  (opp_average_v_position[:CFGA_total].to_f + opp_average_v_position[:UFGA_total].to_f) )
          opp_o_PCT_CFGM_v_position = divide( opp_average_v_position[:CFGM_total].to_f,  (opp_average_v_position[:CFGM_total].to_f + opp_average_v_position[:UFGM_total].to_f) )
        end

        prev_PCT_UFGA = divide( previous_average[:UFGA_total].to_f,  (previous_average[:CFGA_total].to_f + previous_average[:UFGA_total].to_f) )
        prev_PCT_UFGM = divide( previous_average[:UFGM_total].to_f,  (previous_average[:CFGM_total].to_f + previous_average[:UFGM_total].to_f) )
        if opp_average
          opp_o_PCT_UFGA = divide( opp_average[:o_UFGA_total].to_f,  (opp_average[:o_CFGA_total].to_f + opp_average[:o_UFGA_total].to_f) )
        end
        if opp_average_v_position
          opp_o_PCT_UFGA_v_position = divide( opp_average_v_position[:UFGA_total].to_f,  (opp_average_v_position[:CFGA_total].to_f + opp_average_v_position[:UFGA_total].to_f) )
        end
      end

      expected_pace = league_average[:team_mean_pace].to_f
      expected_pace2 = league_average[:team_mean_pace].to_f
      expected_pace3 = league_average[:team_mean_pace].to_f
      if team_average and opp_average
        expected_pace = (team_average[:PACE].to_f + opp_average[:PACE].to_f) / 2
        expected_pace2 = (team_average[:PACE].to_f - league_average[:team_mean_pace].to_f) + (opp_average[:PACE].to_f - league_average[:team_mean_pace].to_f) + league_average[:team_mean_pace].to_f
        expected_pace3 = 1 / ( ( 1 / team_average[:PACE].to_f ) + ( 1 / opp_average[:PACE].to_f ) - ( 1 / league_average[:team_mean_pace].to_f ) )
      end

      pace_ratio = divide( expected_pace, team_average[:PACE].to_f )
      expected_PTS_pace = previous_average[:PTS_mean].to_f * pace_ratio
      expected_OREB_pace = previous_average[:OREB_mean].to_f * pace_ratio
      expected_DREB_pace = previous_average[:DREB_mean].to_f * pace_ratio
      expected_AST_pace = previous_average[:AST_mean].to_f * pace_ratio
      expected_TOV_pace = previous_average[:TOV_mean].to_f * pace_ratio
      expected_BLK_pace = previous_average[:BLK_mean].to_f * pace_ratio
      expected_STL_pace = previous_average[:STL_mean].to_f * pace_ratio

      expected_PTS_pace_per_min = expected_PTS_pace / average_seconds
      expected_OREB_pace_per_min = expected_OREB_pace / average_seconds
      expected_DREB_pace_per_min = expected_DREB_pace / average_seconds
      expected_AST_pace_per_min = expected_AST_pace / average_seconds
      expected_TOV_pace_per_min = expected_TOV_pace / average_seconds
      expected_BLK_pace_per_min = expected_BLK_pace / average_seconds
      expected_STL_pace_per_min = expected_STL_pace / average_seconds

      pts_pace_effect = expected_PTS_pace - previous_average[:PTS_mean].to_f
      oreb_pace_effect = expected_OREB_pace - previous_average[:OREB_mean].to_f
      dreb_pace_effect = expected_DREB_pace - previous_average[:DREB_mean].to_f
      ast_pace_effect = expected_AST_pace - previous_average[:AST_mean].to_f
      tov_pace_effect = expected_TOV_pace - previous_average[:TOV_mean].to_f
      blk_pace_effect = expected_BLK_pace - previous_average[:BLK_mean].to_f
      stl_pace_effect = expected_STL_pace - previous_average[:STL_mean].to_f

      pts_pace_effect_per_min = divide(60*pts_pace_effect, average_seconds)
      oreb_pace_effect_per_min = divide(60*oreb_pace_effect, average_seconds)
      dreb_pace_effect_per_min = divide(60*dreb_pace_effect, average_seconds)
      ast_pace_effect_per_min = divide(60*ast_pace_effect, average_seconds)
      tov_pace_effect_per_min = divide(60*tov_pace_effect, average_seconds)
      blk_pace_effect_per_min = divide(60*blk_pace_effect, average_seconds)
      stl_pace_effect_per_min = divide(60*stl_pace_effect, average_seconds)

      #notoroious (Hollinger)
      pace_ratio2 = divide( expected_pace2, team_average[:PACE].to_f )
      expected_PTS_pace2 = previous_average[:PTS_mean].to_f * pace_ratio2
      expected_OREB_pace2 = previous_average[:OREB_mean].to_f * pace_ratio2
      expected_DREB_pace2 = previous_average[:DREB_mean].to_f * pace_ratio2
      expected_AST_pace2 = previous_average[:AST_mean].to_f * pace_ratio2
      expected_TOV_pace2 = previous_average[:TOV_mean].to_f * pace_ratio2
      expected_BLK_pace2 = previous_average[:BLK_mean].to_f * pace_ratio2
      expected_STL_pace2 = previous_average[:STL_mean].to_f * pace_ratio2

      expected_PTS_pace2_per_min = expected_PTS_pace2 / average_seconds
      expected_OREB_pace2_per_min = expected_OREB_pace2 / average_seconds
      expected_DREB_pace2_per_min = expected_DREB_pace2 / average_seconds
      expected_AST_pace2_per_min = expected_AST_pace2 / average_seconds
      expected_TOV_pace2_per_min = expected_TOV_pace2 / average_seconds
      expected_BLK_pace2_per_min = expected_BLK_pace2 / average_seconds
      expected_STL_pace2_per_min = expected_STL_pace2 / average_seconds

      pts_pace2_effect = expected_PTS_pace2 - previous_average[:PTS_mean].to_f
      oreb_pace2_effect = expected_OREB_pace2 - previous_average[:OREB_mean].to_f
      dreb_pace2_effect = expected_DREB_pace2 - previous_average[:DREB_mean].to_f
      ast_pace2_effect = expected_AST_pace2 - previous_average[:AST_mean].to_f
      tov_pace2_effect = expected_TOV_pace2 - previous_average[:TOV_mean].to_f
      blK_pace2_effect = expected_BLK_pace2 - previous_average[:BLK_mean].to_f
      stl_pace2_effect = expected_STL_pace2 - previous_average[:STL_mean].to_f

      pts_pace2_effect_per_min = divide(60*pts_pace2_effect, average_seconds)
      oreb_pace2_effect_per_min = divide(60*oreb_pace2_effect, average_seconds)
      dreb_pace2_effect_per_min = divide(60*dreb_pace2_effect, average_seconds)
      ast_pace2_effect_per_min = divide(60*ast_pace2_effect, average_seconds)
      tov_pace2_effect_per_min = divide(60*tov_pace2_effect, average_seconds)
      blk_pace2_effect_per_min = divide(60*blk_pace2_effect, average_seconds)
      stl_pace2_effect_per_min = divide(60*stl_pace2_effect, average_seconds)

      #hoop-ball pace
      pace_ratio3 = divide( expected_pace3, team_average[:PACE].to_f )
      expected_PTS_pace3 = previous_average[:PTS_mean].to_f * pace_ratio3
      expected_OREB_pace3 = previous_average[:OREB_mean].to_f * pace_ratio3
      expected_DREB_pace3 = previous_average[:DREB_mean].to_f * pace_ratio3
      expected_AST_pace3 = previous_average[:AST_mean].to_f * pace_ratio3
      expected_TOV_pace3 = previous_average[:TOV_mean].to_f * pace_ratio3
      expected_BLK_pace3 = previous_average[:BLK_mean].to_f * pace_ratio3
      expected_STL_pace3 = previous_average[:STL_mean].to_f * pace_ratio3

      expected_PTS_pace3_per_min = expected_PTS_pace3 / average_seconds
      expected_OREB_pace3_per_min = expected_OREB_pace3 / average_seconds
      expected_DREB_pace3_per_min = expected_DREB_pace3 / average_seconds
      expected_AST_pace3_per_min = expected_AST_pace3 / average_seconds
      expected_TOV_pace3_per_min = expected_TOV_pace3 / average_seconds
      expected_BLK_pace3_per_min = expected_BLK_pace3 / average_seconds
      expected_STL_pace3_per_min = expected_STL_pace3 / average_seconds

      pts_pace3_effect = expected_PTS_pace3 - previous_average[:PTS_mean].to_f
      oreb_pace3_effect = expected_OREB_pace3 - previous_average[:OREB_mean].to_f
      dreb_pace3_effect = expected_DREB_pace3 - previous_average[:DREB_mean].to_f
      ast_pace3_effect = expected_AST_pace3 - previous_average[:AST_mean].to_f
      tov_pace3_effect = expected_TOV_pace3 - previous_average[:TOV_mean].to_f
      blk_pace3_effect = expected_BLK_pace3 - previous_average[:BLK_mean].to_f
      stl_pace3_effect = expected_STL_pace3 - previous_average[:STL_mean].to_f

      pts_pace3_effect_per_min = divide(60*pts_pace3_effect, average_seconds)
      oreb_pace3_effect_per_min = divide(60*oreb_pace3_effect, average_seconds)
      dreb_pace3_effect_per_min = divide(60*dreb_pace3_effect, average_seconds)
      ast_pace3_effect_per_min = divide(60*ast_pace3_effect, average_seconds)
      tov_pace3_effect_per_min = divide(60*tov_pace3_effect, average_seconds)
      blk_pace3_effect_per_min = divide(60*blk_pace3_effect, average_seconds)
      stl_pace3_effect_per_min = divide(60*stl_pace3_effect, average_seconds)

      opponent_def_rtg = (opp_average and opp_average[:DEF_RATING]) ? opp_average[:DEF_RATING].to_f : 0.0
      if league_average[:team_mean_def_rtg]
        def_rtg_ratio = divide( opponent_def_rtg, league_average[:team_mean_def_rtg].to_f )

        expected_PTS_def_rtg = previous_average[:PTS_mean].to_f * def_rtg_ratio
        expected_OREB_def_rtg = previous_average[:OREB_mean].to_f * def_rtg_ratio
        expected_DREB_def_rtg = previous_average[:DREB_mean].to_f * def_rtg_ratio #sketchy?
        expected_AST_def_rtg = previous_average[:AST_mean].to_f * def_rtg_ratio
        expected_TOV_def_rtg = previous_average[:TOV_mean].to_f * def_rtg_ratio
        expected_BLK_def_rtg = previous_average[:BLK_mean].to_f * def_rtg_ratio #sketchy?
        expected_STL_def_rtg = previous_average[:STL_mean].to_f * def_rtg_ratio #sketchy?

        expected_PTS_def_rtg_per_min = divide(60*previous_average[:PTS_mean].to_f * def_rtg_ratio, average_seconds)
        expected_OREB_def_rtg_per_min = divide(60*previous_average[:OREB_mean].to_f * def_rtg_ratio, average_seconds)
        expected_DREB_def_rtg_per_min = divide(60*previous_average[:DREB_mean].to_f * def_rtg_ratio, average_seconds) #sketchy
        expected_AST_def_rtg_per_min = divide(60*previous_average[:AST_mean].to_f * def_rtg_ratio, average_seconds)
        expected_TOV_def_rtg_per_min = divide(60*previous_average[:TOV_mean].to_f * def_rtg_ratio, average_seconds)
        expected_BLK_def_rtg_per_min = divide(60*previous_average[:BLK_mean].to_f * def_rtg_ratio, average_seconds) #sketchy
        expected_STL_def_rtg_per_min = divide(60*previous_average[:STL_mean].to_f * def_rtg_ratio, average_seconds) #sketchy

        def_rtg_effect = expected_PTS_def_rtg - previous_average[:PTS_mean].to_f
        def_rtg_OREB_effect = expected_OREB_def_rtg - previous_average[:OREB_mean].to_f
        def_rtg_DREB_effect = expected_DREB_def_rtg - previous_average[:DREB_mean].to_f
        def_rtg_AST_effect = expected_AST_def_rtg - previous_average[:AST_mean].to_f
        def_rtg_TOV_effect = expected_TOV_def_rtg - previous_average[:TOV_mean].to_f
        def_rtg_BLK_effect = expected_BLK_def_rtg - previous_average[:BLK_mean].to_f
        def_rtg_STL_effect = expected_STL_def_rtg - previous_average[:STL_mean].to_f

        def_rtg_effect_per_min = divide(60*def_rtg_effect, average_seconds)
        def_rtg_OREB_effect_per_min = divide(60*def_rtg_OREB_effect, average_seconds)
        def_rtg_DREB_effect_per_min = divide(60*def_rtg_DREB_effect, average_seconds)
        def_rtg_AST_effect_per_min = divide(60*def_rtg_AST_effect, average_seconds)
        def_rtg_TOV_effect_per_min = divide(60*def_rtg_TOV_effect, average_seconds)
        def_rtg_BLK_effect_per_min = divide(60*def_rtg_BLK_effect, average_seconds)
        def_rtg_STL_effect_per_min = divide(60*def_rtg_STL_effect, average_seconds)
      end

      opponent_off_rtg = (opp_average and opp_average[:OFF_RATING]) ? opp_average[:OFF_RATING].to_f : 0.0
      if league_average[:team_mean_off_rtg]
        off_rtg_ratio = divide( opponent_off_rtg, league_average[:team_mean_off_rtg].to_f )

        expected_PTS_off_rtg = previous_average[:PTS_mean].to_f * off_rtg_ratio
        expected_OREB_off_rtg = previous_average[:OREB_mean].to_f * off_rtg_ratio
        expected_DREB_off_rtg = previous_average[:DREB_mean].to_f * off_rtg_ratio #sketchy?
        expected_AST_off_rtg = previous_average[:AST_mean].to_f * off_rtg_ratio
        expected_TOV_off_rtg = previous_average[:TOV_mean].to_f * off_rtg_ratio
        expected_BLK_off_rtg = previous_average[:BLK_mean].to_f * off_rtg_ratio #sketchy?
        expected_STL_off_rtg = previous_average[:STL_mean].to_f * off_rtg_ratio #sketchy?

        expected_PTS_off_rtg_per_min = divide(60*previous_average[:PTS_mean].to_f * off_rtg_ratio, average_seconds)
        expected_OREB_off_rtg_per_min = divide(60*previous_average[:OREB_mean].to_f * off_rtg_ratio, average_seconds)
        expected_DREB_off_rtg_per_min = divide(60*previous_average[:DREB_mean].to_f * off_rtg_ratio, average_seconds) #sketchy
        expected_AST_off_rtg_per_min = divide(60*previous_average[:AST_mean].to_f * off_rtg_ratio, average_seconds)
        expected_TOV_off_rtg_per_min = divide(60*previous_average[:TOV_mean].to_f * off_rtg_ratio, average_seconds)
        expected_BLK_off_rtg_per_min = divide(60*previous_average[:BLK_mean].to_f * off_rtg_ratio, average_seconds) #sketchy
        expected_STL_off_rtg_per_min = divide(60*previous_average[:STL_mean].to_f * off_rtg_ratio, average_seconds) #sketchy

        off_rtg_PTS_effect = expected_PTS_off_rtg - previous_average[:PTS_mean].to_f
        off_rtg_OREB_effect = expected_OREB_off_rtg - previous_average[:OREB_mean].to_f
        off_rtg_DREB_effect = expected_DREB_off_rtg - previous_average[:DREB_mean].to_f
        off_rtg_AST_effect = expected_AST_off_rtg - previous_average[:AST_mean].to_f
        off_rtg_TOV_effect = expected_TOV_off_rtg - previous_average[:TOV_mean].to_f
        off_rtg_BLK_effect = expected_BLK_off_rtg - previous_average[:BLK_mean].to_f
        off_rtg_STL_effect = expected_STL_off_rtg - previous_average[:STL_mean].to_f

        off_rtg_PTS_effect_per_min = divide(60*off_rtg_effect, average_seconds)
        off_rtg_OREB_effect_per_min = divide(60*off_rtg_OREB_effect, average_seconds)
        off_rtg_DREB_effect_per_min = divide(60*off_rtg_DREB_effect, average_seconds)
        off_rtg_AST_effect_per_min = divide(60*off_rtg_AST_effect, average_seconds)
        off_rtg_TOV_effect_per_min = divide(60*off_rtg_TOV_effect, average_seconds)
        off_rtg_BLK_effect_per_min = divide(60*off_rtg_BLK_effect, average_seconds)
        off_rtg_STL_effect_per_min = divide(60*off_rtg_STL_effect, average_seconds)
      end
#################
      mean_b2b_PTS = divide( total_b2b_PTS.to_f, num_b2b_games )
      mean_non_b2b_PTS = divide( total_non_b2b_PTS.to_f, num_non_b2b_games )
      mean_front_b2b_PTS = divide( total_front_b2b_PTS.to_f, num_front_b2b_games )
      mean_non_front_b2b_PTS = divide( total_non_front_b2b_PTS.to_f, num_non_front_b2b_games )
      mean_threeg4d_PTS = divide( total_threeg4d_PTS.to_f, num_threeg4d_games )
      mean_extra_rest_PTS = divide( total_extra_rest_PTS.to_f, num_extra_rest_games )
      mean_opp_b2b_PTS = divide( total_opp_b2b_PTS.to_f, num_opp_b2b_games )
      mean_opp_non_b2b_PTS = divide( total_opp_non_b2b_PTS.to_f, num_opp_non_b2b_games )
      mean_opp_front_b2b_PTS = divide( total_opp_front_b2b_PTS.to_f, num_opp_front_b2b_games )
      mean_opp_non_front_b2b_PTS = divide( total_opp_non_front_b2b_PTS.to_f, num_opp_non_front_b2b_games )
      mean_opp_threeg4d_PTS = divide( total_opp_threeg4d_PTS.to_f, num_opp_threeg4d_games )
      mean_opp_extra_rest_PTS = divide( total_opp_extra_rest_PTS.to_f, num_opp_extra_rest_games )

      opponent_def_rtg_v_position = (opp_average_v_position and opp_average_v_position[:DEF_RATING].to_f) ? opp_average_v_position[:DEF_RATING].to_f : 0.0
      if league_average_v_position
        def_rtg_v_position_ratio = divide( opponent_def_rtg_v_position, league_average_v_position[:team_mean_def_rtg].to_f )
        expected_PTS_def_rtg_v_position = previous_average[:PTS_mean].to_f * def_rtg_ratio
        expected_PTS_def_rtg_v_position_per_min = divide(60*expected_PTS_def_rtg_v_position, average_seconds)
        def_rtg_v_position_effect = expected_PTS_def_rtg_v_position - previous_average[:PTS_mean].to_f
        def_rtg_v_position_effect_per_min = divide(60*def_rtg_v_position_effect, average_seconds)
      end

      mean_b2b_OREB = divide( total_b2b_OREB.to_f, num_b2b_games )
      mean_b2b_OREB_PCT = divide( total_b2b_OREB_PCT.to_f, num_b2b_games )
      mean_front_b2b_OREB = divide( total_front_b2b_OREB.to_f, num_front_b2b_games )
      #mean_front_b2b_OREB_PCT = divide( total_front_b2b_OREB_PCT.to_f, num_front_b2b_games )
      #mean_non_front_b2b_OREB = divide( total_non_front_b2b_OREB.to_f, num_non_front_b2b_games )
      #mean_non_front_b2b_OREB_PCT = divide( total_non_front_b2b_OREB_PCT.to_f, num_non_front_b2b_games )
      #mean_non_b2b_OREB = divide( total_non_b2b_OREB.to_f, num_non_b2b_games )
      #mean_non_b2b_OREB_PCT = divide( total_non_b2b_OREB_PCT.to_f, num_non_b2b_games )
      #mean_threeg4d_OREB = divide( total_threeg4d_OREB.to_f, num_threeg4d_games )
      #mean_threeg4d_OREB_PCT = divide( total_threeg4d_OREB_PCT.to_f, num_threeg4d_games )
      mean_extra_rest_OREB = divide( total_extra_rest_OREB.to_f, num_extra_rest_games )
      mean_extra_rest_OREB_PCT = divide( total_extra_rest_OREB_PCT.to_f, num_extra_rest_games )
      mean_opp_b2b_OREB = divide( total_opp_b2b_OREB.to_f, num_opp_b2b_games )
      #mean_opp_b2b_OREB_PCT = divide( total_opp_b2b_OREB_PCT.to_f, num_opp_b2b_games )
      #mean_opp_front_b2b_OREB = divide( total_opp_front_b2b_OREB.to_f, num_opp_front_b2b_games )
      #mean_opp_front_b2b_OREB_PCT = divide( total_opp_front_b2b_OREB_PCT.to_f, num_opp_front_b2b_games )
      #mean_opp_non_front_b2b_OREB = divide( total_opp_non_front_b2b_OREB.to_f, num_opp_non_front_b2b_games )
      #mean_opp_non_front_b2b_OREB_PCT = divide( total_opp_non_front_b2b_OREB_PCT.to_f, num_opp_non_front_b2b_games )
      #mean_opp_non_b2b_OREB = divide( total_opp_non_b2b_OREB.to_f, num_opp_non_b2b_games )
      #mean_opp_non_b2b_OREB_PCT = divide( total_opp_non_b2b_OREB_PCT.to_f, num_opp_non_b2b_games )
      #mean_opp_threeg4d_OREB = divide( total_opp_threeg4d_OREB.to_f, num_opp_threeg4d_games )
      #mean_opp_threeg4d_OREB_PCT = divide( total_opp_threeg4d_OREB_PCT.to_f, num_opp_threeg4d_games )
      mean_opp_extra_rest_OREB = divide( total_opp_extra_rest_OREB.to_f, num_opp_extra_rest_games )
      #mean_opp_extra_rest_OREB_PCT = divide( total_opp_extra_rest_OREB_PCT.to_f, num_opp_extra_rest_games )

      #mean_b2b_DREB = divide( total_b2b_DREB.to_f, num_b2b_games )
      #mean_b2b_DREB_PCT = divide( total_b2b_DREB_PCT.to_f, num_b2b_games )
      mean_front_b2b_DREB = divide( total_front_b2b_DREB.to_f, num_front_b2b_games )
      #mean_front_b2b_DREB_PCT = divide( total_front_b2b_DREB_PCT.to_f, num_front_b2b_games )
      #mean_non_front_b2b_DREB = divide( total_non_front_b2b_DREB.to_f, num_non_front_b2b_games )
      #mean_non_front_b2b_DREB_PCT = divide( total_non_front_b2b_DREB_PCT.to_f, num_non_front_b2b_games )
      #mean_non_b2b_DREB = divide( total_non_b2b_DREB.to_f, num_non_b2b_games )
      #mean_non_b2b_DREB_PCT = divide( total_non_b2b_DREB_PCT.to_f, num_non_b2b_games )
      #mean_threeg4d_DREB = divide( total_threeg4d_DREB.to_f, num_threeg4d_games )
      #mean_threeg4d_DREB_PCT = divide( total_threeg4d_DREB_PCT.to_f, num_threeg4d_games )
      mean_extra_rest_DREB = divide( total_extra_rest_DREB.to_f, num_extra_rest_games )
      #mean_extra_rest_DREB_PCT = divide( total_extra_rest_DREB_PCT.to_f, num_extra_rest_games )
      mean_opp_b2b_DREB = divide( total_opp_b2b_DREB.to_f, num_opp_b2b_games )
      #mean_opp_b2b_DREB_PCT = divide( total_opp_b2b_DREB_PCT.to_f, num_opp_b2b_games )
      #mean_opp_front_b2b_DREB = divide( total_opp_front_b2b_DREB.to_f, num_opp_front_b2b_games )
      #mean_opp_front_b2b_DREB_PCT = divide( total_opp_front_b2b_DREB_PCT.to_f, num_opp_front_b2b_games )
      #mean_opp_non_front_b2b_DREB = divide( total_opp_non_front_b2b_DREB.to_f, num_opp_non_front_b2b_games )
      #mean_opp_non_front_b2b_DREB_PCT = divide( total_opp_non_front_b2b_DREB_PCT.to_f, num_opp_non_front_b2b_games )
      #mean_opp_non_b2b_DREB = divide( total_opp_non_b2b_DREB.to_f, num_opp_non_b2b_games )
      #mean_opp_non_b2b_DREB_PCT = divide( total_opp_non_b2b_DREB_PCT.to_f, num_opp_non_b2b_games )
      #mean_opp_threeg4d_DREB = divide( total_opp_threeg4d_DREB.to_f, num_opp_threeg4d_games )
      #mean_opp_threeg4d_DREB_PCT = divide( total_opp_threeg4d_DREB_PCT.to_f, num_opp_threeg4d_games )
      mean_opp_extra_rest_DREB = divide( total_opp_extra_rest_DREB.to_f, num_opp_extra_rest_games )
      mean_opp_extra_rest_DREB_PCT = divide( total_opp_extra_rest_DREB_PCT.to_f, num_opp_extra_rest_games )

      #mean_b2b_STL = divide( total_b2b_STL.to_f, num_b2b_games )
      #mean_non_b2b_STL = divide( total_non_b2b_STL.to_f, num_non_b2b_games )
      #mean_front_b2b_STL = divide( total_front_b2b_STL.to_f, num_front_b2b_games )
      #mean_non_front_b2b_STL = divide( total_non_front_b2b_STL.to_f, num_non_front_b2b_games )
      #mean_threeg4d_STL = divide( total_threeg4d_STL.to_f, num_threeg4d_games )
      mean_extra_rest_STL = divide( total_extra_rest_STL.to_f, num_extra_rest_games )
      mean_opp_b2b_STL = divide( total_opp_b2b_STL.to_f, num_opp_b2b_games )
      #mean_opp_non_b2b_STL = divide( total_opp_non_b2b_STL.to_f, num_opp_non_b2b_games )
      #mean_opp_front_b2b_STL = divide( total_opp_front_b2b_STL.to_f, num_opp_front_b2b_games )
      #mean_opp_non_front_b2b_STL = divide( total_opp_non_front_b2b_STL.to_f, num_opp_non_front_b2b_games )
      #mean_opp_threeg4d_STL = divide( total_opp_threeg4d_STL.to_f, num_opp_threeg4d_games )
      mean_opp_extra_rest_STL = divide( total_opp_extra_rest_STL.to_f, num_opp_extra_rest_games )

      #mean_b2b_AST = divide( total_b2b_AST.to_f, num_b2b_games )
      #mean_non_b2b_AST = divide( total_non_b2b_AST.to_f, num_non_b2b_games )
      mean_front_b2b_AST = divide( total_front_b2b_AST.to_f, num_front_b2b_games )
      #mean_non_front_b2b_AST = divide( total_non_front_b2b_AST.to_f, num_non_front_b2b_games )
      #mean_threeg4d_AST = divide( total_threeg4d_AST.to_f, num_threeg4d_games )
      mean_extra_rest_AST = divide( total_extra_rest_AST.to_f, num_extra_rest_games )
      mean_opp_b2b_AST = divide( total_opp_b2b_AST.to_f, num_opp_b2b_games )
      #mean_opp_non_b2b_AST = divide( total_opp_non_b2b_AST.to_f, num_opp_non_b2b_games )
      #mean_opp_front_b2b_AST = divide( total_opp_front_b2b_AST.to_f, num_opp_front_b2b_games )
      #mean_opp_non_front_b2b_AST = divide( total_opp_non_front_b2b_AST.to_f, num_opp_non_front_b2b_games )
      #mean_opp_threeg4d_AST = divide( total_opp_threeg4d_AST.to_f, num_opp_threeg4d_games )
      mean_opp_extra_rest_AST = divide( total_opp_extra_rest_AST.to_f, num_opp_extra_rest_games )
      
      mean_b2b_BLK = divide( total_b2b_BLK.to_f, num_b2b_games )
      mean_non_b2b_BLK = divide( total_non_b2b_BLK.to_f, num_non_b2b_games )
      mean_front_b2b_BLK = divide( total_front_b2b_BLK.to_f, num_front_b2b_games )
      mean_non_front_b2b_BLK = divide( total_non_front_b2b_BLK.to_f, num_non_front_b2b_games )
      mean_threeg4d_BLK = divide( total_threeg4d_BLK.to_f, num_threeg4d_games )
      mean_extra_rest_BLK = divide( total_extra_rest_BLK.to_f, num_extra_rest_games )
      mean_opp_b2b_BLK = divide( total_opp_b2b_BLK.to_f, num_opp_b2b_games )
      #mean_opp_non_b2b_BLK = divide( total_opp_non_b2b_BLK.to_f, num_opp_non_b2b_games )
      #mean_opp_front_b2b_BLK = divide( total_opp_front_b2b_BLK.to_f, num_opp_front_b2b_games )
      #mean_opp_non_front_b2b_BLK = divide( total_opp_non_front_b2b_BLK.to_f, num_opp_non_front_b2b_games )
      #mean_opp_threeg4d_BLK = divide( total_opp_threeg4d_BLK.to_f, num_opp_threeg4d_games )
      mean_opp_extra_rest_BLK = divide( total_opp_extra_rest_BLK.to_f, num_opp_extra_rest_games )

      #mean_b2b_TOV = divide( total_b2b_TOV.to_f, num_b2b_games )
      #mean_non_b2b_TOV = divide( total_non_b2b_TOV.to_f, num_non_b2b_games )
      mean_front_b2b_TOV = divide( total_front_b2b_TOV.to_f, num_front_b2b_games )
      #mean_non_front_b2b_TOV = divide( total_non_front_b2b_TOV.to_f, num_non_front_b2b_games )
      #mean_threeg4d_TOV = divide( total_threeg4d_TOV.to_f, num_threeg4d_games )
      mean_extra_rest_TOV = divide( total_extra_rest_TOV.to_f, num_extra_rest_games )
      mean_opp_b2b_TOV = divide( total_opp_b2b_TOV.to_f, num_opp_b2b_games )
      #mean_opp_non_b2b_TOV = divide( total_opp_non_b2b_TOV.to_f, num_opp_non_b2b_games )
      #mean_opp_front_b2b_TOV = divide( total_opp_front_b2b_TOV.to_f, num_opp_front_b2b_games )
      #mean_opp_non_front_b2b_TOV = divide( total_opp_non_front_b2b_TOV.to_f, num_opp_non_front_b2b_games )
      #mean_opp_threeg4d_TOV = divide( total_opp_threeg4d_TOV.to_f, num_opp_threeg4d_games )
      mean_opp_extra_rest_TOV = divide( total_opp_extra_rest_TOV.to_f, num_opp_extra_rest_games )

      if i > 0
        prev_seconds_played = Duration.new( :minutes => player_boxscores[i-1][:MIN].split(":")[0], :seconds => player_boxscores[i-1][:MIN].split(":")[1] ).total
      end
      if i > 1
        prev_prev_seconds_played = Duration.new( :minutes => player_boxscores[i-2][:MIN].split(":")[0], :seconds => player_boxscores[i-2][:MIN].split(":")[1] ).total
      end

      mean_b2b_seconds = divide( total_b2b_SECONDS.to_f, num_b2b_games )
      #mean_non_b2b_seconds = divide( total_non_b2b_SECONDS.to_f, num_non_b2b_games )
      mean_front_b2b_seconds = divide( total_front_b2b_SECONDS.to_f, num_front_b2b_games )
      #mean_non_front_b2b_seconds = divide( total_non_front_b2b_SECONDS.to_f, num_non_front_b2b_games )
      #mean_threeg4d_seconds = divide( total_threeg4d_SECONDS.to_f, num_threeg4d_games )
      mean_extra_rest_seconds = divide( total_extra_rest_SECONDS.to_f, num_extra_rest_games )
      mean_opp_b2b_seconds = divide( total_opp_b2b_SECONDS.to_f, num_opp_b2b_games )
      mean_opp_non_b2b_seconds = divide( total_opp_non_b2b_SECONDS.to_f, num_opp_non_b2b_games )
      mean_opp_front_b2b_seconds = divide( total_opp_front_b2b_SECONDS.to_f, num_opp_front_b2b_games )
      #mean_opp_non_front_b2b_seconds = divide( total_opp_non_front_b2b_SECONDS.to_f, num_opp_non_front_b2b_games )
      #mean_opp_threeg4d_seconds = divide( total_opp_threeg4d_SECONDS.to_f, num_opp_threeg4d_games )
      mean_opp_extra_rest_seconds = divide( total_opp_extra_rest_SECONDS.to_f, num_opp_extra_rest_games )

      entries = database[:"#{tablename}"].select_all.where(:date => average_date, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "player vs #{opponent_abbr}").entries
      v_opp = entries.first
      if v_opp
        in_season_mean_PTS_v_team = v_opp[:PTS_mean]
        in_season_mean_OREB_v_team = v_opp[:OREB_mean]
        in_season_mean_DREB_v_team = v_opp[:DREB_mean]
        in_season_mean_STL_v_team = v_opp[:STL_mean]
        in_season_mean_AST_v_team = v_opp[:AST_mean]
        in_season_mean_BLK_v_team = v_opp[:BLK_mean]
        in_season_mean_TOV_v_team = v_opp[:TOV_mean]
        in_season_mean_SECONDS_v_team = v_opp[:seconds_played_mean]
      else
        p "#{player[:PLAYER_NAME]} has no stats against #{opponent_abbr} earlier in #{season}"
      end
      
      if DB1
        prev_season = getPreviousSeason( season )
        tablename = "_" + prev_season + " " + type + " daily averages"
        tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

        type_index = nil
        if "regularseason" == type
          type_index = 0
        else
          type_index = 1
        end
        entries = DB1[:"#{tablename}"].select_all.where(:date => seasons_h[ prev_season ][ type_index + 1 ], :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "player vs #{opponent_abbr}").entries
        if entries.size > 0
          v_opp_prev = entries.first
          prev_year_mean_PTS_v_team = v_opp_prev[:PTS_mean]
          prev_year_mean_OREB_v_team = v_opp_prev[:OREB_mean]
          prev_year_mean_DREB_v_team = v_opp_prev[:DREB_mean]
          prev_year_mean_STL_v_team = v_opp_prev[:STL_mean]
          prev_year_mean_AST_v_team = v_opp_prev[:AST_mean]
          prev_year_mean_BLK_v_team = v_opp_prev[:BLK_mean]
          prev_year_mean_TOV_v_team = v_opp_prev[:TOV_mean]
          prev_year_mean_SECONDS_v_team = v_opp_prev[:seconds_played_mean]
        else
          p "#{player[:PLAYER_NAME]} has no stats against #{opponent_abbr} in #{prev_season}"
        end

        if DB2
          prev_2_season = getPreviousSeason( prev_season )
          tablename = "_" + prev_2_season + " " + type + " daily averages"
          tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

          type_index = nil
          if "regularseason" == type
            type_index = 0
          else
            type_index = 1
          end
          entries = DB1[:"#{tablename}"].select_all.where(:date => seasons_h[ prev_2_season ][ type_index + 1 ], :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "player vs #{opponent_abbr}").entries
          if entries.size > 0
            v_opp_prev2 = entries.first
            if v_opp_prev2
              prev_year_2_mean_PTS_v_team = v_opp_prev2[:PTS_mean]
              prev_year_2_mean_OREB_v_team = v_opp_prev2[:OREB_mean]
              prev_year_2_mean_DREB_v_team = v_opp_prev2[:DREB_mean]
              prev_year_2_mean_STL_v_team = v_opp_prev2[:STL_mean]
              prev_year_2_mean_AST_v_team = v_opp_prev2[:AST_mean]
              prev_year_2_mean_BLK_v_team = v_opp_prev2[:BLK_mean]
              prev_year_2_mean_TOV_v_team = v_opp_prev2[:TOV_mean]
              prev_year_2_mean_SECONDS_v_team = v_opp_prev2[:seconds_played_mean]
            end
          else
            p "#{player[:PLAYER_NAME]} has no stats against #{opponent_abbr} in #{prev_2_season}"
          end
          if DB3
            prev_3_season = getPreviousSeason( prev_2_season )
            tablename = "_" + prev_3_season + " " + type + " daily averages"
            tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

            type_index = nil
            if "regularseason" == type
              type_index = 0
            else
              type_index = 1
            end
            entries = DB1[:"#{tablename}"].select_all.where(:date => seasons_h[ prev_3_season ][ type_index + 1 ], :PLAYER_NAME => player[:PLAYER_NAME], :average_type => "player vs #{opponent_abbr}").entries
            if entries.size > 0
              v_opp_prev3 = entries.first
              if v_opp_prev3
                prev_year_3_mean_PTS_v_team = v_opp_prev3[:PTS_mean]
                prev_year_3_mean_OREB_v_team = v_opp_prev3[:OREB_mean]
                prev_year_3_mean_DREB_v_team = v_opp_prev3[:DREB_mean]
                prev_year_3_mean_STL_v_team = v_opp_prev3[:STL_mean]
                prev_year_3_mean_AST_v_team = v_opp_prev3[:AST_mean]
                prev_year_3_mean_BLK_v_team = v_opp_prev3[:BLK_mean]
                prev_year_3_mean_TOV_v_team = v_opp_prev3[:TOV_mean]
                prev_year_3_mean_SECONDS_v_team = v_opp_prev3[:seconds_played_mean]
              end
            else
              p "#{player[:PLAYER_NAME]} has no stats against #{opponent_abbr} in #{prev_3_season}"
            end
          end
        end
      end

      #Now we've stored all the predictive stats (Xs).  We can calculate the next (Xs) for the next game by summing with 
      #the actual values now
################################################

      num_reg_games = num_reg_games + 1

      mean_pts_per_min = divide(60*previous_average[:PTS_mean].to_f, average_seconds)
      mean_oreb_per_min = divide(60*previous_average[:OREB_mean].to_f, average_seconds)
      mean_dreb_per_min = divide(60*previous_average[:DREB_mean].to_f, average_seconds)
      mean_ast_per_min = divide(60*previous_average[:AST_mean].to_f, average_seconds)
      mean_tov_per_min = divide(60*previous_average[:TOV_mean].to_f, average_seconds)
      mean_blk_per_min = divide(60*previous_average[:BLK_mean].to_f, average_seconds)
      mean_stl_per_min = divide(60*previous_average[:STL_mean].to_f, average_seconds)

      if 1 == b2b
        total_b2b_PTS = total_b2b_PTS + boxscore[:PTS].to_i
        total_b2b_OREB = total_b2b_OREB + boxscore[:OREB].to_i
        total_b2b_OREB_PCT = total_b2b_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_b2b_DREB = total_b2b_DREB + boxscore[:DREB].to_i
        total_b2b_DREB_PCT = total_b2b_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_b2b_STL = total_b2b_STL + boxscore[:STL].to_i
        total_b2b_AST = total_b2b_AST + boxscore[:AST].to_i
        total_b2b_BLK = total_b2b_BLK + boxscore[:BLK].to_i
        total_b2b_TOV = total_b2b_TOV + boxscore[:TO].to_i
        total_b2b_SECONDS = total_b2b_SECONDS + time_played

        num_b2b_games = num_b2b_games + 1
      elsif 1 == threeg4d
        total_threeg4d_PTS = total_threeg4d_PTS + boxscore[:PTS].to_i
        total_threeg4d_OREB = total_threeg4d_OREB + boxscore[:OREB].to_i
        total_threeg4d_OREB_PCT = total_threeg4d_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_threeg4d_DREB = total_threeg4d_DREB + boxscore[:DREB].to_i
        total_threeg4d_DREB_PCT = total_threeg4d_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_threeg4d_STL = total_threeg4d_STL + boxscore[:STL].to_i
        total_threeg4d_AST = total_threeg4d_AST + boxscore[:AST].to_i
        total_threeg4d_BLK = total_threeg4d_BLK + boxscore[:BLK].to_i
        total_threeg4d_TOV = total_threeg4d_TOV + boxscore[:TO].to_i
        total_threeg4d_SECONDS = total_threeg4d_SECONDS + time_played

        num_threeg4d_games = num_threeg4d_games + 1
      elsif 1 == extra_rest
        total_extra_rest_PTS = total_extra_rest_PTS + boxscore[:PTS].to_i
        total_extra_rest_OREB = total_extra_rest_OREB + boxscore[:OREB].to_i
        total_extra_rest_OREB_PCT = total_extra_rest_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_extra_rest_DREB = total_extra_rest_DREB + boxscore[:DREB].to_i
        total_extra_rest_DREB_PCT = total_extra_rest_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_extra_rest_STL = total_extra_rest_STL + boxscore[:STL].to_i
        total_extra_rest_AST = total_extra_rest_AST + boxscore[:AST].to_i
        total_extra_rest_BLK = total_extra_rest_BLK + boxscore[:BLK].to_i
        total_extra_rest_TOV = total_extra_rest_TOV + boxscore[:TO].to_i
        total_extra_rest_SECONDS = total_extra_rest_SECONDS + time_played

        num_extra_rest_games = num_extra_rest_games + 1
      else
        total_non_b2b_PTS = total_non_b2b_PTS + boxscore[:PTS].to_i
        total_non_b2b_OREB = total_non_b2b_OREB + boxscore[:OREB].to_i
        total_non_b2b_OREB_PCT = total_non_b2b_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_non_b2b_DREB = total_non_b2b_DREB + boxscore[:DREB].to_i
        total_non_b2b_DREB_PCT = total_non_b2b_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_non_b2b_STL = total_non_b2b_STL + boxscore[:STL].to_i
        total_non_b2b_AST = total_non_b2b_AST + boxscore[:AST].to_i
        total_non_b2b_BLK = total_non_b2b_BLK + boxscore[:BLK].to_i
        total_non_b2b_TOV = total_non_b2b_TOV + boxscore[:TO].to_i
        total_non_b2b_SECONDS = total_non_b2b_SECONDS + time_played

        num_non_b2b_games = num_non_b2b_games + 1
      end

      if 1 == front_b2b
        total_front_b2b_PTS = total_front_b2b_PTS + boxscore[:PTS].to_i
        total_front_b2b_OREB = total_front_b2b_OREB + boxscore[:OREB].to_i
        total_front_b2b_OREB_PCT = total_front_b2b_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_front_b2b_DREB = total_front_b2b_DREB + boxscore[:DREB].to_i
        total_front_b2b_DREB_PCT = total_front_b2b_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_front_b2b_STL = total_front_b2b_STL + boxscore[:STL].to_i
        total_front_b2b_AST = total_front_b2b_AST + boxscore[:AST].to_i
        total_front_b2b_BLK = total_front_b2b_BLK + boxscore[:BLK].to_i
        total_front_b2b_TOV = total_front_b2b_TOV + boxscore[:TO].to_i
        total_front_b2b_SECONDS = total_front_b2b_SECONDS + time_played

        num_front_b2b_games = num_front_b2b_games + 1
      else
        total_non_front_b2b_PTS = total_non_front_b2b_PTS + boxscore[:PTS].to_i
        total_non_front_b2b_OREB = total_non_front_b2b_OREB + boxscore[:OREB].to_i
        total_non_front_b2b_OREB_PCT = total_non_front_b2b_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_non_front_b2b_DREB = total_non_front_b2b_DREB + boxscore[:DREB].to_i
        total_non_front_b2b_DREB_PCT = total_non_front_b2b_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_non_front_b2b_STL = total_non_front_b2b_STL + boxscore[:STL].to_i
        total_non_front_b2b_AST = total_non_front_b2b_AST + boxscore[:AST].to_i
        total_non_front_b2b_BLK = total_non_front_b2b_BLK + boxscore[:BLK].to_i
        total_non_front_b2b_TOV = total_non_front_b2b_TOV + boxscore[:TO].to_i
        total_non_front_b2b_SECONDS = total_non_front_b2b_SECONDS + time_played

        num_non_front_b2b_games = num_non_front_b2b_games + 1
      end

      if 1 == opp_b2b
        total_opp_b2b_PTS = total_opp_b2b_PTS + boxscore[:PTS].to_i
        total_opp_b2b_OREB = total_opp_b2b_OREB + boxscore[:OREB].to_i
        total_opp_b2b_OREB_PCT = total_opp_b2b_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_opp_b2b_DREB = total_opp_b2b_DREB + boxscore[:DREB].to_i
        total_opp_b2b_DREB_PCT = total_opp_b2b_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_opp_b2b_STL = total_opp_b2b_STL + boxscore[:STL].to_i
        total_opp_b2b_AST = total_opp_b2b_AST + boxscore[:AST].to_i
        total_opp_b2b_BLK = total_opp_b2b_BLK + boxscore[:BLK].to_i
        total_opp_b2b_TOV = total_opp_b2b_TOV + boxscore[:TO].to_i
        total_opp_b2b_SECONDS = total_opp_b2b_SECONDS + time_played

        num_opp_b2b_games = num_opp_b2b_games + 1
      elsif 1 == opp_threeg4d
        total_opp_threeg4d_PTS = total_opp_threeg4d_PTS + boxscore[:PTS].to_i
        total_opp_threeg4d_OREB = total_opp_threeg4d_OREB + boxscore[:OREB].to_i
        total_opp_threeg4d_OREB_PCT = total_opp_threeg4d_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_opp_threeg4d_DREB = total_opp_threeg4d_DREB + boxscore[:DREB].to_i
        total_opp_threeg4d_DREB_PCT = total_opp_threeg4d_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_opp_threeg4d_STL = total_opp_threeg4d_STL + boxscore[:STL].to_i
        total_opp_threeg4d_AST = total_opp_threeg4d_AST + boxscore[:AST].to_i
        total_opp_threeg4d_BLK = total_opp_threeg4d_BLK + boxscore[:BLK].to_i
        total_opp_threeg4d_TOV = total_opp_threeg4d_TOV + boxscore[:TO].to_i
        total_opp_threeg4d_SECONDS = total_opp_threeg4d_SECONDS + time_played

        num_opp_threeg4d_games = num_opp_threeg4d_games + 1
      elsif 1 == opp_extra_rest
        total_opp_extra_rest_PTS = total_opp_extra_rest_PTS + boxscore[:PTS].to_i
        total_opp_extra_rest_OREB = total_opp_extra_rest_OREB + boxscore[:OREB].to_i
        total_opp_extra_rest_OREB_PCT = total_opp_extra_rest_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_opp_extra_rest_DREB = total_opp_extra_rest_DREB + boxscore[:DREB].to_i
        total_opp_extra_rest_DREB_PCT = total_opp_extra_rest_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_opp_extra_rest_STL = total_opp_extra_rest_STL + boxscore[:STL].to_i
        total_opp_extra_rest_AST = total_opp_extra_rest_AST + boxscore[:AST].to_i
        total_opp_extra_rest_BLK = total_opp_extra_rest_BLK + boxscore[:BLK].to_i
        total_opp_extra_rest_TOV = total_opp_extra_rest_TOV + boxscore[:TO].to_i
        total_opp_extra_rest_SECONDS = total_opp_extra_rest_SECONDS + time_played

        num_opp_extra_rest_games = num_opp_extra_rest_games + 1
      else
        total_opp_non_b2b_PTS = total_opp_non_b2b_PTS + boxscore[:PTS].to_i
        total_opp_non_b2b_OREB = total_opp_non_b2b_OREB + boxscore[:OREB].to_i
        total_opp_non_b2b_OREB_PCT = total_opp_non_b2b_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_opp_non_b2b_DREB = total_opp_non_b2b_DREB + boxscore[:DREB].to_i
        total_opp_non_b2b_DREB_PCT = total_opp_non_b2b_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_opp_non_b2b_STL = total_opp_non_b2b_STL + boxscore[:STL].to_i
        total_opp_non_b2b_AST = total_opp_non_b2b_AST + boxscore[:AST].to_i
        total_opp_non_b2b_BLK = total_opp_non_b2b_BLK + boxscore[:BLK].to_i
        total_opp_non_b2b_TOV = total_opp_non_b2b_TOV + boxscore[:TO].to_i
        total_opp_non_b2b_SECONDS = total_opp_non_b2b_SECONDS + time_played

        num_opp_non_b2b_games = num_opp_non_b2b_games + 1
      end

      if 1 == opp_front_b2b
        total_opp_front_b2b_PTS = total_opp_front_b2b_PTS + boxscore[:PTS].to_i
        total_opp_front_b2b_OREB = total_opp_front_b2b_OREB + boxscore[:OREB].to_i
        total_opp_front_b2b_OREB_PCT = total_opp_front_b2b_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_opp_front_b2b_DREB = total_opp_front_b2b_DREB + boxscore[:DREB].to_i
        total_opp_front_b2b_DREB_PCT = total_opp_front_b2b_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_opp_front_b2b_STL = total_opp_front_b2b_STL + boxscore[:STL].to_i
        total_opp_front_b2b_AST = total_opp_front_b2b_AST + boxscore[:AST].to_i
        total_opp_front_b2b_BLK = total_opp_front_b2b_BLK + boxscore[:BLK].to_i
        total_opp_front_b2b_TOV = total_opp_front_b2b_TOV + boxscore[:TO].to_i
        total_opp_front_b2b_SECONDS = total_opp_front_b2b_SECONDS + time_played

        num_opp_front_b2b_games = num_opp_front_b2b_games + 1
      else
        total_opp_non_front_b2b_PTS = total_opp_non_front_b2b_PTS + boxscore[:PTS].to_i
        total_opp_non_front_b2b_OREB = total_opp_non_front_b2b_OREB + boxscore[:OREB].to_i
        total_opp_non_front_b2b_OREB_PCT = total_opp_non_front_b2b_OREB_PCT + boxscore[:OREB_PCT].to_f
        total_opp_non_front_b2b_DREB = total_opp_non_front_b2b_DREB + boxscore[:DREB].to_i
        total_opp_non_front_b2b_DREB_PCT = total_opp_non_front_b2b_DREB_PCT + boxscore[:DREB_PCT].to_f
        total_opp_non_front_b2b_STL = total_opp_non_front_b2b_STL + boxscore[:STL].to_i
        total_opp_non_front_b2b_AST = total_opp_non_front_b2b_AST + boxscore[:AST].to_i
        total_opp_non_front_b2b_BLK = total_opp_non_front_b2b_BLK + boxscore[:BLK].to_i
        total_opp_non_front_b2b_TOV = total_opp_non_front_b2b_TOV + boxscore[:TO].to_i
        total_opp_non_front_b2b_SECONDS = total_opp_non_front_b2b_SECONDS + time_played

        num_opp_non_front_b2b_games = num_opp_non_front_b2b_games + 1
      end
      
      def_rtg_delta = 0
      if opp_average
        def_rtg_delta = opp_average[:DEF_RATING].to_f - league_average_v_position[:team_mean_def_rtg].to_f
      end
      def_rtg_v_position_delta = 0
      if opp_average_v_position
        def_rtg_v_position_delta = opp_average_v_position[:DEF_RATING].to_f - league_average_v_position[:team_mean_def_rtg].to_f
        o_pts_delta = opp_average_v_position[:PTS_mean].to_f - league_average_v_position[:team_mean_PTS].to_f
        o_oreb_delta = opp_average_v_position[:OREB_mean].to_f - previous_average[:OREB_mean].to_f
        o_oreb_pct_delta = opp_average_v_position[:OREB_PCT].to_f - previous_average[:OREB_PCT].to_f
        o_dreb_delta = opp_average_v_position[:DREB_mean].to_f.to_f - league_average[:team_mean_DREB].to_f
        o_dreb_pct_delta = opp_average_v_position[:DREB_PCT].to_f.to_f - league_average[:team_mean_DREB].to_f
        o_ast_delta = opp_average_v_position[:AST_mean].to_f - league_average[:team_mean_AST].to_f
        o_ast_pct_delta = opp_average_v_position[:AST_PCT].to_f - league_average[:team_mean_AST_PCT].to_f
        o_tov_delta = opp_average_v_position[:TOV_mean].to_f - league_average[:team_mean_TOV].to_f
        o_tov_pct_delta = opp_average_v_position[:TO_PCT].to_f.to_f - league_average[:team_mean_TOV_PCT].to_f
        o_blk_delta = opp_average_v_position[:BLK_mean].to_f - league_average[:team_mean_BLK].to_f
        o_blk_pct_delta = opp_average_v_position[:PCT_BLK].to_f.to_f - league_average[:team_mean_PCT_BLK].to_f
        o_stl_delta = opp_average_v_position[:STL_mean].to_f - league_average[:team_mean_STL].to_f
        o_stl_pct_delta = opp_average_v_position[:PCT_STL].to_f - league_average[:team_mean_PCT_STL].to_f
      end

      off_rtg_delta = 0
      if opp_average
        off_rtg_delta = opp_average[:OFF_RATING].to_f - league_average_v_position[:team_mean_off_rtg].to_f
      end

      off_rtg_v_position_delta = 0
      if opp_average_v_position
        off_rtg_v_position_delta = opp_average_v_position[:OFF_RATING].to_f - league_average_v_position[:team_mean_off_rtg].to_f
      end

      if opp_average
        oa_ft_misses = opp_average[:FTA_mean].to_f - opp_average[:FTM_mean].to_f
        oa_3p_misses = opp_average[:FG3A_mean].to_f - opp_average[:FG3M_mean].to_f
        oa_2p_misses = opp_average[:FG2A_mean].to_f - opp_average[:FG2M_mean].to_f
        oa_misses = opp_average[:FGA_mean].to_f - opp_average[:FGM_mean].to_f

        oa_ft_misses_per_min = oa_ft_misses / average_seconds
        oa_3p_misses_per_min = oa_3p_misses / average_seconds
        oa_2p_misses_per_min = oa_2p_misses / average_seconds
        oa_misses_per_min = oa_misses / average_seconds
      end

      if team_average
        team_ft_misses = opp_average[:FTA_mean].to_f - opp_average[:FTM_mean].to_f
        team_3p_misses = opp_average[:FG3A_mean].to_f - opp_average[:FG3M_mean].to_f
        team_2p_misses = opp_average[:FG2A_mean].to_f - opp_average[:FG2M_mean].to_f
        team_misses = opp_average[:FGA_mean].to_f - opp_average[:FGM_mean].to_f

        team_ft_misses_per_min = team_ft_misses / average_seconds
        team_3p_misses_per_min = team_3p_misses / average_seconds
        team_2p_misses_per_min = team_2p_misses / average_seconds
        team_misses_per_min = team_misses / average_seconds
      end

      location = (location == "home") ? 1 : 0

      rest_pts = previous_average[:PTS_mean].to_f
      rest_oreb = previous_average[:OREB_mean].to_f
      rest_dreb = previous_average[:DREB_mean].to_f
      rest_ast = previous_average[:AST_mean].to_f
      rest_tov = previous_average[:TOV_mean].to_f
      rest_blk = previous_average[:BLK_mean].to_f
      rest_stl = previous_average[:STL_mean].to_f

      rest_pts_per_min = divide(60*previous_average[:PTS_mean].to_f, average_seconds)
      rest_oreb_per_min = divide(60*previous_average[:OREB_mean].to_f, average_seconds)
      rest_dreb_per_min = divide(60*previous_average[:DREB_mean].to_f, average_seconds)
      rest_ast_per_min = divide(60*previous_average[:AST_mean].to_f, average_seconds)
      rest_tov_per_min = divide(60*previous_average[:TOV_mean].to_f, average_seconds)
      rest_blk_per_min = divide(60*previous_average[:BLK_mean].to_f, average_seconds)
      rest_stl_per_min = divide(60*previous_average[:STL_mean].to_f, average_seconds)
      rest_seconds = average_seconds

      if 1 == b2b
        rest_pts = divide( total_b2b_PTS.to_f, num_b2b_games )
        rest_oreb = divide( total_b2b_DREB.to_f, num_b2b_games )
        rest_dreb = divide( total_b2b_DREB.to_f, num_b2b_games )
        rest_ast = divide( total_b2b_AST.to_f, num_b2b_games )
        rest_tov = divide( total_b2b_TOV.to_f, num_b2b_games )
        rest_blk = divide( total_b2b_BLK.to_f, num_b2b_games )
        rest_stl = divide( total_b2b_STL.to_f, num_b2b_games )
        rest_pts_per_min = divide(60*mean_b2b_PTS.to_f, mean_b2b_seconds.to_f)
        rest_oreb_per_min = divide(60*mean_b2b_OREB.to_f, mean_b2b_seconds.to_f)
        rest_dreb_per_min = divide(60*mean_b2b_DREB.to_f, mean_b2b_seconds.to_f)
        rest_ast_per_min = divide(60*mean_b2b_AST.to_f, mean_b2b_seconds.to_f)
        rest_tov_per_min = divide(60*mean_b2b_TOV.to_f, mean_b2b_seconds.to_f)
        rest_blk_per_min = divide(60*mean_b2b_BLK.to_f, mean_b2b_seconds.to_f)
        rest_stl_per_min = divide(60*mean_b2b_STL.to_f, mean_b2b_seconds.to_f)
        rest_seconds = mean_b2b_seconds.to_f
=begin
      elsif 1 == front_b2b
        if 1 == extra_rest
          rest_pts = (mean_front_b2b_PTS.to_f + mean_extra_rest_PTS.to_f) / 2
          rest_oreb = mean_front_b2b_OREB.to_f
          rest_dreb = mean_front_b2b_DREB.to_f
          rest_ast = mean_front_b2b_AST.to_f
          rest_tov = mean_front_b2b_TOV.to_f
          rest_blk = mean_front_b2b_BLK.to_f
          rest_pts_per_min = divide(60*(mean_front_b2b_PTS.to_f, mean_front_b2b_seconds.to_f + mean_extra_rest_PTS.to_f / mean_extra_rest_seconds.to_f) / 2)
        else
          rest_pts = mean_front_b2b_PTS.to_f
          rest_pts_per_min = divide(60*mean_front_b2b_PTS.to_f, mean_front_b2b_seconds.to_f)
        end
=end
      elsif 1 == extra_rest
        rest_pts = mean_extra_rest_PTS.to_f
        rest_oreb = mean_extra_rest_OREB.to_f
        rest_dreb = mean_extra_rest_DREB.to_f
        rest_ast = mean_extra_rest_AST.to_f
        rest_tov = mean_extra_rest_TOV.to_f
        rest_blk = mean_extra_rest_BLK.to_f
        rest_stl = mean_extra_rest_STL.to_f
        rest_pts_per_min = divide(60*mean_extra_rest_PTS.to_f, mean_extra_rest_seconds.to_f)
        rest_oreb_per_min = divide(60*mean_extra_rest_OREB.to_f, mean_extra_rest_seconds.to_f)
        rest_dreb_per_min = divide(60*mean_extra_rest_DREB.to_f, mean_extra_rest_seconds.to_f)
        rest_ast_per_min = divide(60*mean_extra_rest_AST.to_f, mean_extra_rest_seconds.to_f)
        rest_tov_per_min = divide(60*mean_extra_rest_TOV.to_f, mean_extra_rest_seconds.to_f)
        rest_blk_per_min = divide(60*mean_extra_rest_BLK.to_f, mean_extra_rest_seconds.to_f)
        rest_stl_per_min = divide(60*mean_extra_rest_STL.to_f, mean_extra_rest_seconds.to_f)
        rest_seconds = mean_extra_rest_seconds.to_f
      else # 1 == non_b2b
        #rest_pts = mean_non_b2b_PTS.to_f
      end
      rest_effect = rest_pts - previous_average[:PTS_mean].to_f
      rest_oreb_effect = rest_oreb - previous_average[:OREB_mean].to_f
      rest_dreb_effect = rest_dreb - previous_average[:DREB_mean].to_f
      rest_ast_effect = rest_ast - previous_average[:AST_mean].to_f
      rest_tov_effect = rest_tov - previous_average[:TOV_mean].to_f
      rest_blk_effect = rest_blk - previous_average[:BLK_mean].to_f
      rest_stl_effect = rest_stl - previous_average[:STL_mean].to_f
      rest_effect_per_min = rest_pts_per_min - mean_pts_per_min

      rest_oreb_effect_per_min = rest_oreb_per_min - mean_oreb_per_min
      rest_dreb_effect_per_min = rest_dreb_per_min - mean_dreb_per_min
      rest_ast_effect_per_min = rest_ast_per_min - mean_ast_per_min
      rest_tov_effect_per_min = rest_tov_per_min - mean_tov_per_min
      rest_blk_effect_per_min = rest_blk_per_min - mean_blk_per_min
      rest_stl_effect_per_min = rest_stl_per_min - mean_stl_per_min
      rest_effect_seconds = rest_seconds - average_seconds

      opp_rest_pts = previous_average[:PTS_mean].to_f
      opp_rest_oreb = previous_average[:OREB_mean].to_f
      opp_rest_dreb = previous_average[:DREB_mean].to_f
      opp_rest_ast = previous_average[:AST_mean].to_f
      opp_rest_tov = previous_average[:TOV_mean].to_f
      opp_rest_blk = previous_average[:BLK_mean].to_f
      opp_rest_stl = previous_average[:STL_mean].to_f
      opp_rest_pts_per_min = divide(60*previous_average[:PTS_mean].to_f, average_seconds)
      opp_rest_oreb_per_min = divide(60*previous_average[:OREB_mean].to_f, average_seconds)
      opp_rest_dreb_per_min = divide(60*previous_average[:DREB_mean].to_f, average_seconds)
      opp_rest_ast_per_min = divide(60*previous_average[:AST_mean].to_f, average_seconds)
      opp_rest_tov_per_min = divide(60*previous_average[:TOV_mean].to_f, average_seconds)
      opp_rest_blk_per_min = divide(60*previous_average[:BLK_mean].to_f, average_seconds)
      opp_rest_stl_per_min = divide(60*previous_average[:STL_mean].to_f, average_seconds)
      opp_rest_seconds = average_seconds
      if 1 == opp_b2b
        opp_rest_pts = mean_opp_b2b_PTS.to_f
        opp_rest_oreb = mean_opp_b2b_OREB.to_f
        opp_rest_dreb = mean_opp_b2b_DREB.to_f
        opp_rest_ast = mean_opp_b2b_AST.to_f
        opp_rest_tov = mean_opp_b2b_TOV.to_f
        opp_rest_blk = mean_opp_b2b_BLK.to_f
        opp_rest_stl = mean_opp_b2b_STL.to_f
        opp_rest_pts_per_min = divide(60*mean_opp_b2b_PTS.to_f, mean_opp_b2b_seconds.to_f)
        opp_rest_oreb_per_min = divide(60*mean_opp_b2b_OREB.to_f, mean_opp_b2b_seconds.to_f)
        opp_rest_dreb_per_min = divide(60*mean_opp_b2b_DREB.to_f, mean_opp_b2b_seconds.to_f)
        opp_rest_ast_per_min = divide(60*mean_opp_b2b_AST.to_f, mean_opp_b2b_seconds.to_f)
        opp_rest_tov_per_min = divide(60*mean_opp_b2b_TOV.to_f, mean_opp_b2b_seconds.to_f)
        opp_rest_blk_per_min = divide(60*mean_opp_b2b_BLK.to_f, mean_opp_b2b_seconds.to_f)
        opp_rest_stl_per_min = divide(60*mean_opp_b2b_STL.to_f, mean_opp_b2b_seconds.to_f)
        opp_rest_seconds = mean_opp_b2b_seconds.to_f
=begin
      elsif 1 == opp_front_b2b
        if 1 == opp_extra_rest
          opp_rest_pts = (mean_opp_front_b2b_PTS.to_f + mean_opp_extra_rest_PTS.to_f) / 2
          opp_rest_pts_per_min = 60*(divide(mean_opp_front_b2b_PTS.to_f, mean_opp_front_b2b_seconds.to_f) + divide(mean_opp_extra_rest_PTS.to_f, mean_opp_extra_rest_seconds.to_f)) / 2
        else
          opp_rest_pts = mean_opp_front_b2b_PTS.to_f
          opp_rest_pts_per_min = divide(60*mean_opp_front_b2b_PTS.to_f, mean_opp_front_b2b_seconds.to_f)
        end
=end
      elsif 1 == opp_extra_rest
        opp_rest_pts = mean_opp_extra_rest_PTS.to_f
        opp_rest_oreb = mean_opp_extra_rest_OREB.to_f
        opp_rest_dreb = mean_opp_extra_rest_DREB.to_f
        opp_rest_ast = mean_opp_extra_rest_AST.to_f
        opp_rest_tov = mean_opp_extra_rest_TOV.to_f
        opp_rest_blk = mean_opp_extra_rest_BLK.to_f
        opp_rest_stl = mean_opp_extra_rest_STL.to_f
        opp_rest_pts_per_min = divide(60*mean_opp_extra_rest_PTS.to_f, mean_opp_extra_rest_seconds.to_f)
        opp_rest_oreb_per_min = divide(60*mean_opp_extra_rest_OREB.to_f, mean_opp_extra_rest_seconds.to_f)
        opp_rest_dreb_per_min = divide(60*mean_opp_extra_rest_DREB.to_f, mean_opp_extra_rest_seconds.to_f)
        opp_rest_ast_per_min = divide(60*mean_opp_extra_rest_AST.to_f, mean_opp_extra_rest_seconds.to_f)
        opp_rest_tov_per_min = divide(60*mean_opp_extra_rest_TOV.to_f, mean_opp_extra_rest_seconds.to_f)
        opp_rest_blk_per_min = divide(60*mean_opp_extra_rest_BLK.to_f, mean_opp_extra_rest_seconds.to_f)
        opp_rest_stl_per_min = divide(60*mean_opp_extra_rest_STL.to_f, mean_opp_extra_rest_seconds.to_f)
        opp_rest_seconds = mean_opp_extra_rest_seconds.to_f
      else
        #opp_rest_pts = mean_opp_non_b2b_PTS.to_f
      end
      opp_rest_effect = opp_rest_pts - previous_average[:PTS_mean].to_f
      opp_rest_oreb_effect = opp_rest_oreb - previous_average[:OREB_mean].to_f
      opp_rest_dreb_effect = opp_rest_dreb - previous_average[:DREB_mean].to_f
      opp_rest_ast_effect = opp_rest_ast - previous_average[:AST_mean].to_f
      opp_rest_tov_effect = opp_rest_tov - previous_average[:TOV_mean].to_f
      opp_rest_blk_effect = opp_rest_blk - previous_average[:BLK_mean].to_f
      opp_rest_stl_effect = opp_rest_stl - previous_average[:STL_mean].to_f
      opp_rest_effect_per_min = opp_rest_pts_per_min - mean_pts_per_min
      opp_rest_oreb_effect_per_min = opp_rest_oreb_per_min - mean_oreb_per_min
      opp_rest_dreb_effect_per_min = opp_rest_dreb_per_min - mean_dreb_per_min
      opp_rest_ast_effect_per_min = opp_rest_ast_per_min - mean_ast_per_min
      opp_rest_tov_effect_per_min = opp_rest_tov_per_min - mean_tov_per_min
      opp_rest_blk_effect_per_min = opp_rest_blk_per_min - mean_blk_per_min
      opp_rest_stl_effect_per_min = opp_rest_stl_per_min - mean_stl_per_min
      opp_rest_effect_seconds = opp_rest_seconds - average_seconds

      location_pts = ("home" == location) ? previous_average_home[:PTS_mean].to_f : previous_average_away[:PTS_mean].to_f
      location_oreb = ("home" == location) ? previous_average_home[:OREB_mean].to_f : previous_average_away[:OREB_mean].to_f
      location_dreb = ("home" == location) ? previous_average_home[:DREB_mean].to_f : previous_average_away[:DREB_mean].to_f
      location_ast = ("home" == location) ? previous_average_home[:AST_mean].to_f : previous_average_away[:AST_mean].to_f
      location_tov = ("home" == location) ? previous_average_home[:TOV_mean].to_f : previous_average_away[:TOV_mean].to_f
      location_blk = ("home" == location) ? previous_average_home[:BLK_mean].to_f : previous_average_away[:mean_away_BLK].to_f
      location_stl = ("home" == location) ? previous_average_home[:STL_mean].to_f : previous_average_away[:mean_away_STL].to_f
      location_seconds = ("home" == location) ? previous_average_home[:seconds_played_mean].to_f : previous_average_away[:seconds_played_mean].to_f

      location_pts_per_min = ("home" == location) ? 60*divide(previous_average_home[:PTS_mean].to_f, previous_average_away[:seconds_played_mean].to_f) : divide(60*previous_average_home[:PTS_mean].to_f, previous_average_away[:seconds_played_mean].to_f)
      location_oreb_per_min = ("home" == location) ? 60*divide(previous_average_home[:OREB_mean].to_f, previous_average_away[:seconds_played_mean].to_f) : divide(60*previous_average_home[:OREB_mean].to_f, previous_average_away[:seconds_played_mean].to_f)
      location_dreb_per_min = ("home" == location) ? 60*divide(previous_average_home[:DREB_mean].to_f, previous_average_away[:seconds_played_mean].to_f) : divide(60*previous_average_home[:DREB_mean].to_f, previous_average_away[:seconds_played_mean].to_f)
      location_ast_per_min = ("home" == location) ? 60*divide(previous_average_home[:AST_mean].to_f, previous_average_away[:seconds_played_mean].to_f) : divide(60*previous_average_home[:AST_mean].to_f, previous_average_away[:seconds_played_mean].to_f)
      location_tov_per_min = ("home" == location) ? 60*divide(previous_average_home[:TOV_mean].to_f, previous_average_away[:seconds_played_mean].to_f) : divide(60*previous_average_home[:TOV_mean].to_f, previous_average_away[:seconds_played_mean].to_f)
      location_blk_per_min = ("home" == location) ? 60*divide(previous_average_home[:BLK_mean].to_f, previous_average_away[:seconds_played_mean].to_f) : divide(60*previous_average_home[:BLK_mean].to_f, previous_average_away[:seconds_played_mean].to_f)
      location_stl_per_min = ("home" == location) ? 60*divide(previous_average_home[:STL_mean].to_f, previous_average_away[:seconds_played_mean].to_f) : divide(60*previous_average_home[:STL_mean].to_f, previous_average_away[:seconds_played_mean].to_f)

      location_pts_effect = location_pts - previous_average[:PTS_mean].to_f
      location_oreb_effect = location_oreb - previous_average[:OREB_mean].to_f
      location_dreb_effect = location_dreb - previous_average[:DREB_mean].to_f
      location_ast_effect = location_ast - previous_average[:AST_mean].to_f
      location_tov_effect = location_tov - previous_average[:TOV_mean].to_f
      location_blk_effect = location_blk - previous_average[:BLK_mean].to_f
      location_stl_effect = location_stl - previous_average[:STL_mean].to_f

      location_pts_effect_per_min = location_pts_per_min - mean_pts_per_min
      location_oreb_effect_per_min = location_oreb_per_min - mean_oreb_per_min
      location_dreb_effect_per_min = location_dreb_per_min - mean_dreb_per_min
      location_ast_effect_per_min = location_ast_per_min - mean_ast_per_min
      location_tov_effect_per_min = location_tov_per_min - mean_tov_per_min
      location_blk_effect_per_min = location_blk_per_min - mean_blk_per_min
      location_stL_effect_per_min = location_stl_per_min - mean_stl_per_min
      location_pts_effect_seconds = location_seconds - average_seconds

      if opp_average_v_position
        expected_PTS_opp_PTS = divide(previous_average[:PTS_mean].to_f * opp_average_v_position[:PTS_mean].to_f, league_average_v_position[:team_mean_PTS].to_f)
        expected_OREB_opp_OREB = divide(previous_average[:OREB_mean].to_f * opp_average_v_position[:OREB_mean].to_f, league_average_v_position[:team_mean_OREB].to_f)
        expected_DREB_opp_DREB = divide(previous_average[:DREB_mean].to_f * opp_average_v_position[:DREB_mean].to_f, league_average_v_position[:team_mean_DREB].to_f)
        expected_AST_opp_AST = divide(previous_average[:AST_mean].to_f * opp_average_v_position[:AST_mean].to_f, league_average_v_position[:team_mean_AST].to_f)
        expected_TOV_opp_TOV = divide(previous_average[:TOV_mean].to_f * opp_average_v_position[:TOV_mean].to_f, league_average_v_position[:team_mean_TOV].to_f)
        expected_BLK_opp_BLK = divide(previous_average[:BLK_mean].to_f * opp_average_v_position[:BLK_mean].to_f, league_average_v_position[:team_mean_BLK].to_f)
        expected_STl_opp_STl = divide(previous_average[:STl_mean].to_f * opp_average_v_position[:STl_mean].to_f, league_average_v_position[:team_mean_STl].to_f)
        expected_PTS_opp_PTS_per_min = divide(60*expected_PTS_opp_PTS, average_seconds)
        expected_OREB_opp_OREB_per_min = divide(60*expected_OREB_opp_OREB, average_seconds)
        expected_DREB_opp_DREB_per_min = divide(60*expected_DREB_opp_DREB, average_seconds)
        expected_AST_opp_AST_per_min = divide(60*expected_AST_opp_AST, average_seconds)
        expected_TOV_opp_TOV_per_min = divide(60*expected_TOV_opp_TOV, average_seconds)
        expected_BLK_opp_BLK_per_min = divide(60*expected_BLK_opp_BLK, average_seconds)
        expected_STl_opp_STl_per_min = divide(60*expected_STl_opp_STl, average_seconds)
      else
        expected_PTS_opp_PTS = 0
        expected_OREB_opp_OREB = 0
        expected_DREB_opp_DREB = 0
        expected_AST_opp_AST = 0
        expected_TOV_opp_TOV = 0
        expected_BLK_opp_BLK = 0
        expected_STl_opp_STl = 0
      end
      expected_PTS_opp_PTS_effect = expected_PTS_opp_PTS - previous_average[:PTS_mean].to_f
      expected_OREB_opp_OREB_effect = expected_OREB_opp_OREB - previous_average[:OREB_mean].to_f
      expected_DREB_opp_DREB_effect = expected_DREB_opp_DREB - previous_average[:DREB_mean].to_f
      expected_AST_opp_AST_effect = expected_AST_opp_AST - previous_average[:AST_mean].to_f
      expected_TOV_opp_TOV_effect = expected_TOV_opp_TOV - previous_average[:TOV_mean].to_f
      expected_BLK_opp_BLK_effect = expected_BLK_opp_BLK - previous_average[:BLK_mean].to_f
      expected_STl_opp_STl_effect = expected_STl_opp_STl - previous_average[:STl_mean].to_f
      expected_PTS_opp_PTS_effect_per_min = divide(60*expected_PTS_opp_PTS_effect, average_seconds)
      expected_OREB_opp_OREB_effect_per_min = divide(60*expected_OREB_opp_OREB_effect, average_seconds)
      expected_DREB_opp_DREB_effect_per_min = divide(60*expected_DREB_opp_DREB_effect, average_seconds)
      expected_AST_opp_AST_effect_per_min = divide(60*expected_AST_opp_AST_effect, average_seconds)
      expected_TOV_opp_TOV_effect_per_min = divide(60*expected_TOV_opp_TOV_effect, average_seconds)
      expected_BLK_opp_BLK_effect_per_min = divide(60*expected_BLK_opp_BLK_effect, average_seconds)
      expected_STl_opp_STl_effect_per_min = divide(60*expected_STl_opp_STl_effect, average_seconds)

      if opp_average and team_average
        fb_effect = previous_average[:PTS_FB_mean].to_f * divide(opp_average[:o_PTS_FB_mean].to_f, team_average[:PTS_FB_mean].to_f) - previous_average[:PTS_FB_mean].to_f
      else
        fb_effect = 0
      end
      fb_effect_per_min = divide(60*fb_effect, average_seconds)

      if opp_average and team_average
        pts_paint_effect = previous_average[:PTS_PAINT_mean].to_f * divide(opp_average[:o_PTS_PAINT_mean].to_f, team_average[:PTS_PAINT_mean].to_f) - previous_average[:PTS_PAINT_mean].to_f
      else
        pts_paint_effect = 0
      end
      pts_paint_effect_per_min = divide(60*pts_paint_effect, average_seconds)

      if opp_average and team_average
        pts_2ndchance_effect = previous_average[:PTS_2ND_CHANCE_mean].to_f * divide(opp_average[:o_PTS_2ND_CHANCE_mean].to_f, team_average[:PTS_2ND_CHANCE_mean].to_f) - previous_average[:PTS_2ND_CHANCE_mean].to_f
      else
        pts_2ndchance_effect = 0
      end
      pts_2ndchance_effect_per_min = divide(60*pts_2ndchance_effect, average_seconds)

      if opp_average and team_average
        pts_off_tov_effect = previous_average[:PTS_OFF_TOV_mean].to_f * divide(opp_average[:o_PTS_OFF_TOV_mean].to_f, team_average[:PTS_OFF_TOV_mean].to_f) - previous_average[:PTS_OFF_TOV_mean].to_f
      else
        pts_off_tov_effect = 0
      end
      pts_off_tov_effect_per_min = divide(60*pts_off_tov_effect, average_seconds)

      fg_pts = previous_average[:PTS_mean].to_f - previous_average[:FTM_mean].to_f
      pts_per_fg = divide(fg_pts, previous_average[:FG3M_mean].to_f + previous_average[:FG2M_mean].to_f)
      cfg_pts = pts_per_fg * prev_PCT_CFGM
      ufg_pts = pts_per_fg * prev_PCT_UFGM

      pct_cfga_ratio = divide(opp_o_PCT_CFGA, prev_PCT_CFGA) 
      pct_cfga_pos_ratio = divide(opp_o_PCT_CFGA_v_position, prev_PCT_CFGA) 
      pct_cfga_mixed_ratio = (pct_cfga_ratio + pct_cfga_pos_ratio) / 2
      adjusted_CFGA = previous_average[:CFGA_mean].to_f * pct_cfga_mixed_ratio 

      if opp_average_v_position
        adjusted_opp_cfg_pct = divide(previous_average[:o_CFG_PCT].to_f + opp_average_v_position[:CFG_PCT].to_f, 2)
      else
        adjusted_opp_cfg_pct = 0
      end
      adjusted_cfg_pct = (previous_average[:CFG_PCT].to_f + adjusted_opp_cfg_pct) / 2
      adjusted_cfgm = adjusted_CFGA * adjusted_cfg_pct

      adjusted_cfg_pts = adjusted_cfgm * pts_per_fg
      adjusted_cfg_pts_per_min = divide(60*adjusted_cfg_pts, average_seconds)

      pct_ufga_ratio = divide(opp_o_PCT_UFGA, prev_PCT_UFGA) 
      pct_ufga_pos_ratio = divide(opp_o_PCT_UFGA_v_position, prev_PCT_UFGA) 
      pct_ufga_mixed_ratio = (pct_ufga_ratio + pct_ufga_pos_ratio) / 2
      adjusted_UFGA = previous_average[:UFGA_mean].to_f * pct_ufga_mixed_ratio 

      if opp_average and opp_average_v_position
        adjusted_opp_ufg_pct = divide(opp_average[:o_UFG_PCT].to_f + opp_average_v_position[:UFG_PCT].to_f, 2)
      else
        adjusted_opp_ufg_pct = 0
      end
      adjusted_ufg_pct = (previous_average[:UFG_PCT].to_f + adjusted_opp_ufg_pct) / 2
      adjusted_ufgm = adjusted_UFGA * adjusted_cfg_pct

      adjusted_ufg_pts = adjusted_ufgm * pts_per_fg
      adjusted_ufg_pts_per_min = divide(60*adjusted_ufg_pts, average_seconds)

      adjusted_fg_pts = adjusted_ufg_pts + adjusted_cfg_pts
      adjusted_fg_pts_per_min = divide(60*adjusted_fg_pts, average_seconds)

      cfg_effect = adjusted_fg_pts - fg_pts
      cfg_effect_per_min = divide(60*cfg_effect, average_seconds)
      
      previous_average_starter = player_averages_starter[i-1]
      previous_average_bench = player_averages_bench[i-1]
      if "1" == previous_average[:starter]
        mean_starterbench_seconds = mean_starter_seconds = previous_average_starter[:seconds_played_mean].to_f
        mean_starterbench_pts = mean_starter_pts = previous_average_starter[:PTS_mean].to_f
        mean_starterbench_oreb = mean_starter_oreb = previous_average_starter[:OREB_mean].to_f
        mean_starterbench_dreb = mean_starter_dreb = previous_average_starter[:DREB_mean].to_f
        mean_starterbench_ast = mean_starter_ast = previous_average_starter[:AST_mean].to_f
        mean_starterbench_tov = mean_starter_tov = previous_average_starter[:TOV_mean].to_f
        mean_starterbench_blk = mean_starter_blk = previous_average_starter[:BLK_mean].to_f
        mean_starterbench_stl = mean_starter_stl = previous_average_starter[:STL_mean].to_f
        mean_starterbench_pts_per_min = mean_starter_pts_per_min = (60*mean_starterbench_pts) / mean_starterbench_seconds
        mean_starterbench_oreb_per_min = mean_starter_oreb_per_min = (60*mean_starter_oreb) / mean_starterbench_seconds
        mean_starterbench_dreb_per_min = mean_starter_dreb_per_min = (60*mean_starter_dreb) / mean_starterbench_seconds
        mean_starterbench_ast_per_min = mean_starter_ast_per_min = (60*mean_starter_ast) / mean_starterbench_seconds
        mean_starterbench_tov_per_min = mean_starter_tov_per_min = (60*mean_starter_tov) / mean_starterbench_seconds
        mean_starterbench_blk_per_min = mean_starter_blk_per_min = (60*mean_starter_blk) / mean_starterbench_seconds
        mean_starterbench_stl_per_min = mean_starter_stl_per_min = (60*mean_starter_stl) / mean_starterbench_seconds
      elsif "0" == previous_average[:starter]
        mean_starterbench_seconds = mean_bench_seconds = previous_average_bench[:seconds_played_mean].to_f
        mean_starterbench_pts = mean_bench_pts = previous_average_bench[:PTS_mean].to_f
        mean_starterbench_oreb = mean_bench_oreb = previous_average_bench[:OREB_mean].to_f
        mean_starterbench_dreb = mean_bench_dreb = previous_average_bench[:DREB_mean].to_f
        mean_starterbench_ast = mean_bench_ast = previous_average_bench[:AST_mean].to_f
        mean_starterbench_tov = mean_bench_tov = previous_average_bench[:TOV_mean].to_f
        mean_starterbench_blk = mean_bench_blk = previous_average_bench[:BLK_mean].to_f
        mean_starterbench_stl = mean_bench_stl = previous_average_bench[:STL_mean].to_f

        mean_starterbench_pts_per_min = mean_bench_pts_per_min = (60*mean_bench_pts) / mean_bench_seconds
        mean_starterbench_oreb_per_min = mean_bench_oreb_per_min = (60*mean_bench_oreb) / mean_bench_seconds
        mean_starterbench_dreb_per_min = mean_bench_dreb_per_min = (60*mean_bench_dreb) / mean_bench_seconds
        mean_starterbench_ast_per_min = mean_bench_ast_per_min = (60*mean_bench_ast) / mean_bench_seconds
        mean_starterbench_tov_per_min = mean_bench_tov_per_min = (60*mean_bench_tov) / mean_bench_seconds
        mean_starterbench_blk_per_min = mean_bench_blk_per_min = (60*mean_bench_blk) / mean_bench_seconds
        mean_starterbench_stl_per_min = mean_bench_stl_per_min = (60*mean_bench_stl) / mean_bench_seconds
      else
        binding.pry
        p "hi"
      end

      starterbench_pts_effect = mean_starterbench_pts - previous_average[:PTS_mean].to_f
      starterbench_oreb_effect = mean_starterbench_oreb - previous_average[:OREB_mean].to_f
      starterbench_dreb_effect = mean_starterbench_dreb - previous_average[:DREB_mean].to_f
      starterbench_ast_effect = mean_starterbench_ast - previous_average[:AST_mean].to_f
      starterbench_tov_effect = mean_starterbench_tov - previous_average[:TOV_mean].to_f
      starterbench_blk_effect = mean_starterbench_blk - previous_average[:BLK_mean].to_f
      starterbench_stl_effect = mean_starterbench_stl - previous_average[:STL_mean].to_f

      starterbench_pts_effect_per_min = (60*starterbench_pts_effect) / mean_starterbench_seconds
      starterbench_oreb_effect_per_min = (60*mean_starterbench_oreb_effect) / mean_starterbench_seconds
      starterbench_dreb_effect_per_min = (60*mean_starterbench_dreb_effect) / mean_starterbench_seconds
      starterbench_ast_effect_per_min = (60*mean_starterbench_ast_effect) / mean_starterbench_seconds
      starterbench_tov_effect_per_min = (60*mean_starterbench_tov_effect) / mean_starterbench_seconds
      starterbench_blk_effect_per_min = (60*mean_starterbench_blk_effect) / mean_starterbench_seconds
      starterbench_stl_effect_per_min = (60*mean_starterbench_stl_effect) / mean_starterbench_seconds

      if i > 0
        prev_pts = player_boxscores[i-1][:PTS].to_f
        prev_oreb = player_boxscores[i-1][:OREB].to_f
        prev_dreb = player_boxscores[i-1][:DREB].to_f
        prev_stl = player_boxscores[i-1][:STL].to_f
        prev_blk = player_boxscores[i-1][:BLK].to_f
        prev_ast = player_boxscores[i-1][:AST].to_f
        prev_tov = player_boxscores[i-1][:TO].to_f

        begin
          prev_seconds = Duration.new( :minutes => player_boxscores[i-1][:MIN].split(":")[0], :seconds => player_boxscores[i-1][:MIN].split(":")[1] ).total
        rescue StandardError => e
          binding.pry
          p 'hi'
        end

        prev_pts_per_min = prev_pts / prev_seconds
        prev_oreb_per_min = prev_oreb / prev_seconds
        prev_dreb_per_min = prev_dreb / prev_seconds
        prev_stl_per_min = prev_stl / prev_seconds
        prev_blk_per_min = prev_blk / prev_seconds
        prev_ast_per_min = prev_ast / prev_seconds
        prev_tov_per_min = prev_tov / prev_seconds
      else
        prev_pts = previous_average[:PTS_mean].to_f
        prev_oreb = previous_average[:OREB_mean].to_f
        prev_dreb = previous_average[:DREB_mean].to_f
        prev_stl = previous_average[:STL_mean].to_f
        prev_blk = previous_average[:BLK_mean].to_f
        prev_ast = previous_average[:AST_mean].to_f
        prev_tov = previous_average[:TOV_mean].to_f
        prev_seconds = average_seconds

        prev_pts_per_min = previous_average[:PTS_mean].to_f / average_seconds
        prev_oreb_per_min = previous_average[:OREB_mean].to_f / average_seconds
        prev_dreb_per_min = previous_average[:DREB_mean].to_f / average_seconds
        prev_stl_per_min = previous_average[:STL_mean].to_f / average_seconds
        prev_blk_per_min = previous_average[:BLK_mean].to_f / average_seconds
        prev_ast_per_min = previous_average[:AST_mean].to_f / average_seconds
        prev_tov_per_min = previous_average[:TOV_mean].to_f / average_seconds
      end

      if i > 1
        prev2_pts = previous_average_prev2[:PTS_mean].to_f
        prev2_oreb = previous_average_prev2[:OREB_mean].to_f
        prev2_dreb = previous_average_prev2[:DREB_mean].to_f
        prev2_stl = previous_average_prev2[:STL_mean].to_f
        prev2_blk = previous_average_prev2[:BLK_mean].to_f
        prev2_ast = previous_average_prev2[:AST_mean].to_f
        prev2_tov = previous_average_prev2[:TOV_mean].to_f
        prev2_seconds = previous_average_prev2[:seconds_played_mean].to_f

        prev2_pts_per_min = previous_average_prev2[:PTS_mean].to_f / previous_average_prev2[:seconds_played_mean].to_f
        prev2_oreb_per_min = previous_average_prev2[:OREB_mean].to_f / previous_average_prev2[:seconds_played_mean].to_f
        prev2_dreb_per_min = previous_average_prev2[:DREB_mean].to_f / previous_average_prev2[:seconds_played_mean].to_f
        prev2_stl_per_min = previous_average_prev2[:STL_mean].to_f / previous_average_prev2[:seconds_played_mean].to_f
        prev2_blk_per_min = previous_average_prev2[:BLK_mean].to_f / previous_average_prev2[:seconds_played_mean].to_f
        prev2_ast_per_min = previous_average_prev2[:AST_mean].to_f / previous_average_prev2[:seconds_played_mean].to_f
        prev2_tov_per_min = previous_average_prev2[:TOV_mean].to_f / previous_average_prev2[:seconds_played_mean].to_f
      else
        prev2_pts = previous_average[:PTS_mean].to_f
        prev2_oreb = previous_average[:OREB_mean].to_f
        prev2_dreb = previous_average[:DREB_mean].to_f
        prev2_stl = previous_average[:STL_mean].to_f
        prev2_blk = previous_average[:BLK_mean].to_f
        prev2_ast = previous_average[:AST_mean].to_f
        prev2_tov = previous_average[:TOV_mean].to_f
        prev2_seconds = average_seconds

        prev2_pts_per_min = previous_average[:PTS_mean].to_f / average_seconds
        prev2_oreb_per_min = previous_average[:OREB_mean].to_f / average_seconds
        prev2_dreb_per_min = previous_average[:DREB_mean].to_f / average_seconds
        prev2_stl_per_min = previous_average[:STL_mean].to_f / average_seconds
        prev2_blk_per_min = previous_average[:BLK_mean].to_f / average_seconds
        prev2_ast_per_min = previous_average[:AST_mean].to_f / average_seconds
        prev2_tov_per_min = previous_average[:TOV_mean].to_f / average_seconds
      end

      if i > 4
        prev5_pts = previous_average_prev5[:PTS_mean].to_f
        prev5_oreb = previous_average_prev5[:OREB_mean].to_f
        prev5_dreb = previous_average_prev5[:DREB_mean].to_f
        prev5_stl = previous_average_prev5[:STL_mean].to_f
        prev5_blk = previous_average_prev5[:BLK_mean].to_f
        prev5_ast = previous_average_prev5[:AST_mean].to_f
        prev5_tov = previous_average_prev5[:TOV_mean].to_f
        prev5_seconds = previous_average_prev5[:seconds_played_mean].to_f

        prev5_pts_per_min = previous_average_prev5[:PTS_mean].to_f / previous_average_prev5[:seconds_played_mean].to_f
        prev5_oreb_per_min = previous_average_prev5[:OREB_mean].to_f / previous_average_prev5[:seconds_played_mean].to_f
        prev5_dreb_per_min = previous_average_prev5[:DREB_mean].to_f / previous_average_prev5[:seconds_played_mean].to_f
        prev5_stl_per_min = previous_average_prev5[:STL_mean].to_f / previous_average_prev5[:seconds_played_mean].to_f
        prev5_blk_per_min = previous_average_prev5[:BLK_mean].to_f / previous_average_prev5[:seconds_played_mean].to_f
        prev5_ast_per_min = previous_average_prev5[:AST_mean].to_f / previous_average_prev5[:seconds_played_mean].to_f
        prev5_tov_per_min = previous_average_prev5[:TOV_mean].to_f / previous_average_prev5[:seconds_played_mean].to_f
      else
        prev5_pts = previous_average[:PTS_mean].to_f
        prev5_oreb = previous_average[:OREB_mean].to_f
        prev5_dreb = previous_average[:DREB_mean].to_f
        prev5_stl = previous_average[:STL_mean].to_f
        prev5_blk = previous_average[:BLK_mean].to_f
        prev5_ast = previous_average[:AST_mean].to_f
        prev5_tov = previous_average[:TOV_mean].to_f
        prev5_seconds = average_seconds

        prev5_pts_per_min = previous_average[:PTS_mean].to_f / average_seconds
        prev5_oreb_per_min = previous_average[:OREB_mean].to_f / average_seconds
        prev5_dreb_per_min = previous_average[:DREB_mean].to_f / average_seconds
        prev5_stl_per_min = previous_average[:STL_mean].to_f / average_seconds
        prev5_blk_per_min = previous_average[:BLK_mean].to_f / average_seconds
        prev5_ast_per_min = previous_average[:AST_mean].to_f / average_seconds
        prev5_tov_per_min = previous_average[:TOV_mean].to_f / average_seconds
      end

      ft_pct = divide( previous_average[:FTM_mean].to_f, previous_average[:FTA_mean].to_f )
      if team_average and opp_average
        expected_FTA = previous_average[:FTA_mean].to_f * divide( team_average[:FTA_mean].to_f, opp_average[:o_FTA_mean].to_f ) 
      else
        expected_FTA = 0
      end
      expected_FTM = ft_pct * expected_FTA
      expected_FTM_per_min = expected_FTM / average_seconds

      extra_FTA = expected_FTA - previous_average[:FTA_mean].to_f
      ft_effect = ft_pct * extra_FTA
      ft_effect_per_min = ft_effect / average_seconds

      prev_seconds_delta = prev_seconds - mean_seconds
      prev2_seconds_delta = prev2_seconds - mean_seconds
      prev5_seconds_delta = prev5_seconds - mean_seconds
      prev_pts_delta = prev_pts - previous_average[:PTS_mean].to_f
      prev2_pts_delta = prev2_pts - previous_average[:PTS_mean].to_f
      prev5_pts_delta = prev5_pts - previous_average[:PTS_mean].to_f
      prev_oreb_delta = prev_oreb - previous_average[:OREB_mean].to_f
      prev2_oreb_delta = prev2_oreb - previous_average[:OREB_mean].to_f
      prev5_oreb_delta = prev5_oreb - previous_average[:OREB_mean].to_f
      prev_dreb_delta = prev_dreb - previous_average[:DREB_mean].to_f
      prev2_dreb_delta = prev2_dreb - previous_average[:DREB_mean].to_f
      prev5_dreb_delta = prev5_dreb - previous_average[:DREB_mean].to_f
      prev_ast_delta = prev_ast - previous_average[:AST_mean].to_f
      prev2_ast_delta = prev2_ast - previous_average[:AST_mean].to_f
      prev5_ast_delta = prev5_ast - previous_average[:AST_mean].to_f
      prev_tov_delta = prev_tov - previous_average[:TOV_mean].to_f
      prev2_tov_delta = prev2_tov - previous_average[:TOV_mean].to_f
      prev5_tov_delta = prev5_tov - previous_average[:TOV_mean].to_f
      prev_blk_delta = prev_blk - previous_average[:BLK_mean].to_f
      prev2_blk_delta = prev2_blk - previous_average[:BLK_mean].to_f
      prev5_blk_delta = prev5_blk - previous_average[:BLK_mean].to_f
      prev_stl_delta = prev_stl - previous_average[:STL_mean].to_f
      prev2_stl_delta = prev2_stl - previous_average[:STL_mean].to_f
      prev5_stl_delta = prev5_stl - previous_average[:STL_mean].to_f

      prev_pts_delta_per_min = prev_pts_delta / average_seconds
      prev2_pts_delta_per_min = prev2_pts_delta / average_seconds
      prev5_pts_delta_per_min = prev5_pts_delta / average_seconds
      prev_oreb_delta_per_min = prev_oreb_delta / average_seconds
      prev2_oreb_delta_per_min = prev2_oreb_delta / average_seconds
      prev5_oreb_delta_per_min = prev5_oreb_delta / average_seconds
      prev_dreb_delta_per_min = prev_dreb_delta / average_seconds
      prev2_dreb_delta_per_min = prev2_dreb_delta / average_seconds
      prev5_dreb_delta_per_min = prev5_dreb_delta / average_seconds
      prev_ast_delta_per_min = prev_ast_delta / average_seconds
      prev2_ast_delta_per_min = prev2_ast_delta / average_seconds
      prev5_ast_delta_per_min = prev5_ast_delta / average_seconds
      prev_tov_delta_per_min = prev_tov_delta / average_seconds
      prev2_tov_delta_per_min = prev2_tov_delta / average_seconds
      prev5_tov_delta_per_min = prev5_tov_delta / average_seconds
      prev_blk_delta_per_min = prev_blk_delta / average_seconds
      prev2_blk_delta_per_min = prev2_blk_delta / average_seconds
      prev5_blk_delta_per_min = prev5_blk_delta / average_seconds
      prev_stl_delta_per_min = prev_stl_delta / average_seconds
      prev2_stl_delta_per_min = prev2_stl_delta / average_seconds
      prev5_stl_delta_per_min = prev5_stl_delta / average_seconds

      if opp_average_v_position
        o_pts_delta_per_min = 60* (divide(opp_average_v_position[:PTS_mean].to_f, opp_average_v_position[:team_mean_SECONDS].to_i - divide(league_average_v_position[:team_mean_PTS].to_f, league_average[:team_mean_SECONDS].to_f)))
        o_oreb_delta_per_min = 60* (divide(opp_average_v_position[:OREB_mean].to_f, opp_average_v_position[:team_mean_SECONDS].to_i - divide(league_average_v_position[:team_mean_OREB].to_f, league_average[:team_mean_SECONDS].to_f)))
        o_dreb_delta_per_min = 60* (divide(opp_average_v_position[:DREB_mean].to_f, opp_average_v_position[:team_mean_SECONDS].to_i - divide(league_average_v_position[:team_mean_DREB].to_f, league_average[:team_mean_SECONDS].to_f)))
        o_ast_delta_per_min = 60* (divide(opp_average_v_position[:AST_mean].to_f, opp_average_v_position[:team_mean_SECONDS].to_i - divide(league_average_v_position[:team_mean_AST].to_f, league_average[:team_mean_SECONDS].to_f)))
        o_tov_delta_per_min = 60* (divide(opp_average_v_position[:TOV_mean].to_f, opp_average_v_position[:team_mean_SECONDS].to_i - divide(league_average_v_position[:team_mean_TOV].to_f, league_average[:team_mean_SECONDS].to_f)))
        o_blk_delta_per_min = 60* (divide(opp_average_v_position[:BLK_mean].to_f, opp_average_v_position[:team_mean_SECONDS].to_i - divide(league_average_v_position[:team_mean_BLK].to_f, league_average[:team_mean_SECONDS].to_f)))
        o_stl_delta_per_min = 60* (divide(opp_average_v_position[:STL_mean].to_f, opp_average_v_position[:team_mean_SECONDS].to_i - divide(league_average_v_position[:team_mean_STL].to_f, league_average[:team_mean_SECONDS].to_f)))
      else
        o_pts_delta_per_min = 0
        o_oreb_delta_per_min = 0
        o_dreb_delta_per_min = 0
        o_ast_delta_per_min = 0
        o_tov_delta_per_min = 0
        o_blk_delta_per_min = 0
        o_stl_delta_per_min = 0
      end

      actual_pts_per_min = divide(60*boxscore[:PTS].to_i, actual_SECONDS )
      actual_oreb_per_min = divide(60*boxscore[:OREB].to_i, actual_SECONDS )
      actual_dreb_per_min = divide(60*boxscore[:DREB].to_i, actual_SECONDS )
      actual_ast_per_min = divide(60*boxscore[:AST].to_i, actual_SECONDS )
      actual_tov_per_min = divide(60*boxscore[:TOV].to_i, actual_SECONDS )
      actual_blk_per_min = divide(60*boxscore[:BLK].to_i, actual_SECONDS )
      actual_stl_per_min = divide(60*boxscore[:STL].to_i, actual_SECONDS )
      actual_mins = actual_SECONDS / 60.0

      #expected_FTA_v_position = previous_average[:FTA_mean].to_f * divide( team_average[:FTA_mean].to_f, opp_average[:o_FTA_mean].to_f ) 

      if team_average and opp_average 
        expected_OREB = previous_average[:OREB_mean].to_f * divide(opp_average[:o_team_OREB_mean].to_f, team_average[:OREB_mean].to_f)
        expected_OREB_effect = previous_average[:OREB_mean].to_f * divide(opp_average[:o_team_OREB_mean].to_f, team_average[:OREB_mean].to_f) - previous_average[:OREB_mean].to_f
        expected_OREB_per_min = expected_OREB_per_min / actual_mins
        expected_OREB_effect_per_min = expected_OREB_effect_per_min / actual_mins

        expected_DREB = previous_average[:DREB_mean].to_f * divide(opp_average[:o_team_DREB_mean].to_f, team_average[:DREB_mean].to_f)
        expected_DREB_effect = previous_average[:DREB_mean].to_f * divide(opp_average[:o_team_DREB_mean].to_f, team_average[:DREB_mean].to_f) - previous_average[:DREB_mean].to_f
        expected_DREB_per_min = expected_DREB_per_min / actual_mins
        expected_DREB_effect_per_min = expected_DREB_effect_per_min / actual_mins

        #binding.pry#look at dreb team 
        expected_AST = previous_average[:AST_mean].to_f * divide(opp_average[:o_team_AST_mean].to_f.to_f, team_average[:AST_mean].to_f.to_f)
        expected_AST_effect = previous_average[:AST_mean].to_f * divide(opp_average[:o_team_AST_mean].to_f.to_f, team_average[:AST_mean].to_f.to_f) - previous_average[:AST_mean].to_f

        expected_AST_per_min = expected_AST_per_min / actual_mins
        expected_AST_effect_per_min = expected_AST_effect_per_min / actual_mins

        expected_TOV = previous_average[:TOV_mean].to_f * divide(opp_average[:TOV_mean].to_f, team_average[:TOV_mean].to_f.to_f)
        expected_TOV_effect = previous_average[:TOV_mean].to_f * divide(opp_average[:TOV_mean].to_f, team_average[:TOV_mean].to_f.to_f) - previous_average[:TOV_mean].to_f

        expected_TOV_per_min = expected_TOV_per_min / actual_mins
        expected_TOV_effect_per_min = expected_TOV_effect_per_min / actual_mins

        expected_BLK = previous_average[:BLK_mean].to_f * divide(opp_average[:o_team_BLK_mean].to_f.to_f, team_average[:BLK_mean].to_f.to_f)
        expected_BLK_effect = previous_average[:BLK_mean].to_f * divide(opp_average[:o_team_BLK_mean].to_f.to_f, team_average[:BLK_mean].to_f.to_f) - previous_average[:BLK_mean].to_f

        expected_BLK_per_min = expected_BLK_per_min / actual_mins
        expected_BLK_effect_per_min = expected_BLK_effect_per_min / actual_mins

        expected_STL = previous_average[:STL_mean].to_f * divide(opp_average[:o_team_STL_mean].to_f.to_f, team_average[:STL_mean].to_f.to_f)
        expected_STL_effect = previous_average[:STL_mean].to_f * divide(opp_average[:o_team_STL_mean].to_f.to_f, team_average[:STL_mean].to_f.to_f) - previous_average[:STL_mean].to_f

        expected_STL_per_min = expected_STL_per_min / actual_mins
        expected_STL_effect_per_min = expected_STL_effect_per_min / actual_mins

        scaled_oreb_pct = previous_average[:OREB_PCT].to_f * divide(opp_average_v_position[:OREB_PCT].to_f, league_average[:team_mean_OREB_PCT].to_f)
        scaled_oreb_pct_effect = scaled_oreb_pct - previous_average[:OREB_PCT].to_f
        scaled_dreb_pct = previous_average[:DREB_PCT].to_f * divide(opp_average_v_position[:DREB_PCT].to_f.to_f, league_average[:team_mean_DREB].to_f)
        scaled_dreb_pct_effect = scaled_dreb_pct - previous_average[:DREB_PCT].to_f
        scaled_assist_pct = previous_average[:AST_PCT].to_f * divide(opp_average_v_position[:AST_PCT].to_f, league_average[:team_mean_AST_PCT].to_f)
        scaled_assist_pct_effect = scaled_assist_pct - previous_average[:AST_PCT].to_f
        scaled_turnover_pct = previous_average[:TO_PCT].to_f * divide(opp_average_v_position[:TO_PCT].to_f.to_f, league_average[:team_mean_TOV_PCT].to_f)
        scaled_turnover_pct_effect = scaled_turnover_pct - previous_average[:TO_PCT].to_f
        scaled_block_pct = previous_average[:PCT_BLK].to_f * divide(opp_average_v_position[:PCT_BLK].to_f.to_f, league_average[:team_mean_PCT_BLK].to_f)
        scaled_block_pct_effect = scaled_block_pct - previous_average[:PCT_BLK].to_f
        scaled_pct_stl = previous_average[:PCT_STL].to_f * divide(opp_average_v_position[:PCT_STL].to_f.to_f, league_average[:team_mean_PCT_STL].to_f)
        scaled_pct_stl_effect = scaled_pct_stl - previous_average[:PCT_STL].to_f

        scaled_oreb = previous_average[:OREB_mean].to_f * divide(opp_average_v_position[:OREB_mean].to_f, league_average[:team_mean_OREB].to_f)
        scaled_oreb_effect = scaled_oreb - previous_average[:OREB_mean].to_f
        scaled_oreb_per_min = scaled_oreb_per_min / actual_mins
        scaled_oreb_effect_per_min = scaled_oreb_effect_per_min / actual_mins
        
        scaled_dreb = previous_average[:DREB_mean].to_f * divide(opp_average_v_position[:DREB_mean].to_f.to_f, league_average[:team_mean_DREB].to_f)
        scaled_dreb_effect = scaled_dreb - previous_average[:DREB_mean].to_f
        scaled_dreb_per_min = scaled_dreb_per_min / actual_mins
        scaled_dreb_effect_per_min = scaled_dreb_effect_per_min / actual_mins

        scaled_assist = previous_average[:AST_mean].to_f * divide(opa_AST, league_average[:team_mean_AST].to_f)
        scaled_assist_effect = scaled_assist - previous_average[:AST_mean].to_f
        scaled_assist_per_min = scaled_assist_per_min / actual_mins
        scaled_assist_effect_per_min = scaled_assist_effect_per_min / actual_mins

        scaled_turnover = previous_average[:TOV_mean].to_f * divide(opp_average_v_position[:TOV_mean].to_f, league_average[:team_mean_TOV].to_f)
        scaled_turnover_effect = scaled_turnover - previous_average[:TOV_mean].to_f
        scaled_turnover_per_min = scaled_turnover_per_min / actual_mins
        scaled_turnover_effect_per_min = scaled_turnover_effect_per_min / actual_mins

        scaled_block = previous_average[:BLK_mean].to_f * divide(opp_average_v_position[:BLK_mean].to_f, league_average[:team_mean_BLK].to_f)
        scaled_block_effect = scaled_block - previous_average[:BLK_mean].to_f
        scaled_block_per_min = scaled_block_per_min / actual_mins
        scaled_block_effect_per_min = scaled_block_effect_per_min / actual_mins

        scaled_steal = previous_average[:STL_mean].to_f * divide(opp_average_v_position[:STL_mean].to_f, league_average[:team_mean_STL].to_f)
        scaled_steal_effect = scaled_steal - previous_average[:STL_mean].to_f
        scaled_steal_per_min = scaled_steal_per_min / actual_mins
        scaled_steal_effect_per_min = scaled_steal_effect_per_min / actual_mins
      else
        expected_OREB = previous_average[:OREB_mean].to_f
        expected_OREB_effect = 0
        expected_oreb_per_min = expected_OREB_per_min / actual_mins
        expected_oreb_effect_per_min = 0
        expected_DREB = previous_average[:DREB_mean].to_f
        expected_DREB_effect = 0
        expected_dreb_per_min = expected_DREB_per_min / actual_mins
        expected_dreb_effect_per_min = 0
        expected_AST = previous_average[:AST_mean].to_f
        expected_AST_effect = 0
        expected_assist_per_min = expected_AST_per_min / actual_mins
        expected_assist_effect_per_min = 0
        expected_TOV = previous_average[:TOV_mean].to_f
        expected_TOV_effect = 0
        expected_turnover_per_min = expected_TOV_per_min / actual_mins
        expected_turnover_effect_per_min = 0
        expected_BLK = previous_average[:BLK_mean].to_f
        expected_BLK_effect = 0
        expected_block_per_min = expected_BLK_per_min / actual_mins
        expected_block_effect_per_min = 0
        expected_STL = previous_average[:STL_mean].to_f
        expected_STL_effect = 0
        expected_steal_per_min = expected_STL_per_min / actual_mins
        expected_steal_effect_per_min = 0

        scaled_oreb_pct = previous_average[:OREB_PCT].to_f
        scaled_oreb_pct_effect = 0
        scaled_dreb_pct = previous_average[:DREB_PCT].to_f
        scaled_dreb_pct_effect = 0
        scaled_assist_pct = previous_average[:AST_PCT].to_f
        scaled_assist_pct_effect = 0
        scaled_turnover_pct = previous_average[:TO_PCT].to_f
        scaled_turnover_pct_effect = 0
        scaled_block_pct = previous_average[:PCT_BLK].to_f
        scaled_block_pct_effect = 0
        scaled_pct_stl = previous_average[:PCT_STL].to_f
        scaled_pct_stl_effect = 0

        scaled_oreb = previous_average[:OREB_mean].to_f
        scaled_oreb_effect = 0
        scaled_oreb_per_min = scaled_oreb_per_min / actual_mins
        scaled_oreb_effect_per_min = 0
        scaled_dreb = previous_average[:DREB_mean].to_f
        scaled_dreb_effect = 0
        scaled_dreb_per_min = scaled_dreb_per_min / actual_mins
        scaled_dreb_effect_per_min = 0
        scaled_assist = previous_average[:AST_mean].to_f
        scaled_assist_effect = 0
        scaled_assist_per_min = scaled_assist_per_min / actual_mins
        scaled_assist_effect_per_min = 0
        scaled_turnover = previous_average[:TOV_mean].to_f
        scaled_turnover_effect = 0
        scaled_turnover_per_min = scaled_turnover_per_min / actual_mins
        scaled_turnover_effect_per_min = 0
        scaled_block = previous_average[:BLK_mean].to_f
        scaled_block_effect = 0
        scaled_block_per_min = scaled_block_per_min / actual_mins
        scaled_block_effect_per_min = 0
        scaled_steal = previous_average[:STL_mean].to_f
        scaled_steal_effect = 0
        scaled_steal_per_min = scaled_steal_per_min / actual_mins
        scaled_steal_effect_per_min = 0
      end

      modded_oreb = (previous_average[:OREB_mean].to_f + league_average[:team_mean_DREB].to_f) / 2
      modded_oreb_effect = modded_oreb - previous_average[:OREB_mean].to_f
      modded_oreb_per_min = modded_oreb_per_min / actual_mins
      modded_oreb_effect_per_min = 0
      modded_dreb = (previous_average[:DREB_mean].to_f + league_average[:team_mean_DREB].to_f) / 2
      modded_dreb_effect = modded_dreb - previous_average[:DREB_mean].to_f
      modded_dreb_per_min = modded_dreb_per_min / actual_mins
      modded_dreb_effect_per_min = 0
      modded_assist = (previous_average[:AST_mean].to_f + league_average[:team_mean_AST].to_f) / 2
      modded_assist_effect = modded_assist - previous_average[:AST_mean].to_f
      modded_assist_per_min = modded_assist_per_min / actual_mins
      modded_assist_effect_per_min = 0
      modded_turnover = (previous_average[:TOV_mean].to_f + league_average[:team_mean_TOV].to_f) / 2
      modded_turnover_effect = modded_turnover - previous_average[:TOV_mean].to_f
      modded_turnover_per_min = modded_turnover_per_min / actual_mins
      modded_turnover_effect_per_min = 0
      modded_block = (previous_average[:BLK_mean].to_f + league_average[:team_mean_BLK].to_f) / 2
      modded_block_effect = modded_block - previous_average[:BLK_mean].to_f
      modded_block_per_min = modded_block_per_min / actual_mins
      modded_block_effect_per_min = 0
      modded_steal = (previous_average[:STL_mean].to_f + league_average[:team_mean_STL].to_f) / 2
      modded_steal_effect = modded_steal - previous_average[:STL_mean].to_f
      modded_steal_per_min = modded_steal_per_min / actual_mins
      modded_steal_effect_per_min = 0

      #Make buckets of these
      #pt_spread = team_average[:point_spread_mean].to_f
      pt_spread = team_average[:point_spread_mean].to_f
      point_spread_abs_3_or_less = (abs(pt_spread) <= 3) ? 1 : 0
      point_spread_abs_6_or_less = (abs(pt_spread) > 3 and abs(pt_spread) <= 6) ? 1 : 0
      point_spread_abs_9_or_less = (abs(pt_spread) > 6 and abs(pt_spread) <= 9) ? 1 : 0
      point_spread_abs_12_or_less = (abs(pt_spread) > 9 and abs(pt_spread) <= 12) ? 1 : 0
      point_spread_abs_over_9 = (abs(pt_spread) > 9) ? 1 : 0
      point_spread_abs_over_12 = (abs(pt_spread) > 12) ? 1 : 0

      point_spread_3_or_less = (pt_spread <= 3) ? 1 : 0
      point_spread_6_or_less = (pt_spread > 3 and pt_spread <= 6) ? 1 : 0
      point_spread_9_or_less = (pt_spread > 6 and pt_spread <= 9) ? 1 : 0
      point_spread_12_or_less = (pt_spread > 9 and pt_spread <= 12) ? 1 : 0
      point_spread_over_9 = (pt_spread > 9) ? 1 : 0
      point_spread_over_12 = (pt_spread > 12) ? 1 : 0

      point_spread_neg_3_or_less = (pt_spread >= -3) ? 1 : 0
      point_spread_neg_6_or_less = (pt_spread < -3 and pt_spread >= -6) ? 1 : 0
      point_spread_neg_9_or_less = (pt_spread < -6 and pt_spread >= -9) ? 1 : 0
      point_spread_neg_12_or_less = (pt_spread < -9 and pt_spread >= -12) ? 1 : 0
      point_spread_neg_over_9 = (pt_spread < -9) ? 1 : 0
      point_spread_neg_over_12 = (pt_spread < -12) ? 1 : 0

      #pinnacle_pt_spread = team_average[:point_spread_Pinnacle].to_f
      pinnacle_pt_spread = team_average[:point_spread_Pinnacle].to_f
      pn_point_spread_abs_3_or_less = (abs(pinnacle_pt_spread) <= 3) ? 1 : 0
      pn_point_spread_abs_6_or_less = (abs(pinnacle_pt_spread) > 3 and abs(pinnacle_pt_spread) <= 6) ? 1 : 0
      pn_point_spread_abs_9_or_less = (abs(pinnacle_pt_spread) > 6 and abs(pinnacle_pt_spread) <= 9) ? 1 : 0
      pn_point_spread_abs_12_or_less = (abs(pinnacle_pt_spread) > 9 and abs(pinnacle_pt_spread) <= 12) ? 1 : 0
      pn_point_spread_abs_over_9 = (abs(pinnacle_pt_spread) > 9) ? 1 : 0
      pn_point_spread_abs_over_12 = (abs(pinnacle_pt_spread) > 12) ? 1 : 0

      pn_point_spread_3_or_less = (pinnacle_pt_spread <= 3) ? 1 : 0
      pn_point_spread_6_or_less = (pinnacle_pt_spread > 3 and pinnacle_pt_spread <= 6) ? 1 : 0
      pn_point_spread_9_or_less = (pinnacle_pt_spread > 6 and pinnacle_pt_spread <= 9) ? 1 : 0
      pn_point_spread_12_or_less = (pinnacle_pt_spread > 9 and pinnacle_pt_spread <= 12) ? 1 : 0
      pn_point_spread_over_9 = (pinnacle_pt_spread > 9) ? 1 : 0
      pn_point_spread_over_12 = (pinnacle_pt_spread > 12) ? 1 : 0

      pn_point_spread_neg_3_or_less = (pinnacle_pt_spread >= -3) ? 1 : 0
      pn_point_spread_neg_6_or_less = (pinnacle_pt_spread < -3 and pinnacle_pt_spread >= -6) ? 1 : 0
      pn_point_spread_neg_9_or_less = (pinnacle_pt_spread < -6 and pinnacle_pt_spread >= -9) ? 1 : 0
      pn_point_spread_neg_12_or_less = (pinnacle_pt_spread < -9 and pinnacle_pt_spread >= -12) ? 1 : 0
      pn_point_spread_neg_over_9 = (pinnacle_pt_spread < -9) ? 1 : 0
      pn_point_spread_neg_over_12 = (pinnacle_pt_spread < -12) ? 1 : 0

      team_pts_ratio_pinnacle = vegas_average[:est_vegas_team_PTS_Pinnacle].to_f / team_average[:PTS_mean].to_f
      opp_pts_ratio_pinnacle = vegas_average[:est_vegas_opp_PTS_Pinnacle].to_f / opp_average[:PTS_mean].to_f
      over_under_ratio_pinnacle = vegas_average[:over_under_Pinnacle].to_f / ( team_average[:PTS_mean].to_f + opp_average[:PTS_mean].to_f )
      team_pts_ratio = vegas_average[:est_vegas_team_PTS].to_f / team_average[:PTS_mean].to_f
      opp_pts_ratio = vegas_average[:est_vegas_opp_PTS].to_f / opp_average[:PTS_mean].to_f

      vegas_ratio_pts = previous_average[:PTS_mean].to_f * team_pts_ratio
      vegas_ratio_oreb = previous_average[:OREB_mean].to_f * team_pts_ratio
      vegas_ratio_dreb = previous_average[:DREB_mean].to_f * team_pts_ratio
      vegas_ratio_ast = previous_average[:AST_mean].to_f * team_pts_ratio
      vegas_ratio_tov = previous_average[:TOV_mean].to_f * team_pts_ratio
      vegas_ratio_blk = previous_average[:BLK_mean].to_f * team_pts_ratio
      vegas_ratio_stl = previous_average[:STL_mean].to_f * team_pts_ratio

      vegas_ratio_pts_per_min = vegas_ratio_pts / average_seconds
      vegas_ratio_oreb_per_min = vegas_ratio_oreb / average_seconds
      vegas_ratio_dreb_per_min = vegas_ratio_dreb / average_seconds
      vegas_ratio_ast_per_min = vegas_ratio_ast / average_seconds
      vegas_ratio_tov_per_min = vegas_ratio_tov / average_seconds
      vegas_ratio_blk_per_min = vegas_ratio_blk / average_seconds
      vegas_ratio_stl_per_min = vegas_ratio_stl / average_seconds

      vegas_ratio_pts_pinnacle = previous_average[:PTS_mean].to_f * team_pts_ratio_pinnacle
      vegas_ratio_oreb_pinnacle = previous_average[:OREB_mean].to_f * team_pts_ratio_pinnacle
      vegas_ratio_dreb_pinnacle = previous_average[:DREB_mean].to_f * team_pts_ratio_pinnacle
      vegas_ratio_ast_pinnacle = previous_average[:AST_mean].to_f * team_pts_ratio_pinnacle
      vegas_ratio_tov_pinnacle = previous_average[:TOV_mean].to_f * team_pts_ratio_pinnacle
      vegas_ratio_blk_pinnacle = previous_average[:BLK_mean].to_f * team_pts_ratio_pinnacle
      vegas_ratio_stl_pinnacle = previous_average[:STL_mean].to_f * team_pts_ratio_pinnacle

      vegas_ratio_pts_pinnacle_per_min = vegas_ratio_pts_pinnacle / average_seconds
      vegas_ratio_oreb_pinnacle_per_min = vegas_ratio_oreb_pinnacle / average_seconds
      vegas_ratio_dreb_pinnacle_per_min = vegas_ratio_dreb_pinnacle / average_seconds
      vegas_ratio_ast_pinnacle_per_min = vegas_ratio_ast_pinnacle / average_seconds
      vegas_ratio_tov_pinnacle_per_min = vegas_ratio_tov_pinnacle / average_seconds
      vegas_ratio_blk_pinnacle_per_min = vegas_ratio_blk_pinnacle / average_seconds
      vegas_ratio_stl_pinnacle_per_min = vegas_ratio_stl_pinnacle / average_seconds

      vegas_ratio_pts_opp_pinnacle = previous_average[:PTS_mean].to_f * opp_pts_ratio_pinnacle
      vegas_ratio_oreb_opp_pinnacle = previous_average[:OREB_mean].to_f * opp_pts_ratio_pinnacle
      vegas_ratio_dreb_opp_pinnacle = previous_average[:DREB_mean].to_f * opp_pts_ratio_pinnacle
      vegas_ratio_ast_opp_pinnacle = previous_average[:AST_mean].to_f * opp_pts_ratio_pinnacle
      vegas_ratio_tov_opp_pinnacle = previous_average[:TOV_mean].to_f * opp_pts_ratio_pinnacle
      vegas_ratio_blk_opp_pinnacle = previous_average[:BLK_mean].to_f * opp_pts_ratio_pinnacle
      vegas_ratio_stl_opp_pinnacle = previous_average[:STL_mean].to_f * opp_pts_ratio_pinnacle

      vegas_ratio_pts_opp_pinnacle_per_min = vegas_ratio_pts_opp_pinnacle / average_seconds
      vegas_ratio_oreb_opp_pinnacle_per_min = vegas_ratio_oreb_opp_pinnacle / average_seconds
      vegas_ratio_dreb_opp_pinnacle_per_min = vegas_ratio_dreb_opp_pinnacle / average_seconds
      vegas_ratio_ast_opp_pinnacle_per_min = vegas_ratio_ast_opp_pinnacle / average_seconds
      vegas_ratio_tov_opp_pinnacle_per_min = vegas_ratio_tov_opp_pinnacle / average_seconds
      vegas_ratio_blk_opp_pinnacle_per_min = vegas_ratio_blk_opp_pinnacle / average_seconds
      vegas_ratio_stl_opp_pinnacle_per_min = vegas_ratio_stl_opp_pinnacle / average_seconds

      vegas_ratio_pts_ou_pinnacle = previous_average[:PTS_mean].to_f * over_under_ratio_pinnacle
      vegas_ratio_oreb_ou_pinnacle = previous_average[:OREB_mean].to_f * over_under_ratio_pinnacle
      vegas_ratio_dreb_ou_pinnacle = previous_average[:DREB_mean].to_f * over_under_ratio_pinnacle
      vegas_ratio_ast_ou_pinnacle = previous_average[:AST_mean].to_f * over_under_ratio_pinnacle
      vegas_ratio_tov_ou_pinnacle = previous_average[:TOV_mean].to_f * over_under_ratio_pinnacle
      vegas_ratio_blk_ou_pinnacle = previous_average[:BLK_mean].to_f * over_under_ratio_pinnacle
      vegas_ratio_stl_ou_pinnacle = previous_average[:STL_mean].to_f * over_under_ratio_pinnacle

      vegas_ratio_pts_ou_pinnacle_per_min = vegas_ratio_pts_ou_pinnacle / average_seconds
      vegas_ratio_oreb_ou_pinnacle_per_min = vegas_ratio_oreb_ou_pinnacle / average_seconds
      vegas_ratio_dreb_ou_pinnacle_per_min = vegas_ratio_dreb_ou_pinnacle / average_seconds
      vegas_ratio_ast_ou_pinnacle_per_min = vegas_ratio_ast_ou_pinnacle / average_seconds
      vegas_ratio_tov_ou_pinnacle_per_min = vegas_ratio_tov_ou_pinnacle / average_seconds
      vegas_ratio_blk_ou_pinnacle_per_min = vegas_ratio_blk_ou_pinnacle / average_seconds
      vegas_ratio_stl_ou_pinnacle_per_min = vegas_ratio_stl_ou_pinnacle / average_seconds

      vegas_ratio_pts_effect = vegas_ratio_pts - previous_average[:PTS_mean].to_f
      vegas_ratio_oreb_effect = vegas_ratio_oreb - previous_average[:OREB_mean].to_f
      vegas_ratio_dreb_effect = vegas_ratio_dreb - previous_average[:DREB_mean].to_f
      vegas_ratio_ast_effect = vegas_ratio_ast - previous_average[:AST_mean].to_f
      vegas_ratio_tov_effect = vegas_ratio_tov - previous_average[:TOV_mean].to_f
      vegas_ratio_blocks_effect = vegas_ratio_blk - previous_average[:BLK_mean].to_f
      vegas_ratio_stl_effect = vegas_ratio_stl - previous_average[:STL_mean].to_f

      vegas_ratio_pts_effect_per_min = vegas_ratio_pts_effect / average_seconds
      vegas_ratio_oreb_effect_per_min = vegas_ratio_oreb_effect / average_seconds
      vegas_ratio_dreb_effect_per_min = vegas_ratio_dreb_effect / average_seconds
      vegas_ratio_ast_effect_per_min = vegas_ratio_ast_effect / average_seconds
      vegas_ratio_tov_effect_per_min = vegas_ratio_tov_effect / average_seconds
      vegas_ratio_blk_effect_per_min = vegas_ratio_blk_effect / average_seconds
      vegas_ratio_stl_effect_per_min = vegas_ratio_stl_effect / average_seconds

      vegas_ratio_pts_pinnacle_effect = vegas_ratio_pts_pinnacle - previous_average[:PTS_mean].to_f
      vegas_ratio_oreb_pinnacle_effect = vegas_ratio_oreb_pinnacle - previous_average[:OREB_mean].to_f
      vegas_ratio_dreb_pinnacle_effect = vegas_ratio_dreb_pinnacle - previous_average[:DREB_mean].to_f
      vegas_ratio_ast_pinnacle_effect = vegas_ratio_ast_pinnacle - previous_average[:AST_mean].to_f
      vegas_ratio_tov_pinnacle_effect = vegas_ratio_tov_pinnacle - previous_average[:TOV_mean].to_f
      vegas_ratio_blk_pinnacle_effect = vegas_ratio_blk_pinnacle - previous_average[:BLK_mean].to_f
      vegas_ratio_stl_pinnacle_effect = vegas_ratio_stl_pinnacle - previous_average[:STL_mean].to_f

      vegas_ratio_pts_pinnacle_effect_per_min = vegas_ratio_pts_pinnacle_effect / average_seconds
      vegas_ratio_oreb_pinnacle_effect_per_min = vegas_ratio_oreb_pinnacle_effect / average_seconds
      vegas_ratio_dreb_pinnacle_effect_per_min = vegas_ratio_dreb_pinnacle_effect / average_seconds
      vegas_ratio_ast_pinnacle_effect_per_min = vegas_ratio_ast_pinnacle_effect / average_seconds
      vegas_ratio_tov_pinnacle_effect_per_min = vegas_ratio_tov_pinnacle_effect / average_seconds
      vegas_ratio_blk_pinnacle_effect_per_min = vegas_ratio_blk_pinnacle_effect / average_seconds
      vegas_ratio_stl_pinnacle_effect_per_min = vegas_ratio_stl_pinnacle_effect / average_seconds

      vegas_ratio_pts_opp_pinnacle_effect = vegas_ratio_pts_opp_pinnacle - previous_average[:PTS_mean].to_f
      vegas_ratio_oreb_opp_pinnacle_effect = vegas_ratio_oreb_opp_pinnacle - previous_average[:OREB_mean].to_f
      vegas_ratio_dreb_opp_pinnacle_effect = vegas_ratio_dreb_opp_pinnacle - previous_average[:DREB_mean].to_f
      vegas_ratio_ast_opp_pinnacle_effect = vegas_ratio_ast_opp_pinnacle - previous_average[:AST_mean].to_f
      vegas_ratio_tov_opp_pinnacle_effect = vegas_ratio_tov_opp_pinnacle - previous_average[:TOV_mean].to_f
      vegas_ratio_blk_opp_pinnacle_effect = vegas_ratio_blk_opp_pinnacle - previous_average[:BLK_mean].to_f
      vegas_ratio_stl_opp_pinnacle_effect = vegas_ratio_stl_opp_pinnacle - previous_average[:STL_mean].to_f

      vegas_ratio_pts_opp_pinnacle_effect_per_min = vegas_ratio_pts_opp_pinnacle_effect / average_seconds
      vegas_ratio_oreb_opp_pinnacle_effect_per_min = vegas_ratio_oreb_opp_pinnacle_effect / average_seconds
      vegas_ratio_dreb_opp_pinnacle_effect_per_min = vegas_ratio_dreb_opp_pinnacle_effect / average_seconds
      vegas_ratio_ast_opp_pinnacle_effect_per_min = vegas_ratio_ast_opp_pinnacle_effect / average_seconds
      vegas_ratio_tov_opp_pinnacle_effect_per_min = vegas_ratio_tov_opp_pinnacle_effect / average_seconds
      vegas_ratio_blk_opp_pinnacle_effect_per_min = vegas_ratio_blk_opp_pinnacle_effect / average_seconds
      vegas_ratio_stl_opp_pinnacle_effect_per_min = vegas_ratio_stl_opp_pinnacle_effect / average_seconds

      vegas_ratio_pts_ou_pinnacle_effect = vegas_ratio_pts_ou_pinnacle - previous_average[:PTS_mean].to_f
      vegas_ratio_oreb_ou_pinnacle_effect = vegas_ratio_oreb_ou_pinnacle - previous_average[:OREB_mean].to_f
      vegas_ratio_dreb_ou_pinnacle_effect = vegas_ratio_dreb_ou_pinnacle - previous_average[:DREB_mean].to_f
      vegas_ratio_ast_ou_pinnacle_effect = vegas_ratio_ast_ou_pinnacle - previous_average[:AST_mean].to_f
      vegas_ratio_tov_ou_pinnacle_effect = vegas_ratio_tov_ou_pinnacle - previous_average[:TOV_mean].to_f
      vegas_ratio_blk_ou_pinnacle_effect = vegas_ratio_blk_ou_pinnacle - previous_average[:BLK_mean].to_f
      vegas_ratio_stl_ou_pinnacle_effect = vegas_ratio_stl_ou_pinnacle - previous_average[:STL_mean].to_f

      vegas_ratio_pts_ou_pinnacle_effect_per_min = vegas_ratio_pts_ou_pinnacle_effect / average_seconds
      vegas_ratio_oreb_ou_pinnacle_effect_per_min = vegas_ratio_oreb_ou_pinnacle_effect / average_seconds
      vegas_ratio_dreb_ou_pinnacle_effect_per_min = vegas_ratio_dreb_ou_pinnacle_effect / average_seconds
      vegas_ratio_ast_ou_pinnacle_effect_per_min = vegas_ratio_ast_ou_pinnacle_effect / average_seconds
      vegas_ratio_tov_ou_pinnacle_effect_per_min = vegas_ratio_tov_ou_pinnacle_effect / average_seconds
      vegas_ratio_blk_ou_pinnacle_effect_per_min = vegas_ratio_blk_ou_pinnacle_effect / average_seconds
      vegas_ratio_stl_ou_pinnacle_effect_per_min = vegas_ratio_stl_ou_pinnacle_effect / average_seconds

      #buckets for minutes
      avg_mins = average_seconds / 60
      avg_mins_10_or_less = (avg_mins <= 10) ? 1 : 0
      avg_mins_20_or_less = (avg_mins > 10 and avg_mins <= 20) ? 1 : 0
      avg_mins_30_or_less = (avg_mins > 20 and avg_mins <= 30) ? 1 : 0
      avg_mins_over_30 = (avg_mins > 30) ? 1 : 0

      merged_rating = (previous_average[:OFF_RATING].to_f + previous_average[:o_DEF_RATING].to_f) / 2

      features_points = [ previous_average[:PTS_mean].to_f, def_rtg_delta, def_rtg_v_position_delta, o_pts_delta, b2b, opp_b2b, extra_rest, opp_extra_rest, location, location_pts_effect, rest_effect, pts_paint_effect, pts_off_tov_effect, fb_effect, pts_2ndchance_effect, previous_average[:USG_PCT]*100, previous_average[:USG_PCT_minus_TOV]*100, location_pts, rest_pts, opp_rest_pts, expected_PTS_pace, pts_pace_effect, expected_PTS_pace2, pts_pace2_effect, expected_PTS_pace3, pts_pace3_effect, expected_PTS_def_rtg, def_rtg_effect, expected_PTS_def_rtg_v_position, def_rtg_v_position_effect, expected_PTS_off_rtg, off_rtg_PTS_effect, expected_PTS_opp_PTS, expected_PTS_opp_PTS_effect, mean_starter_pts, mean_bench_pts, starterbench_pts_effect, starter, mean_starterbench_pts, prev_pts, prev_pts_delta, prev2_pts, prev2_pts_delta, prev5_pts, prev5_pts_delta, ft_effect, expected_FTM, vegas_ratio_pts, vegas_ratio_pts_effect, vegas_ratio_pts_pinnacle, vegas_ratio_pts_pinnacle_effect, vegas_ratio_pts_opp_pinnacle, vegas_ratio_pts_opp_pinnacle_effect, vegas_ratio_pts_ou_pinnacle, vegas_ratio_pts_ou_pinnacle_effect, point_spread_abs_3_or_less, point_spread_abs_6_or_less, adjusted_cfg_pts, adjusted_ufg_pts, adjusted_fg_pts, cfg_effect, ft_effect, expected_FTM, point_spread_abs_9_or_less, point_spread_abs_12_or_less, point_spread_abs_over_9, point_spread_abs_over_12, point_spread_3_or_less, point_spread_6_or_less, point_spread_9_or_less, point_spread_12_or_less, point_spread_over_9, point_spread_over_12, point_spread_neg_3_or_less, point_spread_neg_6_or_less, point_spread_neg_9_or_less, point_spread_neg_12_or_less, point_spread_neg_over_9, point_spread_neg_over_12, average_seconds/60, mean_starter_seconds/60, mean_bench_seconds/60, mean_starterbench_seconds/60, prev_seconds/60, prev2_seconds/60, prev5_seconds/60, boxscore[:PTS].to_i ].to_csv
      features_points_per_min = [ mean_pts_per_min, def_rtg_delta, def_rtg_v_position_delta, o_pts_delta_per_min, b2b, opp_b2b, extra_rest, opp_extra_rest, location, location_pts_effect_per_min, rest_effect_per_min, pace_effect_per_min, pts_paint_effect_per_min,pts_off_tov_effect_per_min, fb_effect_per_min, pts_2ndchance_effect_per_min, previous_average[:USG_PCT]*100, previous_average[:USG_PCT_minus_TOV]*100, location_pts_per_min, rest_pts_per_min, opp_rest_pts_per_min, expected_PTS_pace_per_min, pts_pace_effect_per_min, expected_PTS_pace2_per_min, pts_pace2_effect_per_min, expected_PTS_pace3_per_min, pts_pace3_effect_per_min, expected_PTS_def_rtg_per_min, def_rtg_effect_per_min, expected_PTS_def_rtg_v_position_per_min, def_rtg_v_position_effect_per_min, expected_PTS_def_rtg_v_position_per_min, expected_PTS_off_rtg_per_min, off_rtg_PTS_effect_per_min, expected_PTS_opp_PTS_per_min, expected_PTS_opp_PTS_effect_per_min, mean_starter_pts_per_min, mean_bench_pts_per_min, starterbench_pts_effect_per_min, prev_pts_per_min, prev_pts_delta_per_min, prev2_pts_per_min, prev2_pts_delta_per_min, prev5_pts_per_min, prev5_pts_delta_per_min, ft_effect_per_min, expected_FTM_per_min, vegas_ratio_pts_per_min, vegas_ratio_pts_effect_per_min, vegas_ratio_pts_pinnacle_per_min, vegas_ratio_pts_pinnacle_effect_per_min, vegas_ratio_pts_opp_pinnacle_per_min, vegas_ratio_pts_opp_pinnacle_effect_per_min, vegas_ratio_pts_ou_pinnacle_per_min, vegas_ratio_pts_ou_pinnacle_effect_per_min, adjusted_cfg_pts_per_min, adjusted_ufg_pts_per_min, adjusted_fg_pts_per_min, cfg_effect_per_min, point_spread_abs_3_or_less, point_spread_abs_6_or_less, point_spread_abs_9_or_less, point_spread_abs_12_or_less, point_spread_abs_over_9, point_spread_abs_over_12, point_spread_3_or_less, point_spread_6_or_less, point_spread_9_or_less, point_spread_12_or_less, point_spread_over_9, point_spread_over_12, point_spread_neg_3_or_less, point_spread_neg_6_or_less, point_spread_neg_9_or_less, point_spread_neg_12_or_less, point_spread_neg_over_9, point_spread_neg_over_12, average_seconds, rest_effect_seconds, location_pts_effect_seconds, prev_seconds, prev2_seconds, prev5_seconds, starter, mean_starter_seconds, mean_bench_seconds, mean_starterbench_seconds, actual_mins, actual_pts_per_min ].to_csv

      #team oreb_pct
      #mean_starter_oreb,mean_bench_oreb,mean_starterbench_oreb,prev_oreb,prev2_oreb,prev5_oreb,rest_oreb,rest_oreb_effect,opp_rest_oreb,opp_rest_oreb_effect,location_oreb,location_oreb_effect,expected_OREB,expected_OREB_effect,scaled_oreb_pct,scaled_oreb_pct_effect,scaled_oreb,scaled_oreb_pct_effect,modded_oreb,modded_oreb_effect
      
      features_orebs = [ previous_average[:OREB_mean].to_f, previous_average[:OREB_PCT].to_f, opp_average[:DREB_PCT].to_f / 100.0, opp_average[:DREB_mean].to_f,o_oreb_delta, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_oreb_effect,rest_oreb_effect,location_oreb,rest_oreb,opp_rest_oreb,starter,mean_starter_oreb,mean_bench_oreb,starterbench_oreb_effect,mean_starterbench_oreb,prev_oreb,prev_oreb_delta, prev2_oreb, prev2_oreb_delta, prev5_oreb, prev5_oreb_delta, o_oreb_pct_delta,expected_OREB,expected_OREB_effect,scaled_oreb_pct,scaled_oreb_pct_effect, scaled_oreb, scaled_oreb_effect, modded_oreb, modded_oreb_effect,opp_average_v_position[:OREB_mean].to_f, opp_average_v_position[:OREB_PCT].to_f, opp_average[:OREB_PCT].to_f / 100.0,average_seconds/60, mean_starter_seconds/60, mean_bench_seconds/60, mean_starterbench_seconds/60, prev_seconds/60, prev2_seconds/60, prev5_seconds/60, team_misses, team_3p_misses, team_2p_misses, team_ft_misses, opp_average[:FT_PCT].to_f,opp_average[:FG_PCT].to_f, opp_average[:FG3_PCT].to_f, opp_average[:FG2_PCT].to_f, expected_OREB_pace, oreb_pace_effect, expected_OREB_pace2, oreb_pace2_effect, expected_OREB_pace3, oreb_pace3_effect, expected_OREB_def_rtg, def_rtg_OREB_effect, expected_OREB_off_rtg, off_rtg_OREB_effect, expected_OREB_opp_OREB, expected_OREB_opp_OREB_effect, vegas_ratio_oreb, vegas_ratio_oreb_effect, vegas_ratio_oreb_pinnacle, vegas_ratio_oreb_pinnacle_effect, vegas_ratio_oreb_opp_pinnacle, vegas_ratio_oreb_opp_pinnacle_effect, vegas_ratio_oreb_ou_pinnacle, vegas_ratio_oreb_ou_pinnacle_effect, boxscore[:OREB].to_i ].to_csv

      opp_average_oreb_per_min = divide(60*opp_average[:OREB_mean].to_f,average_seconds)
      opp_average_dreb_per_min = divide(60*opp_average[:DREB_mean].to_f,average_seconds)

      features_orebs_per_min = [ mean_oreb_per_min, previous_average[:OREB_PCT].to_f, opp_average[:DREB_PCT].to_f / 100.0, opp_average_dreb_per_min,o_oreb_delta_per_min, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_oreb_effect_per_min,rest_oreb_effect_per_min,location_oreb_per_min,rest_oreb_per_min,opp_rest_oreb_per_min,starter,mean_starter_oreb_per_min,mean_bench_oreb_per_min,starterbench_oreb_effect_per_min,mean_starterbench_oreb_per_min,prev_oreb_per_min,prev_oreb_delta_per_min, prev2_oreb_per_min, prev2_oreb_delta_per_min, prev5_oreb_per_min, prev5_oreb_delta_per_min, o_oreb_pct_delta,expected_OREB_per_min,expected_OREB_effect_per_min,scaled_oreb_pct,scaled_oreb_pct_effect, scaled_oreb_per_min, scaled_oreb_effect_per_min, modded_oreb_per_min, modded_oreb_effect_per_min,opp_average_oreb_per_min, opp_average_v_position[:OREB_PCT].to_f, opp_average[:OREB_PCT].to_f / 100.0,average_seconds/60, mean_starter_seconds/60, mean_bench_seconds/60, mean_starterbench_seconds/60, prev_seconds/60, prev2_seconds/60, prev5_seconds/60, team_misses_per_min, team_3p_misses_per_min, team_2p_misses_per_min, team_ft_misses_per_min, opp_average[:FT_PCT].to_f,opp_average[:FG_PCT].to_f, opp_average[:FG3_PCT].to_f, opp_average[:FG2_PCT].to_f, expected_OREB_pace_per_min, oreb_pace_effect_per_min, expected_OREB_pace2_per_min, oreb_pace2_effect_per_min, expected_OREB_pace3_per_min, oreb_pace3_effect_per_min, expected_OREB_def_rtg_per_min, def_rtg_OREB_effect_per_min, expected_OREB_off_rtg_per_min, off_rtg_OREB_effect_per_min, expected_OREB_opp_OREB_per_min, expected_OREB_opp_OREB_effect_per_min, vegas_ratio_oreb_per_min, vegas_ratio_oreb_effect_per_min, vegas_ratio_oreb_pinnacle_per_min, vegas_ratio_oreb_pinnacle_effect_per_min, vegas_ratio_oreb_opp_pinnacle_per_min, vegas_ratio_oreb_opp_pinnacle_effect_per_min, vegas_ratio_oreb_ou_pinnacle_per_min, vegas_ratio_oreb_ou_pinnacle_effect_per_min, actual_oreb_per_min ].to_csv

      features_drebs = [ previous_average[:DREB_mean].to_f, previous_average[:DREB_PCT].to_f,oa_OREB_PCT,opp_average[:OREB_mean].to_f,o_dreb_delta, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_dreb_effect,rest_dreb_effect,location_dreb,rest_dreb,opp_rest_dreb,starter,mean_starter_dreb,mean_bench_dreb,starterbench_dreb_effect,mean_starterbench_dreb,prev_dreb,prev_dreb_delta,prev2_dreb,prev2_dreb_delta,prev5_dreb,prev5_dreb_delta,o_dreb_pct_delta,expected_DREB,expected_DREB_effect,scaled_dreb_pct_effect, scaled_dreb_effect,modded_dreb, modded_dreb_effect,opp_average_v_position[:DREB_mean].to_f,opp_average_v_position[:DREB_PCT].to_f,oa_DREB_PCT,average_seconds/60, mean_starter_seconds/60, mean_bench_seconds/60, mean_starterbench_seconds/60, prev_seconds/60, prev2_seconds/60, prev5_seconds/60, oa_misses,oa_3p_misses,oa_2p_misses,oa_ft_misses,opp_average[:FT_PCT].to_f,opp_average[:FG_PCT].to_f,opp_average[:FG3_PCT].to_f,opp_average[:FG2_PCT].to_f,expected_DREB_pace, dreb_pace_effect, expected_DREB_pace2, dreb_pace2_effect, expected_DREB_pace3, dreb_pace3_effect, expected_DREB_def_rtg, def_rtg_DREB_effect, expected_DREB_off_rtg, off_rtg_DREB_effect, expected_DREB_opp_DREB, expected_DREB_opp_DREB_effect, vegas_ratio_dreb, vegas_ratio_dreb_effect, vegas_ratio_dreb_pinnacle, vegas_ratio_dreb_pinnacle_effect, vegas_ratio_dreb_opp_pinnacle, vegas_ratio_dreb_opp_pinnacle_effect, vegas_ratio_dreb_ou_pinnacle, vegas_ratio_dreb_ou_pinnacle_effect, boxscore[:DREB].to_i ].to_csv

      features_drebs_per_min = [ mean_dreb_per_min, previous_average[:DREB_PCT].to_f, opp_average[:OREB_PCT].to_f / 100.0, opp_average_oreb_per_min,o_dreb_delta_per_min, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_dreb_effect_per_min,rest_dreb_effect_per_min,location_dreb_per_min,rest_dreb_per_min,opp_rest_dreb_per_min,starter,mean_starter_dreb_per_min,mean_bench_dreb_per_min,starterbench_dreb_effect_per_min,mean_starterbench_dreb_per_min,prev_dreb_per_min,prev_dreb_delta_per_min, prev2_dreb_per_min, prev2_dreb_delta_per_min, prev5_dreb_per_min, prev5_dreb_delta_per_min, o_dreb_pct_delta,expected_dreb_per_min,expected_dreb_effect_per_min,scaled_dreb_pct,scaled_dreb_pct_effect, scaled_dreb_per_min, scaled_dreb_effect_per_min, modded_dreb_per_min, modded_dreb_effect_per_min,opp_average_dreb_per_min, opp_average_v_position[:DREB_PCT].to_f, opp_average[:DREB_PCT].to_f / 100.0,average_seconds/60, mean_starter_seconds/60, mean_bench_seconds/60, mean_starterbench_seconds/60, prev_seconds/60, prev2_seconds/60, prev5_seconds/60, team_misses_per_min, team_3p_misses_per_min, team_2p_misses_per_min, team_ft_misses_per_min, opp_average[:FT_PCT].to_f,opp_average[:FG_PCT].to_f, opp_average[:FG3_PCT].to_f, opp_average[:FG2_PCT].to_f, expected_DREB_pace_per_min, dreb_pace_effect_per_min, expected_DREB_pace2_per_min, dreb_pace2_effect_per_min, expected_DREB_pace3_per_min, dreb_pace3_effect_per_min, expected_DREB_def_rtg_per_min, def_rtg_DREB_effect_per_min, expected_DREB_off_rtg_per_min, off_rtg_DREB_effect_per_min, expected_DREB_opp_DREB_per_min, expected_DREB_opp_DREB_effect_per_min, vegas_ratio_dreb_per_min, vegas_ratio_dreb_effect_per_min, vegas_ratio_dreb_pinnacle_per_min, vegas_ratio_dreb_pinnacle_effect_per_min, vegas_ratio_dreb_opp_pinnacle_per_min, vegas_ratio_dreb_opp_pinnacle_effect_per_min, vegas_ratio_dreb_ou_pinnacle_per_min, vegas_ratio_dreb_ou_pinnacle_effect_per_min, actual_dreb_per_min ].to_csv

      features_assists = [ previous_average[:AST_mean].to_f, previous_average[:AST_PCT].to_f,team_average[:o_AST_PCT].to_f,o_ast_delta, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_ast,location_ast_effect,rest_ast,rest_ast_effect,opp_rest_ast,opp_rest_ast_effect,starter,mean_starter_ast,mean_bench_ast,starterbench_ast_effect,mean_starterbench_ast,prev_ast,prev_ast_delta,prev2_ast,prev2_ast_delta,prev5_ast,prev5_ast_delta,o_ast_pct_delta,expected_AST,expected_AST_effect,scaled_assist_pct_effect, scaled_assist,scaled_assist_effect,modded_assist,modded_assist_effect,expected_AST_pace, ast_pace_effect, expected_AST_pace2, ast_pace2_effect, expected_AST_pace3, ast_pace3_effect, expected_AST_def_rtg, def_rtg_AST_effect, expected_AST_off_rtg, off_rtg_AST_effect, expected_AST_opp_AST, expected_AST_opp_AST_effect, vegas_ratio_ast, vegas_ratio_ast_effect, vegas_ratio_ast_pinnacle, vegas_ratio_ast_pinnacle_effect, vegas_ratio_ast_opp_pinnacle, vegas_ratio_ast_opp_pinnacle_effect, vegas_ratio_ast_ou_pinnacle, vegas_ratio_ast_ou_pinnacle_effect, boxscore[:AST].to_i ].to_csv

      features_assists_per_min = [ mean_ast_per_min, previous_average[:AST_PCT].to_f,team_average[:o_AST_PCT].to_f,o_ast_delta_per_min, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_ast_per_min,location_ast_effect_per_min,rest_ast_per_min,rest_ast_effect_per_min,opp_rest_ast_per_min,opp_rest_ast_effect_per_min,starter,mean_starter_ast_per_min,mean_bench_ast_per_min,starterbench_ast_effect_per_min,mean_starterbench_ast_per_min,prev_ast_per_min,prev_ast_delta_per_min,prev2_ast_per_min,prev2_ast_delta_per_min,prev5_ast_per_min,prev5_ast_delta_per_min,o_ast_pct_delta,expected_AST_per_min,expected_AST_effect_per_min,scaled_assist_pct_effect, scaled_assist_per_min,scaled_assist_effect_per_min,modded_assist_per_min,modded_assist_effect_per_min,expected_AST_pace_per_min, ast_pace_effect_per_min, expected_AST_pace2_per_min, ast_pace2_effect_per_min, expected_AST_pace3_per_min, ast_pace3_effect_per_min, expected_AST_def_rtg_per_min, def_rtg_AST_effect_per_min, expected_AST_off_rtg_per_min, off_rtg_AST_effect_per_min, expected_AST_opp_AST_per_min, expected_AST_opp_AST_effect_per_min, vegas_ratio_ast_per_min, vegas_ratio_ast_effect_per_min, vegas_ratio_ast_pinnacle_per_min, vegas_ratio_ast_pinnacle_effect_per_min, vegas_ratio_ast_opp_pinnacle_per_min, vegas_ratio_ast_opp_pinnacle_effect_per_min, vegas_ratio_ast_ou_pinnacle_per_min, vegas_ratio_ast_ou_pinnacle_effect_per_min, actual_ast_per_min ].to_csv

      features_turnovers = [ previous_average[:TOV_mean].to_f, previous_average[:TO_PCT].to_f,team_average[:o_TO_PCT].to_f,o_tov_delta, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_tov_effect,rest_tov_effect,location_tov,rest_tov,opp_rest_tov,opp_rest_tov_effect,starter,mean_starter_tov,mean_bench_tov,starterbench_tov_effect,mean_starterbench_tov,prev_tov,prev_tov_delta,prev2_tov,prev2_tov_delta,prev5_tov,prev5_tov_delta,o_tov_pct_delta,expected_TOV,expected_TOV_effect,scaled_turnover,scaled_turnover_pct_effect, scaled_turnover_effect,modded_turnover,modded_turnover_effect,expected_TOV_pace, tov_pace_effect, expected_TOV_pace2, tov_pace2_effect, expected_TOV_pace3, tov_pace3_effect, expected_TOV_def_rtg, def_rtg_TOV_effect, expected_TOV_off_rtg, off_rtg_TOV_effect, expected_TOV_opp_TOV, expected_TOV_opp_TOV_effect, vegas_ratio_tov, vegas_ratio_tov_effect, vegas_ratio_tov_pinnacle, vegas_ratio_tov_pinnacle_effect, vegas_ratio_tov_opp_pinnacle, vegas_ratio_tov_opp_pinnacle_effect, vegas_ratio_tov_ou_pinnacle, vegas_ratio_tov_ou_pinnacle_effect, boxscore[:TOV].to_i ].to_csv

      features_turnovers_per_min = [ mean_tov_per_min, previous_average[:TO_PCT].to_f,team_average[:o_TO_PCT].to_f,o_tov_delta_per_min, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_tov_effect_per_min,rest_tov_effect_per_min,location_tov_per_min,rest_tov_per_min,opp_rest_tov_per_min,opp_rest_tov_effect_per_min,starter,mean_starter_tov_per_min,mean_bench_tov_per_min,starterbench_tov_effect_per_min,mean_starterbench_tov_per_min,prev_tov_per_min,prev_tov_delta_per_min,prev2_tov_per_min,prev2_tov_delta_per_min,prev5_tov_per_min,prev5_tov_delta_per_min,o_tov_pct_delta,expected_TOV_per_min,expected_TOV_effect_per_min,scaled_turnover_per_min,scaled_turnover_pct_effect, scaled_turnover_effect_per_min,modded_turnover_per_min,modded_turnover_effect_per_min,expected_TOV_pace_per_min, tov_pace_effect_per_min, expected_TOV_pace2_per_min, tov_pace2_effect_per_min, expected_TOV_pace3_per_min, tov_pace3_effect_per_min, expected_TOV_def_rtg_per_min, def_rtg_TOV_effect_per_min, expected_TOV_off_rtg_per_min, off_rtg_TOV_effect_per_min, expected_TOV_opp_TOV_per_min, expected_TOV_opp_TOV_effect_per_min, vegas_ratio_tov_per_min, vegas_ratio_tov_effect_per_min, vegas_ratio_tov_pinnacle_per_min, vegas_ratio_tov_pinnacle_effect_per_min, vegas_ratio_tov_opp_pinnacle_per_min, vegas_ratio_tov_opp_pinnacle_effect_per_min, vegas_ratio_tov_ou_pinnacle_per_min, vegas_ratio_tov_ou_pinnacle_effect_per_min, actual_tov_per_min ].to_csv

      features_blocks = [ previous_average[:BLK_mean].to_f, previous_average[:PCT_BLK].to_f,team_average[:o_PCT_BLK].to_f,o_blk_delta, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_blk_effect,rest_blk_effect,location_blk,rest_blk,opp_rest_blk,opp_rest_blk_effect,starter,mean_starter_blk,mean_bench_blk,starterbench_blk_effect,mean_starterbench_blk,prev_blk,prev_blk_delta,prev2_blk,prev2_blk_delta,prev5_blk,prev5_blk_delta,o_blk_pct_delta,expected_BLK,expected_BLK_effect,scaled_block_pct_effect, scaled_block,scaled_block_effect,modded_block,modded_block_effect,expected_BLK_pace, blk_pace_effect, expected_BLK_pace2, blk_pace2_effect, expected_BLK_pace3, blk_pace3_effect, expected_BLK_def_rtg, def_rtg_BLK_effect, expected_BLK_off_rtg, off_rtg_BLK_effect, expected_BLK_opp_BLK, expected_BLK_opp_BLK_effect, vegas_ratio_blk, vegas_ratio_blk_effect, vegas_ratio_blk_pinnacle, vegas_ratio_blk_pinnacle_effect, vegas_ratio_blk_opp_pinnacle, vegas_ratio_blk_opp_pinnacle_effect, vegas_ratio_blk_ou_pinnacle, vegas_ratio_blk_ou_pinnacle_effect, boxscore[:BLK].to_i ].to_csv

      features_blocks_per_min = [ mean_blk_per_min, previous_average[:PCT_BLK].to_f,team_average[:o_PCT_BLK].to_f,o_blk_delta_per_min, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_blk_effect_per_min,rest_blk_effect_per_min,location_blk_per_min,rest_blk_per_min,opp_rest_blk_per_min,opp_rest_blk_effect_per_min,starter,mean_starter_blk_per_min,mean_bench_blk_per_min,starterbench_blk_effect_per_min,mean_starterbench_blk_per_min,prev_blk_per_min,prev_blk_delta_per_min,prev2_blk_per_min,prev2_blk_delta_per_min,prev5_blk_per_min,prev5_blk_delta_per_min,o_blk_pct_delta,expected_BLK_per_min,expected_BLK_effect_per_min,scaled_block_pct_effect_per_min, scaled_block_per_min,scaled_block_effect_per_min,modded_block_per_min,modded_block_effect_per_min,expected_BLK_pace_per_min, blk_pace_effect_per_min, expected_BLK_pace2_per_min, blk_pace2_effect_per_min, expected_BLK_pace3_per_min, blk_pace3_effect_per_min, expected_BLK_def_rtg_per_min, def_rtg_BLK_effect_per_min, expected_BLK_off_rtg_per_min, off_rtg_BLK_effect_per_min, expected_BLK_opp_BLK_per_min, expected_BLK_opp_BLK_effect_per_min, vegas_ratio_blk_per_min, vegas_ratio_blk_effect_per_min, vegas_ratio_blk_pinnacle_per_min, vegas_ratio_blk_pinnacle_effect_per_min, vegas_ratio_blk_opp_pinnacle_per_min, vegas_ratio_blk_opp_pinnacle_effect_per_min, vegas_ratio_blk_ou_pinnacle_per_min, vegas_ratio_blk_ou_pinnacle_effect_per_min, actual_blk_per_min ].to_csv

      features_steals = [ previous_average[:STL_mean].to_f, previous_average[:PCT_STL].to_f,team_average[:o_PCT_STL].to_f,o_stl_delta, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_stl_effect,rest_stl_effect,location_stl,rest_stl,opp_rest_stl,opp_rest_stl_effect,starter,mean_starter_stl,mean_bench_stl,starterbench_stl_effect,mean_starterbench_stl,prev_stl,prev_stl_delta,prev2_stl,prev2_stl_delta,prev5_stl,prev5_stl_delta,o_stl_pct_delta,expected_STL,expected_STL_effect,scaled_steal,scaled_steal_effect,scaled_pct_stl_effect,modded_steal,modded_steal_effect,expected_STL_pace, stl_pace_effect, expected_STL_pace2, stl_pace2_effect, expected_STL_pace3, stl_pace3_effect, expected_STL_def_rtg, def_rtg_STL_effect, expected_STL_off_rtg, off_rtg_STL_effect, expected_STL_opp_STL, expected_STL_opp_STL_effect, vegas_ratio_stl, vegas_ratio_stl_effect, vegas_ratio_stl_pinnacle, vegas_ratio_stl_pinnacle_effect, vegas_ratio_stl_opp_pinnacle, vegas_ratio_stl_opp_pinnacle_effect, vegas_ratio_stl_ou_pinnacle, vegas_ratio_stl_ou_pinnacle_effect, boxscore[:STL].to_i ].to_csv 

      features_steals_per_min = [ mean_stl_per_min, previous_average[:PCT_STL].to_f,team_average[:o_PCT_STL].to_f,o_stl_delta_per_min, b2b,opp_b2b,extra_rest,opp_extra_rest,location,location_stl_effect_per_min,rest_stl_effect_per_min,location_stl_per_min,rest_stl_per_min,opp_rest_stl_per_min,opp_rest_stl_effect_per_min,starter,mean_starter_stl_per_min,mean_bench_stl_per_min,starterbench_stl_effect_per_min,mean_starterbench_stl_per_min,prev_stl_per_min,prev_stl_delta_per_min,prev2_stl_per_min,prev2_stl_delta_per_min,prev5_stl_per_min,prev5_stl_delta_per_min,o_stl_pct_delta,expected_STL_per_min,expected_STL_effect_per_min,scaled_steal_per_min,scaled_steal_effect_per_min,scaled_pct_stl_effect_per_min,modded_steal_per_min,modded_steal_effect_per_min,expected_STL_pace_per_min, stl_pace_effect_per_min, expected_STL_pace2_per_min, stl_pace2_effect_per_min, expected_STL_pace3_per_min, stl_pace3_effect_per_min, expected_STL_def_rtg_per_min, def_rtg_STL_effect_per_min, expected_STL_off_rtg_per_min, off_rtg_STL_effect_per_min, expected_STL_opp_STL_per_min, expected_STL_opp_STL_effect_per_min, vegas_ratio_stl_per_min, vegas_ratio_stl_effect_per_min, vegas_ratio_stl_pinnacle_per_min, vegas_ratio_stl_pinnacle_effect_per_min, vegas_ratio_stl_opp_pinnacle_per_min, vegas_ratio_stl_opp_pinnacle_effect_per_min, vegas_ratio_stl_ou_pinnacle_per_min, vegas_ratio_stl_ou_pinnacle_effect_per_min, actual_stl_per_min ].to_csv 

      #expected_OREB, previous_average[:o_DREB_PCT].to_f, b2b.to_f, mean_b2b_OREB.to_f, mean_b2b_OREB_PCT.to_f, mean_extra_rest_OREB.to_f, mean_extra_rest_OREB_PCT.to_f, r[:mean_opp_b2b

      features_seconds = [ average_seconds, b2b, extra_rest, location, rest_effect_seconds, location_pts_effect_seconds, prev_seconds, prev2_seconds, prev5_seconds, starter, mean_starter_seconds, mean_bench_seconds, mean_starterbench_seconds, avg_mins_10_or_less, avg_mins_20_or_less, avg_mins_30_or_less, avg_mins_over_30, vegas_average[:over_under_Pinnacle].to_f, over_under_ratio_pinnacle, point_spread_abs_3_or_less, point_spread_abs_6_or_less, point_spread_abs_9_or_less, point_spread_abs_12_or_less, point_spread_abs_over_9, point_spread_abs_over_12, point_spread_3_or_less, point_spread_6_or_less, point_spread_9_or_less, point_spread_12_or_less, point_spread_over_9, point_spread_over_12, point_spread_neg_3_or_less, point_spread_neg_6_or_less, point_spread_neg_9_or_less, point_spread_neg_12_or_less, point_spread_neg_over_9, point_spread_neg_over_12, actual_SECONDS ].to_csv
      #features_seconds_delta = [ average_seconds, b2b, extra_rest, location, rest_effect_seconds, location_pts_effect_seconds, prev_seconds_delta, prev2_seconds_delta, prev5_seconds_delta, actual_SECONDS ].to_csv

      File.open("points_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_points ) }
      File.open("orebs_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_orebs ) }
      File.open("drebs_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_drebs ) }
      File.open("steals_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_steals ) }
      File.open("assists_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_assists) }
      File.open("blocks_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_blocks) }
      File.open("turnovers_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_turnovers) }
      File.open("points_per_min_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_points_per_min ) }
      File.open("orebs_per_min_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_orebs_per_min ) }
      File.open("drebs_per_min_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_drebs_per_min ) }
      File.open("assists_per_min_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_assists_per_min ) }
      File.open("turnovers_per_min_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_turnovers_per_min ) }
      File.open("blocks_per_min_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_blocks_per_min ) }
      File.open("steals_per_min_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_steals_per_min ) }
      File.open("seconds_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_seconds ) }

=begin
      begin
        tablename = "_" + season + " " + type + " daily averages"
        tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
        rows_updated = database[:"#{tablename}"].select_all.where(:date => date, :PLAYER_NAME => player[:PLAYER_NAME], :average_type => nil).update( r )
        if 0 == rows_updated
          binding.pry
          p "0 rows updated problem"
        else
          p "#{rows_updated} rows updated for #{player} on #{date}"
        end
        
      rescue StandardError => e
        binding.pry
        p "hi"
      end
=end
    rescue StandardError => e
      binding.pry
      p "hi"
    end
    t1_loop = Time.now
    p "done player boxscore #{i} / #{player_boxscores.size}. #{t1_loop-t0_loop}"
  }
  t1 = Time.now
  p "done #{player_boxscores.size} boxscores. total time: #{t1-t0} per_game: #{(t1-t0).to_f/player_boxscores.size.to_f}"
  
  #Figure out pace and points per possession for team
=begin
  pace = league_average[:PACE]
  team = league_average[:team_abbreviation]
  opponent = league_average[:opponent_against_abbr]
  avg_league_pace = row2[:mean_PACE]

  #Figure out pace and points per possession for player
  #Figure out usage percentage of player, determining how many total possessions he has a chance to use / shoot?
 #Let's combine these for now

 #dig into the hash structure that holds the gameIDs of all the games the player has played
  #parse that and figure out how many actual games the player has played in how many nights
  avg_rest_PTS = #
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end

#=begin 
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

 ##### -adjust for defensive rating of opposing team
  -adjust for primary defender on opposing team
  -how well does opposing team defend 2PFG, 3PFGs, FTAs?
  -adjust for how opposing team defends that position
  -factor in 3PFG, 2PFG, FTs, team assists (easy or hard baskets).  can we
  factor in 2PFGs by layups, midrange, etc.?
#=end
  teamStatSet.split.advancedStats.DEF_RATING.mean
  
  #we need to combine starters and bench stats
  #we need to compared our derived stats with 3rd party sites
  opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ].split.player_name = boxscore_traditional[:PLAYER_NAME]
  opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ].split.team_abbreviation = boxscore_traditional[:TEAM_ABBREVIATION]
  opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ].split.opponent_against_abbr = team[:TEAM_ABBREVIATION]

  avg_location_PTS = split.away.avg_PTS
  avg_opponent_defensive_rating = (opp_defensive_rating / 100) # need to scale this and multiply by coefficient
  avg_opponent_defensive_rating = (opp_defensive_rating / 100) # need to scale this and multiply by coefficient
  avg_opponent_defender_rating = (opp_defender_rating / 100) # need to scale this and multiply by coefficient
  
  if split.games_played > 0
    avg_pts_REST_mode = split.rest_mode.PTS / split.games_played
  end
=end

end

  seasons = seasons_h.keys

  seasontypes = ["regularseason", "playoffs"]
  categories = ["traditional","advanced", "misc", "scoring", "usage", "fourfactors", "playertrack"]

  tables = [];player_tables = [];team_tables = []
  if 1 #SEASON.split("-")[0].to_i > 2014
    tables = [ "advanced_PlayerStats", "advanced_TeamStats", "fourfactors_sqlPlayersFourFactors", "fourfactors_sqlTeamsFourFactors", "misc_sqlPlayersMisc", "misc_sqlTeamsMisc", "playertrack_PlayerStats", "playertrack_TeamStats", "scoring_sqlPlayersScoring", "scoring_sqlTeamsScoring", "traditional_PlayerStats", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlPlayersUsage", "usage_sqlTeamsUsage" ]
  player_tables = [ "advanced_PlayerStats", "fourfactors_sqlPlayersFourFactors", "misc_sqlPlayersMisc", "playertrack_PlayerStats", "scoring_sqlPlayersScoring", "traditional_PlayerStats", "usage_sqlPlayersUsage" ]
  team_tables = [ "advanced_TeamStats", "fourfactors_sqlTeamsFourFactors", "misc_sqlTeamsMisc", "playertrack_TeamStats", "scoring_sqlTeamsScoring", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlTeamsUsage" ]
  else
    tables = [ "advanced_PlayerStats", "advanced_TeamStats", "fourfactors_sqlPlayersFourFactors", "fourfactors_sqlTeamsFourFactors", "misc_sqlPlayersMisc", "misc_sqlTeamsMisc", "playertrack_PlayerTrack", "playertrack_PlayerTrackTeam", "scoring_sqlPlayersScoring", "scoring_sqlTeamsScoring", "traditional_PlayerStats", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlPlayersUsage", "usage_sqlTeamsUsage" ]
  player_tables = [ "advanced_PlayerStats", "fourfactors_sqlPlayersFourFactors", "misc_sqlPlayersMisc", "playertrack_PlayerTrack", "scoring_sqlPlayersScoring", "traditional_PlayerStats", "usage_sqlPlayersUsage" ]
  team_tables = [ "advanced_TeamStats", "fourfactors_sqlTeamsFourFactors", "misc_sqlTeamsMisc", "playertrack_PlayerTrackTeam", "scoring_sqlTeamsScoring", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlTeamsUsage" ]
  end

  
  #http://stats.nba.com/stats/commonplayerinfo?LeagueID=00&PlayerID=202699&SeasonType=Regular+Season

  seasontype_url = [["Regular+Season", "regularseason"],["Playoffs", "playoffs"]]

  def fillMissingDBData( season, type, category, player_id, database )
    filename = season +"/" + type + "/" + player_id + "_" + category + ".csv"
      tablename = season + "/" + type + " " + category
      tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

      p "processing #{filename}"
      options = { :headers    => true,
                  :header_converters => nil,
                  :converters => nil }
          
      data = CSV.table(filename, options)

      begin
        game_id = filename.split("/").last.split("_").first
        #database[tablename].where("game_id = '#{game_id}' and team_id = '#{h['TEAM_ID']}'").delete
        database[tablename].where("game_id = '#{game_id}'").delete

        data.by_row!.each do |row|
          h = row.to_hash
          if h["SEASON"]
            #don't need season bc we are naming the table w/ the season
            h.delete("SEASON")
          end

          database[tablename].insert( h )
        end
      rescue StandardError => e
        binding.pry
        p e
      end
      #binding.pry
      #p "done"
  end

  def fillGamelogDBData( season, type, category, player_id, database )
    filename = season +"/" + type + "/" + player_id + "_" + category + ".csv"
    tablename = season + "/" + type + " " + category
    tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

    p "processing #{filename}"
    options = { :headers    => true,
                :header_converters => nil,
                :converters => nil }

    data = CSV.table(filename, options)

    begin
      data.by_row!.each do |row|
        h = row.to_hash
        if h["SEASON"]
          #don't need season bc we are naming the table w/ the season
          h.delete("SEASON")
        end
        database[tablename].insert( h )
      end
    rescue StandardError => e
      binding.pry
      p e
    end
    #binding.pry
    #p "done"
  end

  def populateData( database, tablename, filename, col_sep = "," )
      tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
        options = { :headers    => true,
                    :header_converters => nil,
                    :col_sep => col_sep,
                    :converters => :all  }

      if !database.table_exists? tablename
        data = CSV.table(filename, options)
        headers = data.headers

        puts "Dropping and re-creating table #{tablename}"
        database.drop_table? tablename
        database.create_table tablename do
          # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
          # primary_key :id
          # Float :price
          data.by_col!.each do |columnName,rows|
            p "columnName: #{columnName}"
            columnType = getCommonClass(rows) || String
            column columnName, columnType
          end
        end
        data.by_row!.each do |row|
          database[tablename].insert(row.to_hash)
        end
      else
        p "processing #{filename}"
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
  end

def parseSeason( season, database )
  categories = ["traditional","advanced", "misc", "scoring", "usage", "fourfactors", "playertrack"]#, "playbyplay"]

  doc = `curl 'http://data.nba.com/data/10s/v2015/json/mobile_teams/nba/#{season.split("-")[0]}/league/00_full_schedule.json' -H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/game/' -H 'Connection: keep-alive' --compressed`
  j = JSON.parse doc

  bSkipRegularSeason = false
  lastGameID = "0"

  if Dir["#{season}/playoffs/*traditional_PlayerStats.csv"].length > 0
    bSkipRegularSeason = true
    lastGameID = Dir["#{season}/playoffs/*traditional_PlayerStats.csv"].sort.last.match(/\/(\d+)/)[1]
  else
    if Dir["#{season}/regularseason/*traditional_PlayerStats.csv"].length > 0
      lastGameID = Dir["#{season}/regularseason/*traditional_PlayerStats.csv"].sort.last.match(/\/(\d+)/)[1]
    end
  end

  p "lastGameID: #{lastGameID}"
  bBreakEarly = false
  j["lscd"].each{|month|
    month["mscd"]["g"].each{|game|
      if game["stt"] != "Final"
        bBreakEarly = true
        break
      end

      season_type = nil
      if game["gid"].match /^002/
        season_type = ["regularseason", "Regular+Season"]
        team_dir = FileUtils::mkdir_p season + "/" + season_type[0]

        if true == bSkipRegularSeason or game["gid"] <= lastGameID
          next
        end
      elsif game["gid"].match /^004/
        season_type = ["playoffs", "Playoffs"]
        team_dir = FileUtils::mkdir_p season + "/" + season_type[0]

        if game["gid"] <= lastGameID
          next
        end
      else
        next
      end

      game_id = game["gid"]
      if game_id >= lastGameID
        p "#{game_id} >= #{lastGameID}"
        #bBreakEarly = true
        #break
      end

      categories.each{|category|
        begin
          d = `curl 'https://stats.nba.com/stats/boxscore#{category}v2?EndPeriod=10&EndRange=28800&GameID=#{game_id}&RangeType=0&Season=#{season}&SeasonType=#{season_type[1]}&StartPeriod=1&StartRange=0' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:64.0) Gecko/20100101 Firefox/64.0' -H 'Accept: application/json, text/plain, */*' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Referer: https://stats.nba.com/game/0021800001/' -H 'X-NewRelic-ID: VQECWF5UChAHUlNTBwgBVw==' -H 'x-nba-stats-origin: stats' -H 'x-nba-stats-token: true' -H 'DNT: 1' -H 'Connection: keep-alive' -H '#{OPTIONS[:cookie]}'`

          #-H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/game/' -H 'Connection: keep-alive' --compressed`
          boxscore_json = JSON.parse( d )


          boxscore_json["resultSets"].each{|resultSet|
            csv = ""
            csv = csv + resultSet["headers"].insert(1,"SEASON").insert(2,"DATE").to_csv

            date = Date.parse( game["gdte"] )
            resultSet["rowSet"].each{|row|
              csv = csv + row.insert(1,season).insert(2,date.to_s).to_csv
            }

            #team_dir = FileUtils::mkdir_p dir[0] + "/" + team_abbr
            filename = season + "/" + season_type[0] + "/" + game_id + "_" + category + "_" + resultSet["name"] + ".csv"
            tablename = season + "_" + season_type[0] + " " + category + "_" + resultSet["name"]
            #tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

            File.open( filename, "w" ){|f|
              f.write csv
            }
            populateData_worker( database, tablename, filename )
          }
        rescue StandardError => e
          binding.pry
          p "hi"
        end
      }
      p "#{game_id} done"
    }
    if true == bBreakEarly
      break
    end
  }
end

def populateFanDuelInfo( seasons, database )
  seasons.each{|season|
    ["regularseason", "playoffs"].each{|type|
      if !database.table_exists? :"#{season.gsub("-","_")}_#{type}_traditional_TeamStats"
        next
      end
      tablename = type + "_fanduelinfo"
      database.drop_table? :"#{tablename}"
      Dir.glob( season + "/" + type + "/fanduelsalaries/*.csv" ).each_with_index{|filename,i|
        bRecreateTable = false
        populateData_worker( database, "#{tablename}", filename, bRecreateTable, ";" )
      }

      #add player ID column
      database.add_column :"#{tablename}", :PLAYER_ID, :text
    }
  }
end

def addDateToFanDuelSalaryFile( filename )
  #check if date is already in columns
  # Load the original CSV file
  rows = CSV.read(filename, headers: true).collect do |row|
    row.to_hash
  end

  # Original CSV column headers
  column_names = rows.first.keys
  hasDate = false
  column_names.each{|column|
    if column.match(/date/i)
      hasDate = true
      break
    end
  }
  if true == hasDate
    return
  end

  date_str = filename.split("-")[3] + "-" + filename.split("-")[4] + "-" + filename.split("-")[2]

  # Load the original CSV file( filename )
  rows = CSV.read(filename, headers: true).collect do |row|
    hash = row.to_hash
    # Merge additional data as a hash.
    hash.merge('Date' => date_str)
    # BONUS: Change any existing data here too!
    #hash.merge('a1' => hash['a1'].to_i + 1 )
  end

  # Extract column names from first row of data
  column_names = rows.first.keys
  txt = CSV.generate do |csv|
    csv << column_names
    rows.each do |row|
      # Extract values for row of data
      csv << row.values
    end
  end

  # Overwrite csv file
  File.open(filename, 'w') { |file| file.write(txt) }
end

def populateData_worker( database, tablename, filename, bRecreate = false, col_sep = "," )
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
    options = { :headers    => true,
                :header_converters => nil,
                :col_sep => col_sep,
                :converters => nil }
  if (true == bRecreate) or !database.table_exists? tablename

    #season = season.gsub(/-/,"")
    #filename = filename.gsub(/-/,"_")

    data = CSV.table(filename, options)
    headers = data.headers
  p "filename: #{filename} #{headers}"

    puts "Dropping and re-creating table #{tablename} #{filename}"
    database.drop_table? tablename
    begin
      database.create_table tablename do
        # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
        # primary_key :id
        # Float :price
        data.by_col!.each do |columnName,rows|
          #columnType = getCommonClass(rows) || String
          columnType = String
          #p "#{columnType} #{rows} #{filename}"
          column columnName, columnType
        end
      end
      data.by_row!.each do |row|
        database[tablename].insert(row.to_hash)
      end
    rescue StandardError => e
      binding.pry
      p 'hi'
    end
  else
    p "processing #{filename}"

    begin
    data = CSV.table(filename, options)
    rescue StandardError => e
      binding.pry
      p "hi"
    end

    begin
      data.by_row!.each do |row|
        database[tablename].insert(row.to_hash)
      end
    rescue StandardError => e
      binding.pry
      p e
    end
  end
end
#Push CSV files into DB
def populateAllData( seasons, seasontype, tables, database, bRecreate = false )
  seasons.each{|season|
    seasontype.each{|type|
      tables.each{|category|
        bRecreateLocal = bRecreate
        Dir.glob( season + "/" + type + "/*" + category + ".csv").each_with_index{|filename,i|
          tablename = season + " " + type + " " + category
          #tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

          #only recreate for 1st file, even w/ bRecreate flag
          if (true == bRecreateLocal) and (0 == i)
            bRecreateLocal = true
          else
            bRecreateLocal = false
          end

          populateData_worker( database, tablename, filename, bRecreateLocal )
        }
      }
    }
  }
end

def populateGamelogs( database, seasons_h )
  seasontypes = [ ["regularseason", "Regular+Season"], ["playoffs", "Playoffs"] ]

  seasons_h.keys.each{|season|
    seasontypes.each{|seasontype|
      Dir.glob( season + "/" + seasontype[0] + "/gamelogs/*.csv").each_with_index{|filename,i|
        tablename = season + " " + seasontype[0] + " gamelogs"
        bRecreate = false

        #only recreate for 1st file, even w/ bRecreate flag
        if 0 == i
          bRecreate = true
        end

        populateData_worker( database, tablename, filename, bRecreate )
      }
    }
  }
end

def getGamelogs( database, seasons_h )
  seasontypes = [ ["regularseason", "Regular+Season"], ["playoffs", "Playoffs"] ]

  seasons_h.keys.each{|season|
    date_index = 0

    seasontypes.each{|seasontype|
      dir = FileUtils::mkdir_p season + "/" + seasontype[0] + "/gamelogs/"

      lastGameDate = nil

      if Dir["#{season}/playoffs/gamelogs/*.csv"].length > 0
        arr = Dir["#{season}/playoffs/gamelogs/*.csv"].sort_by{|str| str.split("_")[2]}
        lastGameDate = arr.last.split("_").last.split(".csv")[0]
      else
        if Dir["#{season}/regularseason/gamelogs/*.csv"].length > 0
          arr = Dir["#{season}/regularseason/gamelogs/*.csv"].sort_by{|str| str.split("_")[2]}
          lastGameDate = arr.last.split("_").last.split(".csv")[0]
        end
      end
      
      lastGameDate = nil

      if nil == lastGameDate
        date = Date.parse( seasons_h.values.first[date_index] )
      else
        date = Date.parse( lastGameDate ) + 1
      end
      
      end_date = Date.parse( seasons_h.values.first[date_index + 1] )

      while date < end_date
        tablename = season + " " + seasontype[0] + " gamelogs"
        tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

        #url = "http://stats.nba.com/stats/leaguegamelog?Counter=1000&Direction=DESC&LeagueID=00&PlayerOrTeam=T&Season=#{season}&SeasonType=#{seasontype[1]}&Sorter=PTS"
        date_str = "#{date.month}%2F#{date.day}%2F#{date.year}"
        url = "https://stats.nba.com/stats/leaguegamelog?Counter=1000&DateFrom=#{date_str}&DateTo=#{date_str}&Direction=DESC&LeagueID=00&PlayerOrTeam=T&Season=#{season}&SeasonType=#{seasontype[1]}&Sorter=DATE"
        #doc = `curl '#{url}' -H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/game/' -H 'Connection: keep-alive' --compressed`
        doc = `curl '#{url}' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:64.0) Gecko/20100101 Firefox/64.0'  -H 'Accept: application/json, text/plain, */*' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Referer: https://stats.nba.com/team/1610612749/boxscores/' -H 'x-nba-stats-origin: stats' -H 'x-nba-stats-token: true' -H 'DNT: 1' -H 'Connection: keep-alive' -H '#{OPTIONS[:cookie]}'`

        p "date: #{date}"
        begin
        json = JSON.parse( doc )
        rescue StandardError => e
          binding.pry
          p 'hi'
        end

        if !database.table_exists? tablename
          options = { :headers    => true,
                      :header_converters => nil,
                      :converters => nil } # This makes all fields strings

          headers = json["resultSets"][0]["headers"]

          puts "Dropping and re-creating table #{tablename}"
          database.drop_table? tablename
          database.create_table tablename do
            headers.each do |columnName|
              columnType = String
              column columnName, columnType
            end
          end
        end

        csv = json["resultSets"][0]["headers"].to_csv
        json["resultSets"][0]["rowSet"].each{|row|

          csv = csv + row.to_csv

          File.open( season + "/" + seasontype[0] + "/gamelogs" + "/" + row[2] + "_" + row[4] + "_" + "#{date.strftime("%Y-%m-%d")}" + ".csv", "w" ){|f|
            f.write csv
          }
          str = season + "/" + seasontype[0] + "/gamelogs" + "/" + row[2] + "_" + row[4] + "_" + "#{date.strftime("%Y-%m-%d")}" + ".csv"
          p str

          h = Hash.new
          row.each_with_index{|item,i|
            h[ json["resultSets"][0]["headers"][i] ] = item
          }

          database[tablename].insert( h )
        }
        date = date + 1
      end

      #jlktodo - no playoffs for now
      break
      date_index = date_index + 1
    }
  }
end

def calculateAverages( boxscore, row, fieldName, statSet, games_played, opponent )
  if true == opponent
    mean_sym = ('mean_o_' + fieldName).to_sym
    median_sym = ('median_o_' + fieldName).to_sym
    total_sym = ('total_o_' + fieldName).to_sym
  else
    mean_sym = ('mean_' + fieldName).to_sym
    median_sym = ('median_' + fieldName).to_sym
    total_sym = ('total_' + fieldName).to_sym
  end

  sym = fieldName.to_sym

  statSet.total, row[ mean_sym ] = mean( boxscore[ sym ].to_f, statSet.total, games_played )
  row[ total_sym ] = statSet.total
  statSet.hash[ boxscore[:GAME_ID]] = boxscore[ sym ].to_f
  row[ median_sym ] = statSet.median = median( hash )
  
  return total
end

def calculateAverages2( boxscore, sym, statSet, games_played )
  #gameTotal = boxscore[ sym ].to_f
  begin
  statSet.game_total = boxscore[ sym ].to_f
  rescue StandardError => e
    binding.pry
    p "hi"
  end
  statSet.total, statSet.mean = mean( boxscore[ sym ].to_f, statSet.total, games_played )
  game_id = boxscore[:GAME_ID]
  if nil == game_id
    game_id = boxscore[:Game_ID]
    if nil == game_id
      binding.pry
      p "hi"
    end
  end

  statSet.hash[ game_id ] = boxscore[ sym ].to_f
  statSet.median = median( statSet.hash )
end

def calculateAverages3( boxscore, game_total, statSet, games_played )
  statSet.game_total = game_total
  statSet.total, statSet.mean = mean( game_total, statSet.total, games_played )
  game_id = boxscore[:GAME_ID]
  if nil == game_id
    game_id = boxscore[:Game_ID]
    if nil == game_id
      binding.pry
      p "hi"
    end
  end

  statSet.hash[ game_id ] = game_total
  statSet.median = median( statSet.hash )
end

def calculateBoxscoreTime( gamelog, boxscore, statSet, bTeam )
  begin
    if boxscore[:MIN]
      time_played = Duration.new( :minutes => boxscore[:MIN].split(":")[0], :seconds => boxscore[:MIN].split(":")[1] )
    else
      time_played = 0
    end
  rescue StandardError => e
    binding.pry
    p "error"
  end

  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    split.timeStats.games_played = split.timeStats.games_played + 1

    if true == bTeam
      if gamelog[:WL] == "W"
        split.timeStats.wins = split.timeStats.wins + 1
      elsif gamelog[:WL] == "L"
        split.timeStats.losses = split.timeStats.losses + 1
      else
        binding.pry
        split.timeStats.ties = split.timeStats.ties + 1
      end

      begin
        split.timeStats.win_pct = split.timeStats.wins.to_f / (split.timeStats.games_played - split.timeStats.ties)
      rescue StandardError => e
        p "hi"
        binding.pry
      end
    end

    split.timeStats.seconds_played.total = split.timeStats.seconds_played.total + time_played.to_i
    split.timeStats.seconds_played.mean = (split.timeStats.seconds_played.total / split.timeStats.games_played)
    game_id = gamelog[:GAME_ID]
    if nil == game_id
      game_id = gamelog[:Game_ID]
      if nil == game_id
        binding.pry
        p "game_id err"
      end
    end
    split.timeStats.seconds_played.hash[ game_id ] = time_played.to_i
    split.timeStats.seconds_played.median = median( split.timeStats.seconds_played.hash )
  }
end

def printTotals( row, type, bTeam )
  if true == bTeam
    if "reg" == type
      puts "GP	MIN	W	L	W%	FGM	FGA	FG%	3PM	3PA	3P%	FTM	FTA	FT%	OREB	DREB	REB	AST	TOV	STL	BLK	PF	PTS	+/-"
      puts "#{row[:games_played]} #{row[:seconds_played_total].to_f/300} #{row[:wins]} #{row[:losses]} #{(100*row[:win_pct]).round(1)} #{row[:FGM_total]} #{row[:FGA_total]} #{(100*row[:FG_PCT]).round(1)} #{row[:FG3M_total]} #{row[:FG3A_total]} #{(100*row[:FG3_PCT]).round(1)} #{row[:FTM_total]} #{row[:FTA_total]} #{(100*row[:FT_PCT]).round(1)} #{row[:OREB_total]} #{row[:DREB_total]} #{row[:REB_total]} #{row[:AST_total]} #{row[:TOV_total]} #{row[:STL_total]} #{row[:BLK_total]} #{row[:o_BLK_total]} #{row[:PF_total]} #{row[:o_PF_total]} #{row[:PTS_total]} #{row[:PLUS_MINUS_total]}"
    elsif "adv" == type
      puts "GP	MIN	OffRtg	DefRtg	NetRtg	AST%	AST/TO	AST Ratio	OREB%	DREB%	REB%	TO Ratio	eFG%	TS%	PACE	PIE"
      puts "#{row[:games_played]} #{row[:seconds_played_total].to_f/300} #{row[:OFF_RATING].round(1)} #{row[:DEF_RATING].round(1)} #{row[:NET_RATING].round(1)} #{row[:AST_PCT].round(1)} #{row[:AST_TOV].round(2)} #{(100*row[:AST_RATIO]).round(1)} #{row[:OREB_PCT].round(1)} #{row[:DREB_PCT].round(1)} #{row[:REB_PCT].round(1)} #{(100*row[:TO_RATIO]).round(1)} #{(100*row[:EFG_PCT]).round(1)} #{(100*row[:TS_PCT]).round(1)} #{row[:PACE].round(2)} #{(100*row[:PIE]).round(1)}" 
    elsif "four" == type
      puts "GP	MIN	W	L	W%	eFG%	FTA Rate	TO Ratio	OREB%	OppeFG%	OppFTARate	OppTORatio	OppOREB%"
      puts "#{row[:games_played]} #{row[:seconds_played_total].to_f/300} #{row[:wins]} #{row[:losses]} #{(100*row[:win_pct]).round(1)} #{(100*row[:EFG_PCT]).round(1)} #{row[:FTA_RATE].round(3)} #{(100*row[:TO_RATIO]).round(1)} #{row[:OREB_PCT].round(1)} #{(100*row[:o_EFG_PCT]).round(1)} #{row[:o_FTA_RATE].round(3)} #{(100*row[:o_TO_RATIO]).round(1)} #{row[:o_OREB_PCT].round(1)}"
    elsif "misc" == type
      puts "GP	MIN	PTSOFFTO	2ndPTS	FBPs	PITP	OppPTSOFFTO	Opp2ndPTS	OppFBPs	OppPITP"
      puts "#{row[:games_played]} #{row[:seconds_played_total].to_f/300} #{row[:PTS_OFF_TOV_total]} #{row[:PTS_2ND_CHANCE_total]} #{row[:PTS_FB_total]} #{row[:PTS_PAINT_total]} #{row[:o_PTS_OFF_TOV_total]} #{row[:o_PTS_2ND_CHANCE_total]} #{row[:o_PTS_FB_total]} #{row[:o_PTS_PAINT_total]}"
    elsif "scoring" == type
      puts "GP	MIN	%FGA2PT	%FGA3PT	%PTS2PT	%PTS2PT-MR	%PTS3PT	%PTSFBPs	%PTSFT	%PTSOffTO	%PTSPITP	2FGM%AST	2FGM%UAST	3FGM%AST	3FGM%UAST	FGM%AST	FGM%UAST"
      #puts "#{row[:games_played]} #{row[:seconds_played_total].to_f/300} #{row[:PCT_AST_2PM]} #{row[:PCT_UAST_2PM]} #{row[:PCT_AST_3PM]} #{row[:PCT_UAST_3PM]} #{row[:PCT_AST_FGM]} #{row[:PCT_UAST_FGM]} #{row[:PCT_FGA_2PT]} #{row[:PCT_PTS_2PT]} #{row[:PCT_PTS_3PT]} #{row[:PCT_PTS_FB]} #{row[:PCT_PTS_FT]} #{row[:PCT_PTS_OFF_TOV]} #{row[:PCT_PTS_PAINT]}"
      puts "#{row[:games_played]} #{row[:seconds_played_total].to_f/300} #{(100*row[:PCT_FGA_2PT]).round(1)} #{(100*row[:PCT_FGA_3PT]).round(1)} #{(100*row[:PCT_PTS_2PT]).round(1)} #{(100*row[:PCT_PTS_2PT_MR]).round(1)} #{(100*row[:PCT_PTS_3PT]).round(1)} #{(100*row[:PCT_PTS_FB]).round(1)} #{(100*row[:PCT_PTS_FT]).round(1)} #{(100*row[:PCT_PTS_OFF_TOV]).round(1)} #{(100*row[:PCT_PTS_PAINT]).round(1)} #{(100*row[:PCT_AST_2PM]).round(1)} #{(100*row[:PCT_UAST_2PM]).round(1)} #{(100*row[:PCT_AST_3PM]).round(1)} #{(100*row[:PCT_UAST_3PM]).round(1)} #{(100*row[:PCT_AST_FGM]).round(1)} #{(100*row[:PCT_UAST_FGM]).round(1)}"
    elsif "opp" == type
      puts "#{row[:games_played]} #{row[:seconds_played_total].to_f/300} #{row[:wins]} #{row[:losses]} #{(100*row[:win_pct]).round(1)} #{row[:o_FGM_total]} #{row[:o_FGA_total]} #{(100*row[:o_FG_PCT]).round(1)} #{row[:o_FG3M_total]} #{row[:o_FG3A_total]} #{(100*row[:o_FG3_PCT]).round(1)} #{row[:o_FTM_total]} #{row[:o_FTA_total]} #{(100*row[:o_FT_PCT]).round(1)} #{row[:o_OREB_total]} #{row[:o_DREB_total]} #{row[:o_REB_total]} #{row[:o_AST_total]} #{row[:o_TOV_total]} #{row[:o_STL_total]} #{row[:o_BLK_total]} #{row[:BLK_total]} #{row[:o_PF_total]} #{row[:PF_total]} #{row[:o_PTS_total]} #{row[:o_PLUS_MINUS_total]}"
    #elsif "shooting" == type
    elsif "tracking" == type
      #puts "#{row[:games_played]} #{row[:seconds_played_total].to_f/300} #{row[:PCT_FGA_2PT]} #{row[:PCT_FGA_3PT]} #{row[:PCT_PTS_2PT]} #{row[:PTS_PAINT_total]} #{row[:o_PTS_OFF_TOV_total]} #{row[:o_PTS_2ND_CHANCE_total]} #{row[:o_PTS_FB_total]} #{row[:o_PTS_PAINT_total]}"
    end
  else
    if "reg" == type
      puts "GP	MIN	PTS FGM	FGA	FG%	3PM	3PA	3P%	FTM	FTA	FT%	OREB	DREB	REB	AST	TOV	STL	BLK	PF	+/-"
      puts "#{row[:games_played].to_i} #{(row[:seconds_played_total].to_f/60).round(1)} #{row[:PTS_total].to_i} #{row[:FGM_total].to_i} #{row[:FGA_total].to_i} #{(100*row[:FG_PCT].to_f).round(1)} #{row[:FG3M_total].to_i} #{row[:FG3A_total].to_i} #{(100*row[:FG3_PCT].to_f).round(1)} #{row[:FTM_total].to_i} #{row[:FTA_total].to_i} #{(100*row[:FT_PCT].to_f).round(1)} #{row[:OREB_total].to_i} #{row[:DREB_total].to_i} #{row[:REB_total].to_i} #{row[:AST_total].to_i} #{row[:TOV_total].to_i} #{row[:STL_total].to_i} #{row[:BLK_total].to_i} #{row[:PF_total].to_i} #{row[:PTS_total].to_i} #{row[:PLUS_MINUS_total].to_i}"
    elsif "adv" == type
      puts "GP	MIN	OffRtg	DefRtg	NetRtg	AST%	AST/TO	AST Ratio	OREB%	DREB%	REB%	TO Ratio	eFG%	TS%	PACE	PIE"
      puts "#{row[:games_played].to_f} #{(row[:seconds_played_total].to_f/60).round(1)} #{row[:OFF_RATING].to_f.round(1)} #{row[:DEF_RATING].to_f.round(1)} #{row[:NET_RATING].to_f.round(1)} #{row[:AST_PCT].to_f.round(1)} #{row[:AST_TOV].to_f.round(2)} #{(100*row[:AST_RATIO].to_f).round(1)} #{(100*row[:OREB_PCT]).to_f.round(1)} #{(100*row[:DREB_PCT]).to_f.round(1)} #{(100*row[:REB_PCT]).to_f.round(1)} #{(100*row[:TO_RATIO].to_f).round(1)} #{(100*row[:EFG_PCT].to_f).round(1)} #{(100*row[:TS_PCT].to_f).round(1)} #{(100*row[:USG_PCT].to_f).round(1)} #{row[:PACE].to_f.round(2)} #{(100*row[:PIE].to_f).round(1)}" 
    elsif "misc" == type
      puts "GP	MIN	PTSOFFTO	2ndPTS	FBPs	PITP	OppPTSOFFTO	Opp2ndPTS	OppFBPs	OppPITP"
      puts "#{row[:games_played].to_f} #{(row[:seconds_played_total].to_f/60).round(1)} #{row[:PTS_OFF_TOV_total].to_f} #{row[:PTS_2ND_CHANCE_total].to_f} #{row[:PTS_FB_total].to_f} #{row[:PTS_PAINT_total].to_f} #{row[:o_PTS_OFF_TOV_total].to_f} #{row[:o_PTS_2ND_CHANCE_total].to_f} #{row[:o_PTS_FB_total].to_f} #{row[:o_PTS_PAINT_total].to_f} #{row[:BLK_total].to_f} #{row[:BLKA_total].to_f} #{row[:PF_total].to_f} #{row[:PFD_total].to_f}"
    elsif "scoring" == type
      puts "GP	MIN	%FGA2PT	%FGA3PT	%PTS2PT	%PTS2PT-MR	%PTS3PT	%PTSFBPs	%PTSFT	%PTSOffTO	%PTSPITP	2FGM%AST	2FGM%UAST	3FGM%AST	3FGM%UAST	FGM%AST	FGM%UAST"
      puts "#{row[:games_played].to_f} #{(row[:seconds_played_total].to_f/60).round(1)} #{(100*row[:PCT_FGA_2PT].to_f).round(1)} #{(100*row[:PCT_FGA_3PT].to_f).round(1)} #{(100*row[:PCT_PTS_2PT].to_f).round(1)} #{(100*row[:PCT_PTS_2PT_MR].to_f).round(1)} #{(100*row[:PCT_PTS_3PT].to_f).round(1)} #{(100*row[:PCT_PTS_FB].to_f).round(1)} #{(100*row[:PCT_PTS_FT].to_f).round(1)} #{(100*row[:PCT_PTS_OFF_TOV].to_f).round(1)} #{(100*row[:PCT_PTS_PAINT].to_f).round(1)} #{(100*row[:PCT_AST_2PM].to_f).round(1)} #{(100*row[:PCT_UAST_2PM].to_f).round(1)} #{(100*row[:PCT_AST_3PM].to_f).round(1)} #{(100*row[:PCT_UAST_3PM].to_f).round(1)} #{(100*row[:PCT_AST_FGM].to_f).round(1)} #{(100*row[:PCT_UAST_FGM].to_f).round(1)}"
    elsif "usage" == type
      puts "GP	MIN	USG%	%FGM	%FGA	%3PM	%3PA	%FTM	%FTA	%OREB	%DREB	%REB	%AST	%TOV	%STL	%BLK	%BLKA	%PF	%PFD	%PTS"
      puts "#{row[:games_played].to_f} #{(row[:seconds_played_total].to_f/60).round(1)} #{(100*row[:USG_PCT].to_f).round(1)} #{(100*row[:PCT_FGM].to_f).round(1)} #{(100*row[:PCT_FGA].to_f).round(1)} #{(100*row[:PCT_FG3M].to_f).round(1)} #{(100*row[:PCT_FG3A].to_f).round(1)} #{(100*row[:PCT_FTM].to_f).round(1)} #{(100*row[:PCT_FTA].to_f).round(1)} #{(100*row[:PCT_OREB].to_f).round(1)} #{(100*row[:PCT_DREB].to_f).round(1)} #{(100*row[:PCT_REB].to_f).round(1)} #{(100*row[:PCT_AST].to_f).round(1)} #{(100*row[:PCT_TOV].to_f).round(1)} #{(100*row[:PCT_STL].to_f).round(1)} #{(100*row[:PCT_BLK].to_f).round(1)} #{(100*row[:PCT_BLKA].to_f).round(1)} #{(100*row[:PCT_PF].to_f).round(1)}  #{(100*row[:PCT_PFD].to_f).round(1)} #{(100*row[:PCT_PTS].to_f).round(1)}"
    elsif "tracking" == type
      #puts "#{row[:games_played].to_f} #{(row[:seconds_played_total].to_f/60).round(1)} #{row[:PCT_FGA_2PT].to_f} #{row[:PCT_FGA_3PT].to_f} #{row[:PCT_PTS_2PT].to_f} #{row[:PTS_PAINT_total].to_f} #{row[:o_PTS_OFF_TOV_total].to_f} #{row[:o_PTS_2ND_CHANCE_total].to_f} #{row[:o_PTS_FB_total].to_f} #{row[:o_PTS_PAINT_total].to_f}"
    end
  end
end

def calculateTraditionalStats( boxscore, statSet, boxscore_traditional_team = nil, o_boxscore_traditional_team = nil )
  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    split.gamelogStats.game_id = boxscore[:GAME_ID]
    if nil == split.gamelogStats.game_id
      split.gamelogStats.game_id = boxscore[:Game_ID]
      if nil == split.gamelogStats.game_id
        p "hi"
        binding.pry
      end
    end
    calculateAverages2( boxscore, :FGM, split.gamelogStats.FGM, split.timeStats.games_played )
    calculateAverages2( boxscore, :FGA, split.gamelogStats.FGA, split.timeStats.games_played )
    calculateAverages2( boxscore, :FG_PCT, split.gamelogStats.FG_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :FG3M, split.gamelogStats.FG3M, split.timeStats.games_played )
    calculateAverages2( boxscore, :FG3A, split.gamelogStats.FG3A, split.timeStats.games_played )
    calculateAverages2( boxscore, :FG3_PCT, split.gamelogStats.FG3_PCT, split.timeStats.games_played )

    split.gamelogStats.FG2M.game_total = split.gamelogStats.FGM.game_total - split.gamelogStats.FG3M.game_total
    split.gamelogStats.FG2A.game_total = split.gamelogStats.FGA.game_total - split.gamelogStats.FG3A.game_total
    split.gamelogStats.FG2_PCT.game_total = (0 != split.gamelogStats.FG2A.game_total) ? (split.gamelogStats.FG2M.game_total.to_f / split.gamelogStats.FG2A.game_total.to_f) : 0
    boxscore[:FG2M] = split.gamelogStats.FG2M.game_total
    boxscore[:FG2A] = split.gamelogStats.FG2A.game_total
    boxscore[:FG2_PCT] = split.gamelogStats.FG2_PCT.game_total

    #p "games played: #{split.timeStats.games_played}"
    #p "FGM: #{split.gamelogStats.FGM.game_total} FG3M: #{split.gamelogStats.FG3M.game_total} FG2M: #{split.gamelogStats.FG2M.game_total}"
    #p "FGA: #{split.gamelogStats.FGA.game_total} FG3A: #{split.gamelogStats.FG3A.game_total} FG2A: #{split.gamelogStats.FG2A.game_total}"
    
    calculateAverages2( boxscore, :FG2M, split.gamelogStats.FG2M, split.timeStats.games_played )
    calculateAverages2( boxscore, :FG2A, split.gamelogStats.FG2A, split.timeStats.games_played )
    calculateAverages2( boxscore, :FG2_PCT, split.gamelogStats.FG2_PCT, split.timeStats.games_played )

    #p "FG2M total: #{split.gamelogStats.FG2M.total} mean: #{split.gamelogStats.FG2M.mean} median: #{split.gamelogStats.FG2M.median}"

    calculateAverages2( boxscore, :FTM, split.gamelogStats.FTM, split.timeStats.games_played )
    calculateAverages2( boxscore, :FTA, split.gamelogStats.FTA, split.timeStats.games_played )
    calculateAverages2( boxscore, :FT_PCT, split.gamelogStats.FT_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :OREB, split.gamelogStats.OREB, split.timeStats.games_played )
    calculateAverages2( boxscore, :DREB, split.gamelogStats.DREB, split.timeStats.games_played )
    calculateAverages2( boxscore, :REB, split.gamelogStats.REB, split.timeStats.games_played )
    calculateAverages2( boxscore, :AST, split.gamelogStats.AST, split.timeStats.games_played )
    calculateAverages2( boxscore, :STL, split.gamelogStats.STL, split.timeStats.games_played )
    calculateAverages2( boxscore, :BLK, split.gamelogStats.BLK, split.timeStats.games_played )
    calculateAverages2( boxscore, :TO, split.gamelogStats.TOV, split.timeStats.games_played )
    calculateAverages2( boxscore, :PF, split.gamelogStats.PF, split.timeStats.games_played )
    calculateAverages2( boxscore, :PTS, split.gamelogStats.PTS, split.timeStats.games_played )
    calculateAverages2( boxscore, :PLUS_MINUS, split.gamelogStats.PLUS_MINUS, split.timeStats.games_played )
  }
end

def calculateDerivedTraditionalStats( boxscore, statSet )
  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    split.derivedStats.FG_PCT = divide( split.gamelogStats.FGM.total, split.gamelogStats.FGA.total )
    split.derivedStats.FG3_PCT = divide( split.gamelogStats.FG3M.total, split.gamelogStats.FG3A.total )
    split.derivedStats.FG2_PCT = divide( split.gamelogStats.FG2M.total, split.gamelogStats.FG2A.total )
    split.derivedStats.FT_PCT = divide( split.gamelogStats.FTM.total, split.gamelogStats.FTA.total )
    
    #Need to figure out how to calculate these based on raw data
    tsa = split.gamelogStats.FGA.total + 0.44 * split.gamelogStats.FTA.total
    split.derivedStats.TS_PCT = divide( split.gamelogStats.PTS.total, ( 2 * tsa ).round(3) )
    split.derivedStats.EFG_PCT = divide( split.gamelogStats.FGM.total + 0.5 * split.gamelogStats.FG3M.total, split.gamelogStats.FGA.total )
    split.derivedStats.PCT_FGA_3PT = divide( split.gamelogStats.FG3A.total, split.gamelogStats.FGA.total )
    split.derivedStats.FTA_RATE = divide( split.gamelogStats.FTA.total, split.gamelogStats.FGA.total )

    #scoring
    split.derivedStats.FG2A = split.gamelogStats.FGA.total - split.gamelogStats.FG3A.total
    #binding.pry
    split.derivedStats.PCT_FGA_2PT = divide( split.derivedStats.FG2A, split.gamelogStats.FGA.total )
    #if split.derivedStats.FG2A != split.scoringStats.FGA_2PT.total
      #binding.pry
      #p "FG2A doesn't match. #{split.derivedStats.FG2A} #{split.scoringStats.FGA_2PT.total} #{boxscore}"
    #end

    total_PTS_2PT = split.gamelogStats.PTS.total - split.gamelogStats.FG3M.total * 3 - split.gamelogStats.FTM.total
    split.derivedStats.PCT_PTS_2PT = divide( total_PTS_2PT, split.gamelogStats.PTS.total )
    #if total_PTS_2PT != split.scoringStats.PTS_2PT.total
      #binding.pry
      #p "PTS_2PT. #{total_PTS_2PT} #{split.scoringStats.PTS_2PT.total}"
    #end

    split.derivedStats.PCT_PTS_2PT_MR = divide( split.scoringStats.PTS_2PT_MR.total, split.gamelogStats.PTS.total )

    total_PTS_3PT = split.gamelogStats.FG3M.total * 3
    split.derivedStats.PCT_PTS_3PT = divide( total_PTS_3PT, split.gamelogStats.PTS.total )
    #if total_PTS_3PT != split.scoringStats.PTS_3PT.total
      #binding.pry
      #p "PTS_3PT. #{total_PTS_3PT} #{split.scoringStats.PTS_3PT.total}"
    #end

    split.derivedStats.PCT_PTS_FB = divide( split.miscStats.PTS_FB.total, split.gamelogStats.PTS.total )
    #if split.miscStats.PTS_FB.total != split.scoringStats.PTS_FB.total
      #binding.pry
      #p "PTS_FB. #{split.miscStats.PTS_FB.total} #{split.scoringStats.PTS_FB.total}"
    #end

    split.derivedStats.PCT_PTS_FT = divide( split.gamelogStats.FTM.total, split.gamelogStats.PTS.total )
    #if split.gamelogStats.FTM.total != split.scoringStats.PTS_FT.total
      #binding.pry
      #p "PTS_FT. #{split.gamelogStats.FTM.total} #{split.scoringStats.PTS_FT.total}"
    #end

    split.derivedStats.PCT_PTS_OFF_TOV = divide( split.miscStats.PTS_OFF_TOV.total, split.gamelogStats.PTS.total )
    #if split.miscStats.PTS_OFF_TOV.total != split.scoringStats.PTS_OFF_TOV.total
      #binding.pry
      #p "PTS_OFF_TOV. #{split.miscStats.PTS_OFF_TOV.total} #{split.scoringStats.PTS_OFF_TOV.total}"
    #end

    split.derivedStats.PCT_PTS_PAINT = divide( split.miscStats.PTS_PAINT.total, split.gamelogStats.PTS.total )
    #if split.miscStats.PTS_PAINT.total != split.scoringStats.PTS_PAINT.total
      #binding.pry
      #p "PTS_PAINT. #{split.miscStats.PTS_PAINT.total} #{split.scoringStats.PTS_PAINT.total}"
    #end

    boxscore_FG2M = split.gamelogStats.FGM.game_total - split.gamelogStats.FG3M.game_total

    calculateAverages3( boxscore, (split.scoringDerivedStats.PCT_AST_2PM.game_total * boxscore_FG2M).round.to_f, split.scoringDerivedStats.AST_2PM, split.timeStats.games_played )
    calculateAverages3( boxscore, divide( split.scoringDerivedStats.AST_2PM.total, ( split.gamelogStats.FGM.total - split.gamelogStats.FG3M.total ).to_f ), split.scoringDerivedStats.PCT_AST_2PM, split.timeStats.games_played )

    split.derivedStats.PCT_AST_2PM = divide( split.scoringDerivedStats.AST_2PM.total, (split.gamelogStats.FGM.total - split.gamelogStats.FG3M.total) )

    calculateAverages3( boxscore, boxscore_FG2M - split.scoringDerivedStats.AST_2PM.game_total, split.scoringDerivedStats.UAST_2PM, split.timeStats.games_played )
    calculateAverages3( boxscore, divide( split.scoringDerivedStats.UAST_2PM.total, ( split.gamelogStats.FGM.total - split.gamelogStats.FG3M.total ).to_f ), split.scoringDerivedStats.PCT_UAST_2PM, split.timeStats.games_played )

    split.derivedStats.PCT_UAST_2PM = divide( split.scoringDerivedStats.UAST_2PM.total, (split.gamelogStats.FGM.total - split.gamelogStats.FG3M.total) )

    calculateAverages3( boxscore, (split.scoringDerivedStats.PCT_AST_3PM.game_total * split.gamelogStats.FG3M.game_total).round.to_f, split.scoringDerivedStats.AST_3PM, split.timeStats.games_played )
    calculateAverages3( boxscore, divide( split.scoringDerivedStats.AST_3PM.total, split.gamelogStats.FG3M.total ), split.scoringDerivedStats.PCT_AST_3PM, split.timeStats.games_played )

    split.derivedStats.PCT_AST_3PM = divide( split.scoringDerivedStats.AST_3PM.total, split.gamelogStats.FG3M.total )

    calculateAverages3( boxscore, split.gamelogStats.FG3M.game_total - split.scoringDerivedStats.AST_3PM.game_total, split.scoringDerivedStats.UAST_3PM, split.timeStats.games_played )
    calculateAverages3( boxscore, divide( split.scoringDerivedStats.UAST_3PM.total, split.gamelogStats.FG3M.total ), split.scoringDerivedStats.PCT_UAST_3PM, split.timeStats.games_played )

    split.derivedStats.PCT_UAST_3PM = divide( split.scoringDerivedStats.UAST_3PM.total, split.gamelogStats.FG3M.total )

    calculateAverages3( boxscore, (split.gamelogStats.FGM.game_total * split.scoringDerivedStats.PCT_AST_FGM.game_total).round.to_f, split.scoringDerivedStats.AST_FGM, split.timeStats.games_played )
    calculateAverages3( boxscore, divide( split.scoringDerivedStats.AST_FGM.total, split.gamelogStats.FGM.total ), split.scoringDerivedStats.PCT_AST_FGM, split.timeStats.games_played )

    split.derivedStats.PCT_AST_FGM = divide( split.scoringDerivedStats.AST_FGM.total, split.gamelogStats.FGM.total )

    calculateAverages3( boxscore, split.gamelogStats.FGM.game_total - split.scoringDerivedStats.AST_FGM.game_total, split.scoringDerivedStats.UAST_FGM, split.timeStats.games_played )
    calculateAverages3( boxscore, divide( split.scoringDerivedStats.UAST_FGM.total, split.gamelogStats.FGM.total ), split.scoringDerivedStats.PCT_UAST_FGM, split.timeStats.games_played )

    split.derivedStats.PCT_UAST_FGM = divide( split.scoringDerivedStats.UAST_FGM.total, split.gamelogStats.FGM.total )

    split.derivedStats.CFG_PCT = divide( split.trackingStats.CFGM.total, split.trackingStats.CFGA.total )
    split.derivedStats.UFG_PCT = divide( split.trackingStats.UFGM.total, split.trackingStats.UFGA.total )
    split.derivedStats.DFG_PCT = divide( split.trackingStats.DFGM.total, split.trackingStats.DFGA.total )
  }
end

#jlktodo - make sure time, and total time are saved correctly in gamelog!!!!
#make function to convert splitSet to row[:], and then use it here for team_daily_averages
def calculateDerivedStats( statSet, o_statSet, database, tablename, team_abbreviation, game_id, bTeam ) 
  if true == bTeam
  splits = [ [statSet.split, o_statSet.split] ]
  else
    splits = [ [statSet.split] ]
  end
  if 1 == statSet.away_split.valid
    splits.push( [statSet.away_split, o_statSet.away_split] )
  end
  if 1 == statSet.home_split.valid
    splits.push( [statSet.home_split, o_statSet.home_split] )
  end
  if 1 == statSet.starter_split.valid
    splits.push( [statSet.starter_split, o_statSet.starter_split]  )
  end
  if 1 == statSet.bench_split.valid
    splits.push( [statSet.bench_split, o_statSet.bench_split]  )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( [statSet.total_games_with_rest_split[ i ], o_statSet.total_games_with_rest_split[ i ]] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( [statSet.three_in_four_split, o_statSet.three_in_four_split]  )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( [statSet.four_in_six_split, o_statSet.four_in_six_split]  )
  end

  splits.each{|split_array|
    split = split_array[0]
    if true == bTeam
      o_split = split_array[1]

      team_minutes_played = (split.timeStats.seconds_played.total.to_f / 60) * 5

      split.derivedStats.possessions_total = split.gamelogStats.FGA.total + split.gamelogStats.TOV.total - split.gamelogStats.OREB.total + 0.44 * split.gamelogStats.FTA.total
      o_split.derivedStats.possessions_total = o_split.gamelogStats.FGA.total + o_split.gamelogStats.TOV.total - o_split.gamelogStats.OREB.total + 0.44 * o_split.gamelogStats.FTA.total

      if 0 == (split.gamelogStats.FGA.game_total + split.gamelogStats.TOV.game_total - split.gamelogStats.OREB.game_total + 0.44 * split.gamelogStats.FTA.game_total)
        ortg = 0.0
      else
        ortg = split.gamelogStats.PTS.game_total / (split.gamelogStats.FGA.game_total + split.gamelogStats.TOV.game_total - split.gamelogStats.OREB.game_total + 0.44 * split.gamelogStats.FTA.game_total)
      end
      if 0 == (o_split.gamelogStats.FGA.game_total + o_split.gamelogStats.TOV.game_total - o_split.gamelogStats.OREB.game_total + 0.44 * o_split.gamelogStats.FTA.game_total)
        o_ortg = 0.0
      else
        o_ortg = o_split.gamelogStats.PTS.game_total / (o_split.gamelogStats.FGA.game_total + o_split.gamelogStats.TOV.game_total - o_split.gamelogStats.OREB.game_total + 0.44 * o_split.gamelogStats.FTA.game_total)
      end

      if ( 100*ortg - split.advancedStats.OFF_RATING.game_total ).abs >= 0.2
        #binding.pry
        #p "off error: calc: #{100*ortg} actual: #{split.advancedStats.OFF_RATING.game_total} #{game_id}"
      end

      if ( 100*o_ortg - split.advancedStats.DEF_RATING.game_total ).abs >= 0.2
        #binding.pry
        #p "def error: calc: #{100*o_ortg} actual: #{split.advancedStats.DEF_RATING.game_total} #{game_id}"
      end

      team_total_DREB = split.gamelogStats.DREB.total
      team_total_OREB = split.gamelogStats.OREB.total
      team_total_REB = split.gamelogStats.REB.total
      team_total_FGM = split.gamelogStats.FGM.total
      team_total_FGA = split.gamelogStats.FGA.total
      team_total_FTA = split.gamelogStats.FTA.total
      team_total_TOV = split.gamelogStats.TOV.total
      team_total_o_possessions = o_split.derivedStats.possessions_total
      team_total_o_FG3A = o_split.gamelogStats.FG3A.total
      team_total_o_OREB = o_split.gamelogStats.OREB.total
      team_total_o_DREB = o_split.gamelogStats.DREB.total
      team_total_o_REB = o_split.gamelogStats.REB.total
      team_total_o_FGA = o_split.gamelogStats.FGA.total
      team_total_o_FTA = o_split.gamelogStats.FTA.total
      team_total_TOV = o_split.gamelogStats.TOV.total

      split.derivedStats.OREB_PCT = 100 * ( split.gamelogStats.OREB.total * ( team_minutes_played / 5 ) ) / ( (split.timeStats.seconds_played.total / 60) * ( split.gamelogStats.OREB.total + o_split.gamelogStats.DREB.total ) )
      split.derivedStats.DREB_PCT = 100 * ( split.gamelogStats.DREB.total * ( team_minutes_played / 5 ) ) / ( (split.timeStats.seconds_played.total / 60) * ( split.gamelogStats.DREB.total + o_split.gamelogStats.OREB.total ) )
      split.derivedStats.REB_PCT = 100 * ( split.gamelogStats.REB.total * ( team_minutes_played / 5 ) ) / ( (split.timeStats.seconds_played.total / 60) * ( split.gamelogStats.REB.total + o_split.gamelogStats.REB.total ) )

      split.derivedStats.AST_PCT = divide( 100 * split.gamelogStats.AST.total, team_total_FGM )

      split.derivedStats.OFF_RATING = divide( 100 * split.gamelogStats.PTS.total, split.derivedStats.possessions_total )
      split.derivedStats.DEF_RATING = divide( (100 * o_split.gamelogStats.PTS.total), o_split.derivedStats.possessions_total )
      split.derivedStats.NET_RATING = split.derivedStats.OFF_RATING - split.derivedStats.DEF_RATING

      split.derivedStats.AST_TOV = divide( split.gamelogStats.AST.total, split.gamelogStats.TOV.total )
      split.derivedStats.AST_RATIO = divide( split.gamelogStats.AST.total, ( split.gamelogStats.FGA.total + (split.gamelogStats.FTA.total * 0.44) + split.gamelogStats.AST.total + split.gamelogStats.TOV.total ) )
      split.derivedStats.TO_RATIO = divide( split.gamelogStats.TOV.total, split.derivedStats.possessions_total )
      split.derivedStats.PACE = divide( split.derivedStats.possessions_total, ( (split.timeStats.seconds_played.total.to_f / 60) / (5.0*48.0) ) )
      split.derivedStats.PIE = divide( (split.gamelogStats.PTS.total + split.gamelogStats.FGM.total + split.gamelogStats.FTM.total - split.gamelogStats.FGA.total - split.gamelogStats.FTA.total + split.gamelogStats.DREB.total + (0.5 * split.gamelogStats.OREB.total) + split.gamelogStats.AST.total + split.gamelogStats.STL.total + (0.5 * split.gamelogStats.BLK.total) - split.gamelogStats.PF.total - split.gamelogStats.TOV.total), (split.gamelogStats.PTS.total + o_split.gamelogStats.PTS.total + split.gamelogStats.FGM.total + o_split.gamelogStats.FGM.total + split.gamelogStats.FTM.total + o_split.gamelogStats.FTM.total - split.gamelogStats.FGA.total - o_split.gamelogStats.FGA.total - split.gamelogStats.FTA.total - o_split.gamelogStats.FTA.total + split.gamelogStats.DREB.total + o_split.gamelogStats.DREB.total + (0.5 * (split.gamelogStats.OREB.total + o_split.gamelogStats.OREB.total) ) + split.gamelogStats.AST.total + o_split.gamelogStats.AST.total + split.gamelogStats.STL.total + o_split.gamelogStats.STL.total + (0.5 * (split.gamelogStats.BLK.total + o_split.gamelogStats.BLK.total) ) - split.gamelogStats.PF.total - o_split.gamelogStats.PF.total - split.gamelogStats.TOV.total - o_split.gamelogStats.TOV.total) )

      if 0 == ( (split.timeStats.seconds_played.total.to_f / 60) * ( team_total_OREB + team_total_o_DREB ) )
        binding.pry
        p "hi"
      end

      split.derivedStats.PCT_STL = 100 * (split.gamelogStats.STL.total * (team_minutes_played / 5)) / ( (split.timeStats.seconds_played.total.to_f / 60) * team_total_o_possessions)

      if team_total_o_FGA == team_total_o_FG3A
        split.derivedStats.PCT_BLK = 0
      else
        split.derivedStats.PCT_BLK = 100 * (split.gamelogStats.BLK.total * (team_minutes_played / 5)) / ((split.timeStats.seconds_played.total.to_f / 60) * (team_total_o_FGA - team_total_o_FG3A))
      end

      #OPPONENT STATS - only for teams
      o_split.derivedStats.OREB_PCT = divide( 100 * ( o_split.gamelogStats.OREB.total * ( team_minutes_played / 5 ) ), (split.timeStats.seconds_played.total.to_f / 60) * ( o_split.gamelogStats.OREB.total + split.gamelogStats.DREB.total ) )
      o_split.derivedStats.DREB_PCT = divide( 100 * ( o_split.gamelogStats.DREB.total * ( team_minutes_played / 5 ) ), (split.timeStats.seconds_played.total.to_f / 60) * ( o_split.gamelogStats.DREB.total + split.gamelogStats.OREB.total ) )
      o_split.derivedStats.REB_PCT = divide( 100 * ( o_split.gamelogStats.REB.total * ( team_minutes_played / 5 ) ), (split.timeStats.seconds_played.total.to_f / 60) * ( o_split.gamelogStats.REB.total + split.gamelogStats.REB.total ) )

      o_split.derivedStats.AST_PCT = divide( 100 * o_split.gamelogStats.AST.total, o_split.gamelogStats.FGM.total )
      o_split.derivedStats.PCT_STL = divide( 100 * (o_split.gamelogStats.STL.total * (team_minutes_played / 5)), ((split.timeStats.seconds_played.total.to_f / 60) * split.derivedStats.possessions_total) )
      o_split.derivedStats.PCT_BLK = divide( 100 * (o_split.gamelogStats.BLK.total * (team_minutes_played / 5)), ((split.timeStats.seconds_played.total.to_f / 60) * (split.gamelogStats.FGA.total - split.gamelogStats.FG3A.total))  )

      o_split.derivedStats.USG_PCT = divide( 100 * (o_split.gamelogStats.FGA.total + 0.44 * o_split.gamelogStats.FTA.total + o_split.gamelogStats.TOV.total) * (team_minutes_played / 5), (split.timeStats.seconds_played.total.to_f / 60) * (o_split.gamelogStats.FGA.total + 0.44 * o_split.gamelogStats.FTA.total + o_split.gamelogStats.TOV.total ) )
      o_split.derivedStats.OFF_RATING = divide( (100 * o_split.gamelogStats.PTS.total), o_split.derivedStats.possessions_total )
      o_split.derivedStats.DEF_RATING = divide( (100 * split.gamelogStats.PTS.total), split.derivedStats.possessions_total )
      o_split.derivedStats.NET_RATING = o_split.derivedStats.OFF_RATING - o_split.derivedStats.DEF_RATING
      o_split.derivedStats.AST_TOV = divide( o_split.gamelogStats.AST.total, o_split.gamelogStats.TOV.total )
      o_split.derivedStats.AST_RATIO = divide( o_split.gamelogStats.AST.total, o_split.derivedStats.possessions_total )
      o_split.derivedStats.TO_RATIO = divide( o_split.gamelogStats.TOV.total, o_split.derivedStats.possessions_total )
      o_split.derivedStats.PACE = divide( o_split.derivedStats.possessions_total, (split.timeStats.seconds_played.total.to_f / 60) / (5.0*48.0) )
      o_split.derivedStats.PIE = divide( o_split.gamelogStats.PTS.total + o_split.gamelogStats.FGM.total + o_split.gamelogStats.FTM.total - o_split.gamelogStats.FGA.total - o_split.gamelogStats.FTA.total + o_split.gamelogStats.DREB.total + (0.5 * o_split.gamelogStats.OREB.total) + o_split.gamelogStats.AST.total + o_split.gamelogStats.STL.total + (0.5 * o_split.gamelogStats.BLK.total) - o_split.gamelogStats.PF.total - o_split.gamelogStats.TOV.total, o_split.gamelogStats.PTS.total + split.gamelogStats.PTS.total + o_split.gamelogStats.FGM.total + split.gamelogStats.FGM.total + o_split.gamelogStats.FTM.total + split.gamelogStats.FTM.total - o_split.gamelogStats.FGA.total - split.gamelogStats.FGA.total - o_split.gamelogStats.FTA.total - split.gamelogStats.FTA.total + o_split.gamelogStats.DREB.total + split.gamelogStats.DREB.total + (0.5 * (o_split.gamelogStats.OREB.total + split.gamelogStats.OREB.total) ) + o_split.gamelogStats.AST.total + split.gamelogStats.AST.total + o_split.gamelogStats.STL.total + split.gamelogStats.STL.total + (0.5 * (o_split.gamelogStats.BLK.total + split.gamelogStats.BLK.total) ) - o_split.gamelogStats.PF.total - split.gamelogStats.PF.total - o_split.gamelogStats.TOV.total - split.gamelogStats.TOV.total )

      o_split.derivedStats.CFG_PCT = divide( o_split.trackingStats.CFGM.total.to_f, o_split.trackingStats.CFGA.total.to_f )
      o_split.derivedStats.UFG_PCT = divide( o_split.trackingStats.UFGM.total.to_f, o_split.trackingStats.UFGA.total.to_f )
      o_split.derivedStats.DFG_PCT = divide( o_split.trackingStats.DFGM.total.to_f, o_split.trackingStats.DFGA.total.to_f )
    else # false == bTeam
      team_daily_averages = database[tablename].where(:team_abbreviation => team_abbreviation).where(:game_id => game_id).entries[0]
      split.derivedStats.possessions_total = split.gamelogStats.FGA.total + split.gamelogStats.TOV.total - split.gamelogStats.OREB.total + 0.44 * split.gamelogStats.FTA.total
      begin
      team_minutes_played = team_daily_averages[:seconds_played_total].to_f / 60
      rescue StandardError => e
        binding.pry
        p "hi"
      end

      team_total_DREB = team_daily_averages[:DREB_total] 
      team_total_OREB = team_daily_averages[:OREB_total] 
      team_total_REB = team_daily_averages[:REB_total] 
      team_total_FGM = team_daily_averages[:FGM_total] 
      team_total_FG3M = team_daily_averages[:FG3M_total] 
      team_total_FG3A = team_daily_averages[:FG3A_total] 
      team_total_FGA = team_daily_averages[:FGA_total] 
      team_total_FTA = team_daily_averages[:FTA_total] 
      team_total_FTM = team_daily_averages[:FTM_total] 
      team_total_TOV = team_daily_averages[:TOV_total] 
      team_total_AST = team_daily_averages[:AST_total] 
      team_total_STL = team_daily_averages[:STL_total] 
      team_total_BLK = team_daily_averages[:BLK_total] 
      team_total_PF = team_daily_averages[:PF_total] 
      team_total_PTS = team_daily_averages[:PTS_total] 
      team_total_o_possessions = team_daily_averages[:o_possessions_total] 
      team_total_o_FG3A = team_daily_averages[:o_FG3A_total] 
      team_total_o_OREB = team_daily_averages[:o_OREB_total] 
      team_total_o_DREB = team_daily_averages[:o_DREB_total] 
      team_total_o_REB = team_daily_averages[:o_REB_total] 
      team_total_o_FGA = team_daily_averages[:o_FGA_total] 
      team_total_o_FTA = team_daily_averages[:o_FTA_total] 
      team_total_o_TOV = team_daily_averages[:o_TOV_total] 
      team_total_o_BLK = team_daily_averages[:o_BLK_total] 
      team_total_o_PF = team_daily_averages[:o_PF_total] 

      game_total_PTS = team_daily_averages[:PTS_total] + team_daily_averages[:o_PTS_total]
      game_total_FGM = team_daily_averages[:FGM_total] + team_daily_averages[:o_FGM_total]
      game_total_FTM = team_daily_averages[:FTM_total] + team_daily_averages[:o_FTM_total]
      game_total_FGA = team_daily_averages[:FGA_total] + team_daily_averages[:o_FGA_total]
      game_total_FTA = team_daily_averages[:FTA_total] + team_daily_averages[:o_FTA_total]
      game_total_DREB = team_daily_averages[:DREB_total] + team_daily_averages[:o_DREB_total]
      game_total_OREB = team_daily_averages[:OREB_total] + team_daily_averages[:o_OREB_total]
      game_total_AST = team_daily_averages[:AST_total] + team_daily_averages[:o_AST_total]
      game_total_STL = team_daily_averages[:STL_total] + team_daily_averages[:o_STL_total]
      game_total_BLK = team_daily_averages[:BLK_total] + team_daily_averages[:o_BLK_total]
      game_total_PF = team_daily_averages[:PF_total] + team_daily_averages[:o_PF_total]
      game_total_TOV = team_daily_averages[:TOV_total] + team_daily_averages[:o_TOV_total]

      split.derivedStats.OREB_PCT = divide( split.gamelogStats.OREB.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_OREB + team_total_o_DREB ) )
      split.derivedStats.DREB_PCT = divide( split.gamelogStats.DREB.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_DREB + team_total_o_OREB ) )
      split.derivedStats.REB_PCT = divide( split.gamelogStats.REB.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_REB + team_total_o_REB ) )
      split.derivedStats.PCT_FGM = divide( split.gamelogStats.FGM.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_FGM ) )
      split.derivedStats.PCT_FGA = divide( split.gamelogStats.FGA.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_FGA ) )
      split.derivedStats.PCT_FG3M = divide( split.gamelogStats.FG3M.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_FG3M ) )
      split.derivedStats.PCT_FG3A = divide( split.gamelogStats.FG3A.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_FG3A ) )
      split.derivedStats.PCT_FTM = divide( split.gamelogStats.FTM.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_FTM ) )
      split.derivedStats.PCT_FTA = divide( split.gamelogStats.FTA.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_FTA ) )
      split.derivedStats.PCT_OREB = divide( split.gamelogStats.OREB.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_OREB ) )
      split.derivedStats.PCT_DREB = divide( split.gamelogStats.DREB.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_DREB ) )
      split.derivedStats.PCT_REB = divide( split.gamelogStats.REB.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_REB ) )
      split.derivedStats.PCT_AST = divide( split.gamelogStats.AST.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_AST ) )
      split.derivedStats.PCT_TOV = divide( split.gamelogStats.TOV.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_TOV ) )
      split.derivedStats.PCT_STL = divide( split.gamelogStats.STL.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_STL ) )
      split.derivedStats.PCT_BLK = divide( split.gamelogStats.BLK.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_BLK ) )
      split.derivedStats.PCT_BLKA = divide( split.miscStats.BLKA.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_o_BLK ) )
      split.derivedStats.PCT_PF = divide( split.gamelogStats.PF.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_PF ) )
      split.derivedStats.PCT_PFD = divide( split.miscStats.PFD.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_o_PF ) )
      split.derivedStats.PCT_PTS = divide( split.gamelogStats.PTS.total * ( team_minutes_played / 5 ), (split.timeStats.seconds_played.total / 60) * ( team_total_PTS ) )
      #binding.pry
=begin
      split.derivedStats.OREB_PCT = 100 * ( split.gamelogStats.OREB.total ) / ( split.usageStats.team_OREB.total + split.usageStats.o_team_DREB.total )
      split.derivedStats.DREB_PCT = 100 * ( split.gamelogStats.DREB.total ) / ( split.usageStats.team_DREB.total + split.usageStats.o_team_OREB.total )
      split.derivedStats.REB_PCT = 100 * ( split.gamelogStats.REB.total ) / ( split.usageStats.team_REB.total + split.usageStats.o_team_REB.total )
=end
      if 0 == ( split.usageStats.team_FGM.total - split.gamelogStats.FGM.total)
        split.derivedStats.AST_PCT = 0.0
      else
        split.derivedStats.AST_PCT = ( 100 * split.gamelogStats.AST.total ) / ( split.usageStats.team_FGM.total - split.gamelogStats.FGM.total)
      end

      if 0 == split.gamelogStats.TOV.total
        split.derivedStats.AST_TOV = -1 #jlk? sentinel value for n/a
      else
        split.derivedStats.AST_TOV = split.gamelogStats.AST.total / split.gamelogStats.TOV.total
      end

      if 0 == (split.gamelogStats.FGA.total + (split.gamelogStats.FTA.total * 0.44) + split.gamelogStats.AST.total + split.gamelogStats.TOV.total)
        split.derivedStats.AST_RATIO = 0
      else
        split.derivedStats.AST_RATIO = split.gamelogStats.AST.total / (split.gamelogStats.FGA.total + (split.gamelogStats.FTA.total * 0.44) + split.gamelogStats.AST.total + split.gamelogStats.TOV.total)
      end

      if 0 == (split.gamelogStats.FGA.total + (split.gamelogStats.FTA.total * 0.44) + split.gamelogStats.AST.total + split.gamelogStats.TOV.total)
        split.derivedStats.TO_RATIO = 0
      else
        split.derivedStats.TO_RATIO = split.gamelogStats.TOV.total / (split.gamelogStats.FGA.total + (split.gamelogStats.FTA.total * 0.44) + split.gamelogStats.AST.total + split.gamelogStats.TOV.total)
      end

=begin

      if 0 == split.usageStats.PCT_PTS.game_total
        team_offensive_points = 0.0
      else
        team_offensive_points = ( ( split.gamelogStats.PTS.game_total) / split.usageStats.PCT_PTS.game_total).round
      end
      team_defensive_points = team_offensive_points - split.gamelogStats.PLUS_MINUS.game_total

      split.derivedStats.team_offensive_points_total = split.derivedStats.team_offensive_points_total + team_offensive_points
      split.derivedStats.team_defensive_points_total = split.derivedStats.team_offensive_points_total + team_defensive_points

      if 0 == split.advancedStats.OFF_RATING.game_total
        offensive_possessions = 0.0
      else
        offensive_possessions = (100 * team_offensive_points) / split.advancedStats.OFF_RATING.game_total
      end
      split.derivedStats.offensive_possessions_total = split.derivedStats.offensive_possessions_total + offensive_possessions

      if 0 == split.advancedStats.DEF_RATING.game_total
        defensive_possessions = 0.0
      else
        defensive_possessions = (100 * split.gamelogStats.team_defensive_PTS.game_total) / split.advancedStats.DEF_RATING.game_total
      end
      split.derivedStats.defensive_possessions_total = split.derivedStats.defensive_possessions_total + defensive_possessions
=end
      if 0 == split.usageStats.offensive_possessions.total
        split.derivedStats.OFF_RATING = 0
      else
        split.derivedStats.OFF_RATING = (100 * split.usageStats.team_offensive_PTS.total) / split.usageStats.offensive_possessions.total
      end

      if 0 == split.usageStats.defensive_possessions.total
        split.derivedStats.DEF_RATING = 0
      else
        split.derivedStats.DEF_RATING = (100 * split.usageStats.team_defensive_PTS.total) / split.usageStats.defensive_possessions.total
      end

      split.derivedStats.NET_RATING = split.derivedStats.OFF_RATING - split.derivedStats.DEF_RATING

      #split.derivedStats.USG_PCT = (split.gamelogStats.FGA.total + (0.44 * split.gamelogStats.FTA.total) + split.gamelogStats.TOV.total) / split.usageStats.offensive_possessions.total
      #p "USG_PCT1: #{split.derivedStats.USG_PCT}"
      split.derivedStats.USG_PCT = divide( split.gamelogStats.FGA.total + (0.44 * split.gamelogStats.FTA.total) + split.gamelogStats.TOV.total, split.usageStats.usage_offensive_possessions.total )
      split.derivedStats.USG_PCT_minus_TOV = divide( split.gamelogStats.FGA.total + (0.44 * split.gamelogStats.FTA.total), split.usageStats.usage_offensive_possessions.total )
      #p "USG_PCT2: #{split.derivedStats.USG_PCT}"

      split.derivedStats.PACE = divide( split.usageStats.offensive_possessions.total, ( (split.timeStats.seconds_played.total.to_f / 60) / (48.0) ) )

      split.derivedStats.PIE = divide( split.gamelogStats.PTS.total + split.gamelogStats.FGM.total + split.gamelogStats.FTM.total - split.gamelogStats.FGA.total - split.gamelogStats.FTA.total + split.gamelogStats.DREB.total + (0.5 * split.gamelogStats.OREB.total) + split.gamelogStats.AST.total + split.gamelogStats.STL.total + (0.5 * split.gamelogStats.BLK.total) - split.gamelogStats.PF.total - split.gamelogStats.TOV.total, game_total_PTS + game_total_FGM + game_total_FTM - game_total_FGA - game_total_FTA + game_total_DREB + (0.5 * game_total_OREB) + game_total_AST + game_total_STL + (0.5 * game_total_BLK) - game_total_PF - game_total_TOV )
=begin
      if 0 == split.usageStats.team_FGM.total
        split.derivedStats.PCT_FGM = 0.0
      else
        split.derivedStats.PCT_FGM = (split.gamelogStats.FGM.total / split.usageStats.team_FGM.total).to_f
      end
      if 0 == split.usageStats.team_FGA.total
        split.derivedStats.PCT_FGA = 0.0
      else
        split.derivedStats.PCT_FGA = (split.gamelogStats.FGA.total / split.usageStats.team_FGA.total).to_f
      end
      if 0 == split.usageStats.team_FG3A.total
        split.derivedStats.PCT_FG3A = 0.0
      else
        split.derivedStats.PCT_FG3A = (split.gamelogStats.FG3A.total / split.usageStats.team_FG3A.total).to_f
      end
      if 0 == split.usageStats.team_FTM.total
        split.derivedStats.PCT_FTM = 0.0
      else
        split.derivedStats.PCT_FTM = (split.gamelogStats.FTM.total / split.usageStats.team_FTM.total).to_f
      end
      if 0 == split.usageStats.team_FTA.total
        split.derivedStats.PCT_FTA = 0.0
      else
        split.derivedStats.PCT_FTA = (split.gamelogStats.FTA.total / split.usageStats.team_FTA.total).to_f
      end
      if 0 == split.usageStats.team_OREB.total
        split.derivedStats.PCT_OREB = 0.0
      else
        split.derivedStats.PCT_OREB = (split.gamelogStats.OREB.total / split.usageStats.team_OREB.total).to_f
      end
      if 0 == split.usageStats.team_DREB.total
        split.derivedStats.PCT_DREB = 0.0
      else
        split.derivedStats.PCT_DREB = (split.gamelogStats.DREB.total / split.usageStats.team_DREB.total).to_f
      end
      if 0 == split.usageStats.team_REB.total
        split.derivedStats.PCT_REB = 0.0
      else
        split.derivedStats.PCT_REB = (split.gamelogStats.REB.total / split.usageStats.team_REB.total).to_f
      end
      if 0 == split.usageStats.team_AST.total
        split.derivedStats.PCT_AST = 0.0
      else
        split.derivedStats.PCT_AST = (split.gamelogStats.AST.total / split.usageStats.team_AST.total).to_f
      end
      if 0 == split.usageStats.team_TOV.total
        split.derivedStats.PCT_TOV = 0.0
      else
        split.derivedStats.PCT_TOV = (split.gamelogStats.TOV.total / split.usageStats.team_TOV.total).to_f
      end
      if 0 == split.usageStats.team_STL.total
        split.derivedStats.PCT_STL = 0.0
      else
        split.derivedStats.PCT_STL = (split.gamelogStats.STL.total / split.usageStats.team_STL.total).to_f
      end
      if 0 == split.usageStats.team_BLK.total
        split.derivedStats.PCT_BLK = 0.0
      else
        split.derivedStats.PCT_BLK = (split.gamelogStats.BLK.total / split.usageStats.team_BLK.total).to_f
      end
      if 0 == split.usageStats.team_PF.total
        split.derivedStats.PCT_PF = 0.0
      else
        split.derivedStats.PCT_PF = (split.gamelogStats.PF.total / split.usageStats.team_PF.total).to_f
      end
      if 0 == split.usageStats.team_offensive_PTS.total
        split.derivedStats.PCT_PTS = 0.0
      else
        split.derivedStats.PCT_PTS = (split.gamelogStats.PTS.total / split.usageStats.team_offensive_PTS.total).to_f
      end
      
      if 0 == team_total_o_BLK
        split.derivedStats.PCT_BLKA = 0.0
      else
        split.derivedStats.PCT_BLKA = (split.miscStats.BLKA.total / team_total_o_BLK).to_f
      end

      if 0 == team_total_o_PF
        split.derivedStats.PCT_PFD = 0.0
      else
        split.derivedStats.PCT_PFD = (split.miscStats.PFD.total / team_total_o_PF).to_f
      end
=end
    end

      if 0 == split.derivedStats.possessions_total
        split.derivedStats.TO_PCT = 0
      else
        split.derivedStats.TO_PCT = 100 * divide( split.gamelogStats.TOV.total, split.derivedStats.possessions_total )
      end

      split.derivedStats.CFG_PCT = divide( split.trackingStats.CFGM.total.to_f, split.trackingStats.CFGA.total.to_f )
      split.derivedStats.UFG_PCT = divide( split.trackingStats.UFGM.total.to_f, split.trackingStats.UFGA.total.to_f )
      split.derivedStats.DFG_PCT = divide( split.trackingStats.DFGM.total.to_f, split.trackingStats.DFGA.total.to_f )

    #jlk - go through usage, fourfactors, scoring, shooting, tracking etc. and make sure all the stats are aggregated here
  }
end

def calculatePerMinStats( statSet, o_statSet, bTeam ) 
  if true == bTeam
    splits = [ [statSet.split, o_statSet.split] ]
  else
    splits = [ [statSet.split] ]
  end
  if 1 == statSet.away_split.valid
    splits.push( [statSet.away_split, o_statSet.away_split] )
  end
  if 1 == statSet.home_split.valid
    splits.push( [statSet.home_split, o_statSet.home_split] )
  end
  if 1 == statSet.starter_split.valid
    splits.push( [statSet.starter_split, o_statSet.starter_split]  )
  end
  if 1 == statSet.bench_split.valid
    splits.push( [statSet.bench_split, o_statSet.bench_split]  )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( [statSet.total_games_with_rest_split[ i ], o_statSet.total_games_with_rest_split[ i ]] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( [statSet.three_in_four_split, o_statSet.three_in_four_split]  )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( [statSet.four_in_six_split, o_statSet.four_in_six_split]  )
  end

  splits.each{|split_array|
    split = split_array[0]
    if true == bTeam
      o_split = split_array[1]

      total_minutes = split.timeStats.seconds_played.total.to_f / (60 * 5)

      #OPPONENT STATS - only for teams
      o_split.perMinStats.FGM_per_min = divide( o_split.gamelogStats.FGM.total, total_minutes )
      o_split.perMinStats.FGA_per_min = divide( o_split.gamelogStats.FGA.total, total_minutes )
      o_split.perMinStats.FG2M_per_min = divide( o_split.gamelogStats.FG2M.total, total_minutes )
      o_split.perMinStats.FG2A_per_min = divide( o_split.gamelogStats.FG2A.total, total_minutes )
      o_split.perMinStats.FG3M_per_min = divide( o_split.gamelogStats.FG3M.total, total_minutes )
      o_split.perMinStats.FG3A_per_min = divide( o_split.gamelogStats.FG3A.total, total_minutes )
      o_split.perMinStats.FTM_per_min = divide( o_split.gamelogStats.FTM.total, total_minutes )
      o_split.perMinStats.FTA_per_min = divide( o_split.gamelogStats.FTA.total, total_minutes )
      o_split.perMinStats.PTS_per_min = divide( o_split.gamelogStats.PTS.total, total_minutes )
      o_split.perMinStats.AST_2PM_per_min = divide( o_split.scoringDerivedStats.AST_2PM.total, total_minutes )
      o_split.perMinStats.UAST_2PM_per_min = divide( o_split.scoringDerivedStats.UAST_2PM.total, total_minutes )
      o_split.perMinStats.AST_3PM_per_min = divide( o_split.scoringDerivedStats.AST_3PM.total, total_minutes )
      o_split.perMinStats.UAST_3PM_per_min = divide( o_split.scoringDerivedStats.UAST_3PM.total, total_minutes )
      o_split.perMinStats.AST_FGM_per_min = divide( o_split.scoringDerivedStats.AST_FGM.total, total_minutes )
      o_split.perMinStats.UAST_FGM_per_min = divide( o_split.scoringDerivedStats.UAST_FGM.total, total_minutes )
      o_split.perMinStats.PTS_OFF_TOV_per_min = divide( o_split.miscStats.PTS_OFF_TOV.total, total_minutes )
      o_split.perMinStats.PTS_2ND_CHANCE_per_min = divide( o_split.miscStats.PTS_2ND_CHANCE.total, total_minutes )
      o_split.perMinStats.PTS_FB_per_min = divide( o_split.miscStats.PTS_FB.total, total_minutes )
      o_split.perMinStats.PTS_PAINT_per_min = divide( o_split.miscStats.PTS_PAINT.total, total_minutes )
      o_split.perMinStats.OREB_per_min = divide( o_split.gamelogStats.OREB.total, total_minutes )
      o_split.perMinStats.DREB_per_min = divide( o_split.gamelogStats.DREB.total, total_minutes )
      o_split.perMinStats.REB_per_min = divide( o_split.gamelogStats.REB.total, total_minutes )
      o_split.perMinStats.AST_per_min = divide( o_split.gamelogStats.AST.total, total_minutes )
      o_split.perMinStats.STL_per_min = divide( o_split.gamelogStats.STL.total, total_minutes )
      o_split.perMinStats.BLK_per_min = divide( o_split.gamelogStats.BLK.total, total_minutes )
      o_split.perMinStats.TOV_per_min = divide( o_split.gamelogStats.TOV.total, total_minutes )
      o_split.perMinStats.PF_per_min = divide( o_split.gamelogStats.PF.total, total_minutes )
      o_split.perMinStats.PLUS_MINUS_per_min = divide( o_split.gamelogStats.PLUS_MINUS.total, total_minutes )
      o_split.perMinStats.BLKA_per_min = divide( o_split.miscStats.BLKA.total, total_minutes )
      o_split.perMinStats.PFD_per_min = divide( o_split.miscStats.PFD.total, total_minutes )
      o_split.perMinStats.DIST_per_min = divide( o_split.trackingStats.DIST.total, total_minutes )
      o_split.perMinStats.ORBC_per_min = divide( o_split.trackingStats.ORBC.total, total_minutes )
      o_split.perMinStats.DRBC_per_min = divide( o_split.trackingStats.DRBC.total, total_minutes )
      o_split.perMinStats.RBC_per_min = divide( o_split.trackingStats.RBC.total, total_minutes )
      o_split.perMinStats.TCHS_per_min = divide( o_split.trackingStats.TCHS.total, total_minutes )
      o_split.perMinStats.SAST_per_min = divide( o_split.trackingStats.SAST.total, total_minutes )
      o_split.perMinStats.FTAST_per_min = divide( o_split.trackingStats.FTAST.total, total_minutes )
      o_split.perMinStats.PASS_per_min = divide( o_split.trackingStats.PASS.total, total_minutes )
      o_split.perMinStats.CFGM_per_min = divide( o_split.trackingStats.CFGM.total, total_minutes )
      o_split.perMinStats.CFGA_per_min = divide( o_split.trackingStats.CFGA.total, total_minutes )
      o_split.perMinStats.UFGM_per_min = divide( o_split.trackingStats.UFGM.total, total_minutes )
      o_split.perMinStats.UFGA_per_min = divide( o_split.trackingStats.UFGA.total, total_minutes )
      o_split.perMinStats.DFGM_per_min = divide( o_split.trackingStats.DFGM.total, total_minutes )
      o_split.perMinStats.DFGA_per_min = divide( o_split.trackingStats.DFGA.total, total_minutes )

    else # false == bTeam
      total_minutes = split.timeStats.seconds_played.total.to_f / 60.0
    end

      split.perMinStats.FGM_per_min = divide( split.gamelogStats.FGM.total, total_minutes )
      split.perMinStats.FGA_per_min = divide( split.gamelogStats.FGA.total, total_minutes )
      split.perMinStats.FG2M_per_min = divide( split.gamelogStats.FG2M.total, total_minutes )
      split.perMinStats.FG2A_per_min = divide( split.gamelogStats.FG2A.total, total_minutes )
      split.perMinStats.FG3M_per_min = divide( split.gamelogStats.FG3M.total, total_minutes )
      split.perMinStats.FG3A_per_min = divide( split.gamelogStats.FG3A.total, total_minutes )
      split.perMinStats.FTM_per_min = divide( split.gamelogStats.FTM.total, total_minutes )
      split.perMinStats.FTA_per_min = divide( split.gamelogStats.FTA.total, total_minutes )
      split.perMinStats.PTS_per_min = divide( split.gamelogStats.PTS.total, total_minutes )
      split.perMinStats.AST_2PM_per_min = divide( split.scoringDerivedStats.AST_2PM.total, total_minutes )
      split.perMinStats.UAST_2PM_per_min = divide( split.scoringDerivedStats.UAST_2PM.total, total_minutes )
      split.perMinStats.AST_3PM_per_min = divide( split.scoringDerivedStats.AST_3PM.total, total_minutes )
      split.perMinStats.UAST_3PM_per_min = divide( split.scoringDerivedStats.UAST_3PM.total, total_minutes )
      split.perMinStats.AST_FGM_per_min = divide( split.scoringDerivedStats.AST_FGM.total, total_minutes )
      split.perMinStats.UAST_FGM_per_min = divide( split.scoringDerivedStats.UAST_FGM.total, total_minutes )
      split.perMinStats.PTS_OFF_TOV_per_min = divide( split.miscStats.PTS_OFF_TOV.total, total_minutes )
      split.perMinStats.PTS_2ND_CHANCE_per_min = divide( split.miscStats.PTS_2ND_CHANCE.total, total_minutes )
      split.perMinStats.PTS_FB_per_min = divide( split.miscStats.PTS_FB.total, total_minutes )
      split.perMinStats.PTS_PAINT_per_min = divide( split.miscStats.PTS_PAINT.total, total_minutes )
      split.perMinStats.OREB_per_min = divide( split.gamelogStats.OREB.total, total_minutes )
      split.perMinStats.DREB_per_min = divide( split.gamelogStats.DREB.total, total_minutes )
      split.perMinStats.REB_per_min = divide( split.gamelogStats.REB.total, total_minutes )
      split.perMinStats.AST_per_min = divide( split.gamelogStats.AST.total, total_minutes )
      split.perMinStats.STL_per_min = divide( split.gamelogStats.STL.total, total_minutes )
      split.perMinStats.BLK_per_min = divide( split.gamelogStats.BLK.total, total_minutes )
      split.perMinStats.TOV_per_min = divide( split.gamelogStats.TOV.total, total_minutes )
      split.perMinStats.PF_per_min = divide( split.gamelogStats.PF.total, total_minutes )
      split.perMinStats.PLUS_MINUS_per_min = divide( split.gamelogStats.PLUS_MINUS.total, total_minutes )
      split.perMinStats.BLKA_per_min = divide( split.miscStats.BLKA.total, total_minutes )
      split.perMinStats.PFD_per_min = divide( split.miscStats.PFD.total, total_minutes )
      split.perMinStats.DIST_per_min = divide( split.trackingStats.DIST.total, total_minutes )
      split.perMinStats.ORBC_per_min = divide( split.trackingStats.ORBC.total, total_minutes )
      split.perMinStats.DRBC_per_min = divide( split.trackingStats.DRBC.total, total_minutes )
      split.perMinStats.RBC_per_min = divide( split.trackingStats.RBC.total, total_minutes )
      split.perMinStats.TCHS_per_min = divide( split.trackingStats.TCHS.total, total_minutes )
      split.perMinStats.SAST_per_min = divide( split.trackingStats.SAST.total, total_minutes )
      split.perMinStats.FTAST_per_min = divide( split.trackingStats.FTAST.total, total_minutes )
      split.perMinStats.PASS_per_min = divide( split.trackingStats.PASS.total, total_minutes )
      split.perMinStats.CFGM_per_min = divide( split.trackingStats.CFGM.total, total_minutes )
      split.perMinStats.CFGA_per_min = divide( split.trackingStats.CFGA.total, total_minutes )
      split.perMinStats.UFGM_per_min = divide( split.trackingStats.UFGM.total, total_minutes )
      split.perMinStats.UFGA_per_min = divide( split.trackingStats.UFGA.total, total_minutes )
      split.perMinStats.DFGM_per_min = divide( split.trackingStats.DFGM.total, total_minutes )
      split.perMinStats.DFGA_per_min = divide( split.trackingStats.DFGA.total, total_minutes )
  }

end

def calculateAdvancedStats( boxscore, statSet )
  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    calculateAverages2( boxscore, :TS_PCT, split.advancedStats.TS_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :EFG_PCT, split.advancedStats.EFG_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :OREB_PCT, split.advancedStats.OREB_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :DREB_PCT, split.advancedStats.DREB_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :REB_PCT, split.advancedStats.REB_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :AST_PCT, split.advancedStats.AST_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :USG_PCT, split.advancedStats.USG_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :OFF_RATING, split.advancedStats.OFF_RATING, split.timeStats.games_played )
    calculateAverages2( boxscore, :DEF_RATING, split.advancedStats.DEF_RATING, split.timeStats.games_played )
    calculateAverages2( boxscore, :NET_RATING, split.advancedStats.NET_RATING, split.timeStats.games_played )
    calculateAverages2( boxscore, :AST_TOV, split.advancedStats.AST_TOV, split.timeStats.games_played )
    calculateAverages2( boxscore, :AST_RATIO, split.advancedStats.AST_RATIO, split.timeStats.games_played )
    calculateAverages2( boxscore, :PACE, split.advancedStats.PACE, split.timeStats.games_played )
    calculateAverages2( boxscore, :PIE, split.advancedStats.PIE, split.timeStats.games_played )

    split.advancedStats.TO_PCT.game_total = boxscore[:TM_TOV_PCT].to_f
    split.advancedStats.TO_PCT.total, split.advancedStats.TO_PCT.mean = mean( boxscore[:TM_TOV_PCT].to_f, split.advancedStats.TO_PCT.total, split.timeStats.games_played )
    
    if nil == split.gamelogStats.game_id
      binding.pry
      p "hi"
    end
    split.advancedStats.TO_PCT.hash[ split.gamelogStats.game_id ] = boxscore[:TM_TOV_PCT].to_f
    split.advancedStats.TO_PCT.median = median( split.advancedStats.TO_PCT.hash )

  }

end

def calculateScoringStats( boxscore, gamelog, statSet )
  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    #calculateAverages2( boxscore, :PCT_FGA_2PT, split.scoringStats.PCT_FGA_2PT, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_FGA_3PT, split.scoringStats.PCT_FGA_3PT, split.timeStats.games_played )
    #jlk - save FGA_2PT total, mean, median

=begin
    trial_value = (boxscore[:PCT_FGA_2PT].to_f * gamelog[:FGA].to_f).round.to_f
    if gamelog[:FGA].to_f > 0 and ( trial_value / gamelog[:FGA].to_f ).round(3) != boxscore[:PCT_FGA_2PT].to_f
        binding.pry
      p "gamelog[:FGA].to_f: #{gamelog[:FGA].to_f} trial_value: #{trial_value} PCT_FGA_2PT: #{boxscore[:PCT_FGA_2PT].to_f} #{boxscore}"
    end

    calculateAverages3( boxscore, trial_value, split.scoringStats.FGA_2PT, split.timeStats.games_played )
=end
    calculateAverages2( boxscore, :PCT_PTS_2PT, split.scoringStats.PCT_PTS_2PT, split.timeStats.games_played )

=begin
    trial_value = (boxscore[:PCT_PTS_2PT].to_f * gamelog[:PTS].to_f).round.to_f
    if gamelog[:PTS].to_f > 0 and ( trial_value / gamelog[:PTS].to_f ).round(3) != boxscore[:PCT_PTS_2PT].to_f
      #binding.pry
      p "gamelog[:PTS].to_f: #{gamelog[:PTS].to_f} trial_value: #{trial_value} PCT_PTS_2PT: #{boxscore[:PCT_PTS_2PT].to_f}"
    end
    calculateAverages3( boxscore, trial_value, split.scoringStats.PTS_2PT, split.timeStats.games_played )
=end
    calculateAverages2( boxscore, :PCT_PTS_2PT_MR, split.scoringStats.PCT_PTS_2PT_MR, split.timeStats.games_played )

    trial_value = (boxscore[:PCT_PTS_2PT_MR].to_f * gamelog[:PTS].to_f).round.to_f
    if gamelog[:PTS].to_f > 0 and ( ( trial_value / gamelog[:PTS].to_f ).round(3) - boxscore[:PCT_PTS_2PT_MR].to_f ) > 0.01
      #binding.pry
      p "trial_value/gamelog[:PTS].to_f: #{(trial_value/gamelog[:PTS].to_f).round(3)} trial_value: #{trial_value} PCT_PTS_2PT_MR: #{boxscore[:PCT_PTS_2PT_MR].to_f}"
    end
    calculateAverages3( boxscore, trial_value, split.scoringStats.PTS_2PT_MR, split.timeStats.games_played )

    calculateAverages2( boxscore, :PCT_PTS_3PT, split.scoringStats.PCT_PTS_3PT, split.timeStats.games_played )

=begin
    trial_value = (boxscore[:PCT_PTS_3PT].to_f * gamelog[:PTS].to_f).round.to_f
    if gamelog[:PTS].to_f > 0 and ( trial_value / gamelog[:PTS].to_f ).round(3) != boxscore[:PCT_PTS_3PT].to_f
      #binding.pry
      p "gamelog[:PTS].to_f: #{gamelog[:PTS].to_f} trial_value: #{trial_value} PCT_PTS_3PT: #{boxscore[:PCT_PTS_3PT].to_f}"
    end
    calculateAverages3( boxscore, trial_value, split.scoringStats.PTS_3PT, split.timeStats.games_played )
=end
    calculateAverages2( boxscore, :PCT_PTS_FB, split.scoringStats.PCT_PTS_FB, split.timeStats.games_played )

=begin
    trial_value = (boxscore[:PCT_PTS_FB].to_f * gamelog[:PTS].to_f).round.to_f
    if gamelog[:PTS].to_f > 0 and ( trial_value / gamelog[:PTS].to_f ).round(3) != boxscore[:PCT_PTS_FB].to_f
      #binding.pry
      p "gamelog[:PTS].to_f: #{gamelog[:PTS].to_f} trial_value: #{trial_value} PCT_PTS_FB: #{boxscore[:PCT_PTS_FB].to_f}"
    end
    calculateAverages3( boxscore, trial_value, split.scoringStats.PTS_FB, split.timeStats.games_played )
=end
    calculateAverages2( boxscore, :PCT_PTS_FT, split.scoringStats.PCT_PTS_FT, split.timeStats.games_played )

=begin
    trial_value = (boxscore[:PCT_PTS_FT].to_f * gamelog[:PTS].to_f).round.to_f
    if gamelog[:PTS].to_f > 0 and ( trial_value / gamelog[:PTS].to_f ).round(3) != boxscore[:PCT_PTS_FT].to_f
      #binding.pry
      p "gamelog[:PTS].to_f: #{gamelog[:PTS].to_f} trial_value: #{trial_value} PCT_PTS_FT: #{boxscore[:PCT_PTS_FT].to_f}"
    end
    calculateAverages3( boxscore, trial_value, split.scoringStats.PTS_FT, split.timeStats.games_played )
=end
    calculateAverages2( boxscore, :PCT_PTS_OFF_TOV, split.scoringStats.PCT_PTS_OFF_TOV, split.timeStats.games_played )

=begin
    trial_value = (boxscore[:PCT_PTS_OFF_TOV].to_f * gamelog[:PTS].to_f).round.to_f
    if gamelog[:PTS].to_f > 0 and ( trial_value / gamelog[:PTS].to_f ).round(3) != boxscore[:PCT_PTS_OFF_TOV].to_f
      #binding.pry
      p "gamelog[:PTS].to_f: #{gamelog[:PTS].to_f} trial_value: #{trial_value} PCT_PTS_OFF_TOV: #{boxscore[:PCT_PTS_OFF_TOV].to_f}"
    end
    calculateAverages3( boxscore, trial_value, split.scoringStats.PTS_OFF_TOV, split.timeStats.games_played )
=end
    calculateAverages2( boxscore, :PCT_PTS_PAINT, split.scoringStats.PCT_PTS_PAINT, split.timeStats.games_played )

=begin
    trial_value = (boxscore[:PCT_PTS_PAINT].to_f * gamelog[:PTS].to_f).round.to_f
    if gamelog[:PTS].to_f > 0 and ( trial_value / gamelog[:PTS].to_f ).round(3) != boxscore[:PCT_PTS_PAINT].to_f
      #binding.pry
      p "gamelog[:PTS].to_f: #{gamelog[:PTS].to_f} trial_value: #{trial_value} PCT_PTS_PAINT: #{boxscore[:PCT_PTS_PAINT].to_f}"
    end
    calculateAverages3( boxscore, trial_value, split.scoringStats.PTS_PAINT, split.timeStats.games_played )
=end
  }
end

def calculateMiscStats( boxscore, statSet, bTeam )
  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    calculateAverages2( boxscore, :PTS_OFF_TOV, split.miscStats.PTS_OFF_TOV, split.timeStats.games_played )
    if false == bTeam
      0 == 0
    end
    calculateAverages2( boxscore, :PTS_2ND_CHANCE, split.miscStats.PTS_2ND_CHANCE, split.timeStats.games_played )
    calculateAverages2( boxscore, :PTS_FB, split.miscStats.PTS_FB, split.timeStats.games_played )
    calculateAverages2( boxscore, :PTS_PAINT, split.miscStats.PTS_PAINT, split.timeStats.games_played )
    #calculateAverages2( boxscore, :PTS_2PT_MR, split.miscStats.PTS_2PT_MR, split.timeStats.games_played )
    calculateAverages2( boxscore, :BLKA, split.miscStats.BLKA, split.timeStats.games_played )
    calculateAverages2( boxscore, :PFD, split.miscStats.PFD, split.timeStats.games_played )

    #if false == bTeam
      calculateAverages2( boxscore, :OPP_PTS_OFF_TOV, split.miscStats.o_PTS_OFF_TOV, split.timeStats.games_played )
      calculateAverages2( boxscore, :OPP_PTS_2ND_CHANCE, split.miscStats.o_PTS_2ND_CHANCE, split.timeStats.games_played )
      calculateAverages2( boxscore, :OPP_PTS_FB, split.miscStats.o_PTS_FB, split.timeStats.games_played )
      calculateAverages2( boxscore, :OPP_PTS_PAINT, split.miscStats.o_PTS_PAINT, split.timeStats.games_played )
    #end
  }

end

def calculateScoringDerivedStats( boxscore, statSet )
  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    calculateAverages2( boxscore, :AST_2PM, split.scoringDerivedStats.AST_2PM, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_AST_2PM, split.scoringDerivedStats.PCT_AST_2PM, split.timeStats.games_played )
    calculateAverages2( boxscore, :UAST_2PM, split.scoringDerivedStats.UAST_2PM, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_UAST_2PM, split.scoringDerivedStats.PCT_UAST_2PM, split.timeStats.games_played )
    calculateAverages2( boxscore, :AST_3PM, split.scoringDerivedStats.AST_3PM, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_AST_3PM, split.scoringDerivedStats.PCT_AST_3PM, split.timeStats.games_played )
    calculateAverages2( boxscore, :UAST_3PM, split.scoringDerivedStats.UAST_3PM, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_UAST_3PM, split.scoringDerivedStats.PCT_UAST_3PM, split.timeStats.games_played )
    calculateAverages2( boxscore, :AST_FGM, split.scoringDerivedStats.AST_FGM, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_AST_FGM, split.scoringDerivedStats.PCT_AST_FGM, split.timeStats.games_played )
    calculateAverages2( boxscore, :UAST_FGM, split.scoringDerivedStats.UAST_FGM, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_UAST_FGM, split.scoringDerivedStats.PCT_UAST_FGM, split.timeStats.games_played )
  }
end

def calculateTrackingStats( boxscore, statSet )
  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    calculateAverages2( boxscore, :DIST, split.trackingStats.DIST, split.timeStats.games_played )
    calculateAverages2( boxscore, :ORBC, split.trackingStats.ORBC, split.timeStats.games_played )
    calculateAverages2( boxscore, :DRBC, split.trackingStats.DRBC, split.timeStats.games_played )
    calculateAverages2( boxscore, :RBC, split.trackingStats.RBC, split.timeStats.games_played )
    calculateAverages2( boxscore, :TCHS, split.trackingStats.TCHS, split.timeStats.games_played )
    calculateAverages2( boxscore, :SAST, split.trackingStats.SAST, split.timeStats.games_played )
    calculateAverages2( boxscore, :FTAST, split.trackingStats.FTAST, split.timeStats.games_played )
    calculateAverages2( boxscore, :PASS, split.trackingStats.PASS, split.timeStats.games_played )
    calculateAverages2( boxscore, :AST, split.trackingStats.AST, split.timeStats.games_played )
    calculateAverages2( boxscore, :CFGM, split.trackingStats.CFGM, split.timeStats.games_played )
    calculateAverages2( boxscore, :CFGA, split.trackingStats.CFGA, split.timeStats.games_played )
    calculateAverages2( boxscore, :CFG_PCT, split.trackingStats.CFG_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :UFGM, split.trackingStats.UFGM, split.timeStats.games_played )
    calculateAverages2( boxscore, :UFGA, split.trackingStats.UFGA, split.timeStats.games_played )
    calculateAverages2( boxscore, :UFG_PCT, split.trackingStats.UFG_PCT, split.timeStats.games_played )
    calculateAverages2( boxscore, :DFGM, split.trackingStats.DFGM, split.timeStats.games_played )
    calculateAverages2( boxscore, :DFGA, split.trackingStats.DFGA, split.timeStats.games_played )
    calculateAverages2( boxscore, :DFG_PCT, split.trackingStats.DFG_PCT, split.timeStats.games_played )
  }
end

def calculateFourFactorStats( boxscore, statSet )
  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    calculateAverages2( boxscore, :FTA_RATE, split.fourfactorStats.FTA_RATE, split.timeStats.games_played )
  }
end

def calculateUsageStats( boxscore, statSet )
  splits = [ statSet.split ]
  if 1 == statSet.away_split.valid
    splits.push( statSet.away_split )
  end
  if 1 == statSet.home_split.valid
    splits.push( statSet.home_split )
  end
  if 1 == statSet.starter_split.valid
    splits.push( statSet.starter_split )
  end
  if 1 == statSet.bench_split.valid
    splits.push( statSet.bench_split )
  end
  for i in 0...statSet.total_games_with_rest_split.size
    if 1 == statSet.total_games_with_rest_split[ i ].valid
      splits.push( statSet.total_games_with_rest_split[ i ] )
    end
  end
  if 1 == statSet.three_in_four_split.valid
    splits.push( statSet.three_in_four_split )
  end
  if 1 == statSet.four_in_six_split.valid
    splits.push( statSet.four_in_six_split )
  end

  splits.each{|split|
    calculateAverages2( boxscore, :USG_PCT, split.usageStats.USG_PCT, split.timeStats.games_played )

    game_total = divide( (split.gamelogStats.FGA.game_total + (0.44 * split.gamelogStats.FTA.game_total) + split.gamelogStats.TOV.game_total), boxscore[:USG_PCT].to_f ).to_f
    calculateAverages3( boxscore, game_total, split.usageStats.usage_offensive_possessions, split.timeStats.games_played )

    calculateAverages2( boxscore, :PCT_FGM, split.usageStats.PCT_FGM, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.FGM.game_total, boxscore[:PCT_FGM].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.FGM.game_total / game_total ).round(3) != boxscore[:PCT_FGM].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.FGM.game_total / game_total ).round(3)} #{boxscore[:PCT_FGM].to_f}"
    end
    calculateAverages3( boxscore, game_total, split.usageStats.team_FGM, split.timeStats.games_played )

    calculateAverages2( boxscore, :PCT_FGA, split.usageStats.PCT_FGA, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.FGA.game_total, boxscore[:PCT_FGA].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.FGA.game_total / game_total ).round(3) != boxscore[:PCT_FGA].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.FGA.game_total / game_total ).round(3)} #{boxscore[:PCT_FGA].to_f}"
    end

    calculateAverages3( boxscore, game_total, split.usageStats.team_FGA, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_FG3M, split.usageStats.PCT_FG3M, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.FG3M.game_total, boxscore[:PCT_FG3M].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.FG3M.game_total / game_total ).round(3) != boxscore[:PCT_FG3M].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.FG3M.game_total / game_total ).round(3)} #{boxscore[:PCT_FG3M].to_f}"
    end

    calculateAverages3( boxscore, game_total, split.usageStats.team_FG3M, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_FG3A, split.usageStats.PCT_FG3A, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.FG3A.game_total, boxscore[:PCT_FG3A].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.FG3A.game_total / game_total ).round(3) != boxscore[:PCT_FG3A].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.FG3A.game_total / game_total ).round(3)} #{boxscore[:PCT_FG3A].to_f}"
    end
    
    calculateAverages3( boxscore, game_total, split.usageStats.team_FG3A, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_FTM, split.usageStats.PCT_FTM, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.FTM.game_total, boxscore[:PCT_FTM].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.FTM.game_total / game_total ).round(3) != boxscore[:PCT_FTM].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.FTM.game_total / game_total ).round(3)} #{boxscore[:PCT_FTM].to_f}"
    end

    calculateAverages3( boxscore, game_total, split.usageStats.team_FTM, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_FTA, split.usageStats.PCT_FTA, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.FTA.game_total, boxscore[:PCT_FTA].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.FTA.game_total / game_total ).round(3) != boxscore[:PCT_FTA].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.FTA.game_total / game_total ).round(3)} #{boxscore[:PCT_FTA].to_f}"
    end

    calculateAverages3( boxscore, game_total, split.usageStats.team_FTA, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_OREB, split.usageStats.PCT_OREB, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.OREB.game_total, boxscore[:PCT_OREB].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.OREB.game_total / game_total ).round(3) != boxscore[:PCT_OREB].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.OREB.game_total / game_total ).round(3)} #{boxscore[:PCT_OREB].to_f}"
    end

    calculateAverages3( boxscore, game_total, split.usageStats.team_OREB, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_DREB, split.usageStats.PCT_DREB, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.DREB.game_total, boxscore[:PCT_DREB].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.DREB.game_total / game_total ).round(3) != boxscore[:PCT_DREB].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.DREB.game_total / game_total ).round(3)} #{boxscore[:PCT_DREB].to_f}"
    end
    
    calculateAverages3( boxscore, game_total, split.usageStats.team_DREB, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_REB, split.usageStats.PCT_REB, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.REB.game_total, boxscore[:PCT_REB].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.REB.game_total / game_total ).round(3) != boxscore[:PCT_REB].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.REB.game_total / game_total ).round(3)} #{boxscore[:PCT_REB].to_f}"
    end
    
    calculateAverages3( boxscore, game_total, split.usageStats.team_REB, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_AST, split.usageStats.PCT_AST, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.AST.game_total, boxscore[:PCT_AST].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.AST.game_total / game_total ).round(3) != boxscore[:PCT_AST].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.AST.game_total / game_total ).round(3)} #{boxscore[:PCT_AST].to_f}"
    end
    
    calculateAverages3( boxscore, game_total, split.usageStats.team_AST, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_TOV, split.usageStats.PCT_TOV, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.TOV.game_total, boxscore[:PCT_TOV].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.TOV.game_total / game_total ).round(3) != boxscore[:PCT_TOV].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.TOV.game_total / game_total ).round(3)} #{boxscore[:PCT_TOV].to_f}"
    end
    
    calculateAverages3( boxscore, game_total, split.usageStats.team_TOV, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_STL, split.usageStats.PCT_STL, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.STL.game_total, boxscore[:PCT_STL].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.STL.game_total / game_total ).round(3) != boxscore[:PCT_STL].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.STL.game_total / game_total ).round(3)} #{boxscore[:PCT_STL].to_f}"
    end
    
    calculateAverages3( boxscore, game_total, split.usageStats.team_STL, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_BLK, split.usageStats.PCT_BLK, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.BLK.game_total, boxscore[:PCT_BLK].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.BLK.game_total / game_total ).round(3) != boxscore[:PCT_BLK].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.BLK.game_total / game_total ).round(3)} #{boxscore[:PCT_BLK].to_f}"
    end
    
    calculateAverages3( boxscore, game_total, split.usageStats.team_BLK, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_BLKA, split.usageStats.PCT_BLKA, split.timeStats.games_played )

    calculateAverages2( boxscore, :PCT_PF, split.usageStats.PCT_PF, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.PF.game_total, boxscore[:PCT_PF].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.PF.game_total / game_total ).round(3) != boxscore[:PCT_PF].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.PF.game_total / game_total ).round(3)} #{boxscore[:PCT_PF].to_f}"
    end
    
    calculateAverages3( boxscore, game_total, split.usageStats.team_PF, split.timeStats.games_played )
    calculateAverages2( boxscore, :PCT_PFD, split.usageStats.PCT_PFD, split.timeStats.games_played )

    calculateAverages2( boxscore, :PCT_PTS, split.usageStats.PCT_PTS, split.timeStats.games_played )
    game_total = divide( split.gamelogStats.PTS.game_total, boxscore[:PCT_PTS].to_f ).round.to_f
    if game_total > 0 and ( split.gamelogStats.PTS.game_total / game_total ).round(3) != boxscore[:PCT_PTS].to_f
      #binding.pry
      #p "error #{( split.gamelogStats.PTS.game_total / game_total ).round(3)} #{boxscore[:PCT_PTS].to_f}"
    end
    
    calculateAverages3( boxscore, game_total, split.usageStats.team_offensive_PTS, split.timeStats.games_played )

    game_total = divide( 100 * split.usageStats.team_offensive_PTS.game_total, split.advancedStats.OFF_RATING.game_total ).to_f
    calculateAverages3( boxscore, game_total, split.usageStats.offensive_possessions, split.timeStats.games_played )

    off_total = divide( split.gamelogStats.PTS.game_total, boxscore[:PCT_PTS].to_f ).round.to_f
    game_total = off_total - split.gamelogStats.PLUS_MINUS.game_total.to_f

    calculateAverages3( boxscore, game_total, split.usageStats.team_defensive_PTS, split.timeStats.games_played )
    game_total = divide( 100 * split.usageStats.team_defensive_PTS.game_total, split.advancedStats.DEF_RATING.game_total ).to_f
    calculateAverages3( boxscore, game_total, split.usageStats.defensive_possessions, split.timeStats.games_played )

  }
end

def get_csv_output( doc, tableid )
  #tableref = document.getElementById(tableid)
  tableref = doc.at_css("table#" + tableid )
  pre_filled_value = -9999

  #headrow = tableref.tHead.rows[tableref.tHead.rows.length-1].cells
  begin
    headrow = tableref.at_css("thead").css("tr")[ tableref.at_css("thead").css("tr").size - 1 ].css("th")
  rescue StandardError => e
    p "error"
    return -1
  end

  maxx = tableref.css("tr").size
  maxy = headrow.size
  table_entries = Array.new(maxx)

  for x in 0..maxx
    table_entries[ x ] = Array.new( maxy )
    for y in 0..maxy
      table_entries[x][y] = pre_filled_value
    end
  end

  i = 0
  while maxx > i
    j = 0
    pre_filled_table_entries = 0

    while maxy > j
      if table_entries[i][j] == pre_filled_value
        cell_rowspan = 1
        cell_colspan = 1

        #node_value = get_node_inner_text( tableref.rows[i].cells[ j - pre_filled_table_entries ] )
        if tableref.css("tr")[i].css("th").empty?
          node_value = tableref.css("tr")[i].css("td")[j - pre_filled_table_entries].text
        elsif tableref.css("tr")[i].css("td").empty?
          node_value = tableref.css("tr")[i].css("th")[j - pre_filled_table_entries].text
        else
          binding.pry
          p "error"
        end

        new_node_value = node_value.gsub( /,/, "" )
        node_value = new_node_value
        table_entries[i][j] = node_value

        if cell_rowspan > 1
          for k in 0...cell_rowspan
            for l in 0...cell_colspan
              table_entries[ i + k ][ j + l ] = node_value
            end
          end

          if cell_colspan > 1 
            pre_filled_table_entries += cell_colspan - 1
            j = j + cell_colspan - 1
          end
        elsif cell_colspan && cell_colspan > 1

          for k in 1...cell_colspan
            table_entries[ i ][ j + k ] = node_value
            pre_filled_table_entries = pre_filled_table_entries + 1
          end
          j += cell_colspan - 1
        end 
      end
      j = j + 1
    end
    i = i + 1
  end

  i = 0
  csv_output = ""
  while maxx > i
    row_output = Array.new
    j = 0
    while maxy > j
      row_output.push(table_entries[i][j])
      j = j + 1
    end

    csv_output = csv_output.concat(row_output.join(",")).concat("\n")
    i = i + 1
  end

  #return "<!-- ALREADYCSV -->".concat(csv_output)
  return csv_output
end

def convertBBRTeamAbbr2( abbr, year )
  if "PHO" == abbr and year < 2006
    team_corrected = "PHX"
  elsif "NOK" == abbr
    team_corrected = "NOH" and year < 2006
  else
    team_corrected = abbr
  end
  return team_corrected
end

def convertBBRTeamAbbr( abbr, year )
  if "CHO" == abbr
    team_corrected = "CHA"
  elsif "BRK" == abbr
    team_corrected = "BKN"
  elsif "PHO" == abbr
    team_corrected = "PHX"
  elsif "SEA" == abbr and year < 2009
    team_corrected = "OKC"
  elsif "NOK" == abbr and year < 2008
    team_corrected = "NOP"
  elsif "WSB" == abbr and year < 1998
    team_corrected = "WAS"
  elsif "UTA" == abbr and year < 1997
    team_corrected = "UTH"
  elsif "GSW" == abbr and year < 1997
    team_corrected = "GOS"
  elsif "SAS" == abbr and year < 1997
    team_corrected = "SAN"
  elsif "PHI" == abbr and year < 1997
    team_corrected = "PHL"
  else
    team_corrected = abbr
  end

  return team_corrected
end

def get_team_table( doc, div_id, table_id, skip_headings = false, player_links = nil, team_abbr = nil )
  csv = ""
  table = doc.css("div##{div_id} table##{table_id}")
  #Ignore the over_header of "advanced stats, Four factors, etc."
  #Only grab headings if skip_headings is not true
  if nil == skip_headings or false == skip_headings
    trs = table.css("thead tr")
    tableheadings = trs.last.css("th")

    if tableheadings.size == 0
      #This means the last row is garbage, grab row above
      tableheadings = trs[ trs.size - 2 ].css("th")
    end

    thsize = tableheadings.size
    tableheadings.each_with_index{|th,i|
      csv += th["data-stat"]

      if ( thsize - 1 ) == i
        csv += "\n"
      else
        csv += ","
      end
    }
  end

  table.css("tbody tr").each{|tr|
    trsize = tr.css("th,td").size

    tr.css("th,td").each_with_index{|td,i|
      text = td.text.gsub(/\*|/,"").gsub(/,/, " ")

      if /Team Totals/ =~ text
        text = team_abbr
      end

      if player_links and td.at_css("a") and td.at_css("a")["href"].match(/players/)
        player_links[ td["csk"] ] = "https://www.basketball-reference.com" + td.at_css("a")["href"]
      end

      if ( trsize - 1 ) == i
        csv += text + "\n"
      else
        csv += text + ","
      end
    }
  }

  return csv
end

def grabRosterCSV2( seasons, index, team, database )
  season = seasons[index]
  year = season.split("-")[0].to_i + 1 
  if year > 2014 and "CHA" == team[:TEAM_ABBREVIATION]
    team_corrected = "CHO"
  elsif "BKN" == team[:TEAM_ABBREVIATION]
    team_corrected = "BRK"
  elsif "PHX" == team[:TEAM_ABBREVIATION]
    team_corrected = "PHO"
  elsif "WST" == team[:TEAM_ABBREVIATION] or "EST" == team[:TEAM_ABBREVIATION]
    return
  elsif "OKC" == team[:TEAM_ABBREVIATION] and year < 2009
    team_corrected = "SEA"
  elsif "NOP" == team[:TEAM_ABBREVIATION] and year < 2008
    team_corrected = "NOK"
  elsif "WAS" == team[:TEAM_ABBREVIATION] and year < 1998
    team_corrected = "WSB"
  elsif "UTH" == team[:TEAM_ABBREVIATION] and year < 1997
    team_corrected = "UTA"
  elsif "GOS" == team[:TEAM_ABBREVIATION] and year < 1997
    team_corrected = "GSW"
  elsif "SAN" == team[:TEAM_ABBREVIATION] and year < 1997
    team_corrected = "SAS"
  elsif "PHL" == team[:TEAM_ABBREVIATION] and year < 1997
    team_corrected = "PHI"
  else
    team_corrected = team[:TEAM_ABBREVIATION]
  end
  p "#{year} #{team_corrected}"
  begin
    doc = Nokogiri::HTML( open( "https://www.basketball-reference.com/teams/#{team_corrected}/#{year}.html" ) )
  rescue StandardError => e
    binding.pry
    p 'hi'
  end

  csvArray = Array.new
  csvArray.push( [ "roster", csv_team_roster = get_team_table( doc, "all_roster", "roster", false, nil ) ] )
  csvArray.each{|csvItem|
    FileUtils::mkdir_p( "basketball_box_scores/" + season + "/" + team[:TEAM_ABBREVIATION] )
    File.open( "basketball_box_scores/" + season + "/" + team[:TEAM_ABBREVIATION] + "/" + csvItem[0] + ".csv", "w" ){|f|
      arr = CSV.parse( csvItem[1] )
      csv_str = CSV.generate do |csv|
        arr.each_with_index{|a,ind|
          if 0 == ind
            a.unshift("Team")
            a.push("PLAYER_ID")
          else
            a.unshift(team[:TEAM_ABBREVIATION])
            a.push(nil)
          end
          csv << a
        }
      end
      p "writing #{team[:TEAM_ABBREVIATION]}"
      f.write csv_str
    }
  }
end

def grabRosterCSV( seasons, index, team, database )
  season = seasons[index]
  year = season.split("-")[0].to_i + 1 
  if "CHA" == team[:TEAM_ABBREVIATION]
    team_corrected = "CHO"
  elsif "BKN" == team[:TEAM_ABBREVIATION]
    team_corrected = "BRK"
  elsif "PHX" == team[:TEAM_ABBREVIATION]
    team_corrected = "PHO"
  elsif "WST" == team[:TEAM_ABBREVIATION] or "EST" == team[:TEAM_ABBREVIATION]
    return
  elsif "OKC" == team[:TEAM_ABBREVIATION] and year < 2009
    team_corrected = "SEA"
  elsif "NOP" == team[:TEAM_ABBREVIATION] and year < 2008
    team_corrected = "NOK"
  elsif "WAS" == team[:TEAM_ABBREVIATION] and year < 1998
    team_corrected = "WSB"
  elsif "UTH" == team[:TEAM_ABBREVIATION] and year < 1997
    team_corrected = "UTA"
  elsif "GOS" == team[:TEAM_ABBREVIATION] and year < 1997
    team_corrected = "GSW"
  elsif "SAN" == team[:TEAM_ABBREVIATION] and year < 1997
    team_corrected = "SAS"
  elsif "PHL" == team[:TEAM_ABBREVIATION] and year < 1997
    team_corrected = "PHI"
  else
    team_corrected = team[:TEAM_ABBREVIATION]
  end
  p "#{year} #{team_corrected}"
  doc = Nokogiri::HTML( open( "https://www.basketball-reference.com/teams/#{team_corrected}/#{year}.html" ) )

  csv = get_csv_output(doc, "roster")
  if -1 == csv
    teams = database[ :"#{seasons[index-1].gsub(/-/,"_")}_regularseason_traditional_TeamStats" ].distinct.select(:TEAM_ABBREVIATION, :TEAM_ID).entries
    new_abbr = ""
    teams.each{|t|
      if team[:TEAM_ID] == t[:TEAM_ID]
        new_abbr = t[:TEAM_ABBREVIATION]
        break
      end
    }

    if "" != new_abbr
      p "corrected: #{new_abbr}"
      doc = Nokogiri::HTML( open( "https://www.basketball-reference.com/teams/#{new_abbr}/#{year}.html" ) )
      csv = get_csv_output(doc, "roster")
      binding.pry
    end
  end

  arr = CSV.parse( csv )
  csv_str = CSV.generate do |csv|
    arr.each_with_index{|a,ind|
      if 0 == ind
        a.unshift("Team")
        a.push("PLAYER_ID")
      else
        a.unshift(team[:TEAM_ABBREVIATION])
        a.push("0")
      end
      csv << a
    }
  end
    
  team_dir = FileUtils::mkdir_p "basketball_box_scores/" + season + "/" + team[:TEAM_ABBREVIATION]
  File.open( "basketball_box_scores/" + season + "/" + team[:TEAM_ABBREVIATION] + "/roster.csv", "w" ){|f|
    f.write csv_str
  }
end

def putRosterinDB( season, team, database, i )
  filename = "basketball_box_scores/" + season +"/" + team[:TEAM_ABBREVIATION] + "/roster.csv"
  #Dir.glob("basketball_box_scores/" + season +"/" + team + "/*" + category + ".csv").each_with_index|filename,i|
  tablename = season + "_bioinfo"
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
  if 0 == i
  #if !database.table_exists? tablename
    options = { :headers    => true,
                :header_converters => nil,
                :converters => nil }
    #:converters => [:date_time, :float] 
    #:converters => :all  

    #season = season.gsub(/-/,"")
    #filename = filename.gsub(/-/,"_")

    data = CSV.table(filename, options)
    #data = CSV.foreach(filename, headers: true, converters: :all){|row|
    #  p row
    #}
    headers = data.headers
    #tablename = File.basename(filename, '').gsub(/[^0-9a-zA-Z_]/,'_').to_sym

    puts "Dropping and re-creating table #{tablename}"
    database.drop_table? tablename
    begin
      database.create_table tablename do
        # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
        # primary_key :id
        # Float :price
        data.by_col!.each do |columnName,rows|
          #if columnName.match /_ID/
          #  columnType = String
          #  column columnName, columnType
          #else
          columnType = getCommonClass(rows) || String
          if NilClass == columnType
            columnType = String
          end
          column columnName, columnType
          p "#{columnName} #{columnType}"
        end
      end
    rescue StandardError => e
      binding.pry
      p "hi"
    end
    begin
      data.by_row!.each do |row|
        database[tablename].insert(row.to_hash)
      end
    rescue StandardError => e
      binding.pry
      p e
    end
  else
    p "processing #{filename}"
    options = { :headers    => true,
                :header_converters => nil,
                :converters => nil }
    #:converters => [:date_time, :float ] 
    #:converters => :all 

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
end

def fillRosters( seasons, database )
  team_tables = [ "advanced_TeamStats", "fourfactors_sqlTeamsFourFactors", "misc_sqlTeamsMisc", "playertrack_PlayerTrackTeam", "scoring_sqlTeamsScoring", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlTeamsUsage" ]
    seasons.each_with_index{|season,i|
    type = "regularseason"
    teams = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{team_tables[6]}" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:TEAM_ABBREVIATION).entries
      teams.each_with_index{|team,j|
        p "#{season} #{team}"
        if "WST" == team[:TEAM_ABBREVIATION] or "EST" == team[:TEAM_ABBREVIATION]
          next
        end

        grabRosterCSV2( seasons, i, team, database )
        putRosterinDB( season, team, database, j )
      p "#{j}: #{team}"
      }
    }
end

def convertFanDuelTeamName( team )
  if "nor" == team
    team = "nop"
  elsif "pho" == team
    team = "phx"
  end

  return team.upcase
end
##jlk - eventually need to make fillBBallReferenceBioinfo() dynamically updatable during the season
#
#Match player names and birthdays, and then fill missing _bioinfo playerIDs
def fillBBallReferenceBioinfo( seasons, database )
seasons.each{|season|
  player_tables = [ "advanced_PlayerStats", "fourfactors_sqlPlayersFourFactors", "misc_sqlPlayersMisc", "playertrack_PlayerTrack", "scoring_sqlPlayersScoring", "traditional_PlayerStats", "usage_sqlPlayersUsage" ]
  ["regularseason", "playoffs"].each{|type|
    
    if !database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_#{player_tables[5]}"
      return
    end
    players = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{player_tables[5]}" ].distinct.select(:TEAM_ID, :TEAM_ABBREVIATION, :PLAYER_ID, :PLAYER_NAME).entries
    #players = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{player_tables[5]}" ].distinct.select(:PLAYER_ID, :PLAYER_NAME).entries
    p "#{players.size} players in traditional boxscores"

    arrIndices = Array.new
    players.each_with_index{|player,i|
      #gamelogs = database[ :"#{season.gsub(/-/,"_")}_#{type}_player_gamelogs" ].select(:PLAYER_ID).where(:PLAYER_ID => player[:PLAYER_ID]).entries
      boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].select(:PLAYER_ID).where(:PLAYER_ID => player[:PLAYER_ID]).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries
      if [] == boxscores
        p "#{player} has no boxscores, deleting.."
        arrIndices.push i
      end
    }
    while index = arrIndices.pop
      p "deleting #{index}: #{players[index]}"
      players.delete_at index
    end
    p "#{players.size} players left"

    players.each_with_index{|player,p_index|
      name = player[:PLAYER_NAME]
      #data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => name).limit(1).entries
      data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => name).entries

      if data and data.size > 0
        position = data[0][:Pos]

        hash = { :PLAYER_ID => player[:PLAYER_ID], :Player => name, :Position => position }
        data[0][:PLAYER_ID] = player[:PLAYER_ID]

        row = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:player => data[0][:player], :birth_date => data[0][:birth_date]).update(:PLAYER_ID => player[:PLAYER_ID])
      else
        selection_index = nil

        p "#{p_index} #{player} didn't match"
        lastname = nil
        if ("Jr." == name.split(" ").last or "III" == name.split(" ").last or "II" == name.split(" ").last or "IV" == name.split(" ").last or "V" == name.split(" ").last)
          name = name.split(" ")[ 0...-1 ].join(" ")
          data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => name).entries
        end

        if data and data.size > 0
          p "Jr./II/III/IV/V fix worked"
          row = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:player => data[0][:player], :birth_date => data[0][:birth_date]).update(:PLAYER_ID => player[:PLAYER_ID])
        else
          lastname = name.split(" ").last
          if lastname
            data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(Sequel.like(:Player,"%#{lastname}")).entries
          end
          data_first = nil
          firstname = name.split(" ").first
          if firstname
            data_first = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(Sequel.like(:Player,"#{firstname}%")).entries
          end
          numMatchingTeams = 0
          teamMatchIndex = nil
          data.each_with_index{|d,i|
            if d[:player].gsub(".","") == name
              selection_index = i
              break
            elsif d[:Team] == player[:TEAM_ABBREVIATION]
              numMatchingTeams = numMatchingTeams + 1
              teamMatchIndex = i
            end
          }

          if selection_index
            p "removing '.' fix worked"
            row = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:player => data[selection_index][:player], :birth_date => data[selection_index][:birth_date]).update(:PLAYER_ID => player[:PLAYER_ID])
          elsif (1 == data.size and /#{player[:PLAYER_NAME].split.join(".*")}/.match data[0][:player])
            p "removing '.' split.join fix worked"
            row = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:player => data[0][:player], :birth_date => data[0][:birth_date]).update(:PLAYER_ID => player[:PLAYER_ID])
          elsif 1 == numMatchingTeams
            p "lastname + team fix worked"
            row = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:player => data[0][:player], :birth_date => data[0][:birth_date]).update(:PLAYER_ID => player[:PLAYER_ID])
            #jlk can parse through numMatchingTeams to see which one it is
          elsif data_first and data_first.size == 1
            p "firstname fix worked"
            row = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:player => data_first[0][:player], :birth_date => data_first[0][:birth_date]).update(:PLAYER_ID => player[:PLAYER_ID])
          else
            data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(Sequel.like(:Player,"#{name}%")).entries
            if data and data.size == 1
              p "name fix worked"
              row = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:player => data[0][:player], :birth_date => data[0][:birth_date]).update(:PLAYER_ID => player[:PLAYER_ID])
            else
              if nil == selection_index
                p "player name is #{name}"
                p "Choices are: "
                data.each_with_index{|d,i|
                  p "#{i}: #{d[:player]} #{d[:Pos]} #{d[:Team]}"
                }

                if nil == data or 0 == data.size
                  if lastname
                    data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(Sequel.like(:Player,"%#{lastname}%")).entries
                  else
                    data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(Sequel.like(:Player,"%#{name}%")).entries
                  end
                  if nil == data or 0 == data.size
                    binding.pry #
                    p "Could not find match in NBA bioinfo database for #{player}"
                    next
                  end
                else
                  p "selection_index is what?"
                  if data and data.size > 0
                    bAllMatch = true
                    data_name = data.first[:player]
                    data.each{|d|
                      if d[:player] != data_name
                        bAllMatch = false
                        break
                      end
                    }
                    if true == bAllMatch
                      selection_index = 0
                    end
                  end
                end
              end

              if selection_index
                row = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:player => data[selection_index][:player], :birth_date => data[selection_index][:birth_date]).update(:PLAYER_ID => player[:PLAYER_ID])
              else
                binding.pry
                p "help!!!"
              end
            end
          end
        end
      end
    }
  }
}
end

def fillNBABioStats( seasons, database )
  seasons.each{|season|
    d = `curl 'https://stats.nba.com/stats/leaguedashplayerbiostats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&Season=#{season}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight=' -H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/game/' -H 'Connection: keep-alive' --compressed`
    json = JSON.parse( d )

    tablename = "#{season}_biostats"
    tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

    if !database.table_exists? tablename
      options = { :headers    => true,
                  :header_converters => nil,
                  :converters => nil } # This makes all fields strings
      headers = json["resultSets"][0]["headers"]

      puts "Dropping and re-creating table #{tablename}"
      database.drop_table? tablename
      database.create_table tablename do
        headers.each do |columnName|
          columnType = String
          column columnName, columnType
        end
      end
    end

    json["resultSets"].each{|resultSet|
      resultSet["rowSet"].each{|row|
        h = Hash.new
        row.each_with_index{|item,i|
          h[ json["resultSets"][0]["headers"][i] ] = item
        }

        database[tablename].insert( h )
      }
    }
  }
end
##jlk - eventually need to make fillFanDuleInfo() dynamically updatable during the season
#update fanduelinfo table with playerIDs by matching stat lines
##fill missing _fanduelinfo playerIDs
def bUniqueBiostatsName( database, season, name )
  biostats = database[ :"#{season.gsub(/-/,"_")}_biostats" ].where(:PLAYER_NAME => name).entries
  bAllNamesSame = false

  biostats.each{|biostat|
    if biostat[:PLAYER_NAME] ==  name
      bAllNamesSame = true
    else
      bAllNamesSame = false
      break
    end
  }
  player = nil
  if false == bAllNamesSame
    p "multiple biostat names"
  else
    player = biostats[0]
  end
#Sequel.like(:"Player", "% #{name.split(' ')[1]}%"
  return player
end

def getPtsRebsFromFD( fd_games )
  points = fd_games[0][:"Stat line"].split("pt")[0]
  if points.size > 2
    points = "0"
  end
  rebs = "0"
  if fd_games[0][:"Stat line"].split("pt ") and fd_games[0][:"Stat line"].split("pt ")[1] and fd_games[0][:"Stat line"].split("pt ")[1].split("rb")
    rebs = fd_games[0][:"Stat line"].split("pt ")[1].split("rb")[0]
    if rebs.size > 2
      rebs = "0"
    end
  else
    rebs = "0"
  end
  return points, rebs
end

def matchFanDuelAndBoxscoreLines( database, season, type, game_date, boxscores, fd_games, player, p_index, pts, rebs )
  team_name = convertFanDuelTeamName( fd_games[0][:Team] )
  tablename = type + "_fanduelinfo"

  if 1 == boxscores.size
    player_id = boxscores[0][:PLAYER_ID]
    database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
    p "Matching stats instead of name...*#{p_index}: #{player[:Name]} matching with #{boxscores[0][:PLAYER_NAME]}"
  elsif boxscores.size > 1
    name_first = boxscores[0][:PLAYER_NAME]
    bAllNamesSame = false
    boxscores.each{|boxscore|
      if boxscore[:PLAYER_NAME] == name_first
        bAllNamesSame = true
      else
        bAllNamesSame = false
        break
      end
    }
    if true == bAllNamesSame
      player_id = boxscores[0][:PLAYER_ID]
      database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
      p "AllNamesSame *#{p_index}: #{player[:Name]} matching with #{boxscores[0][:PLAYER_NAME]}"
    else
      binding.pry
      p "pick a match manually"
      p "player name is #{name}"
      p "Choices are: "
      boxscores.each_with_index{|boxscore,i|
        p "#{i}: #{boxscore[:PLAYER_NAME]} #{boxscore[:START_POSITION]} #{boxscore[:TEAM_ABBREVIATION]}"
      }
      binding.pry
      p "selection_index is what?"
      #database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
    end
  else #no boxscores match
    boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:DATE => Date.parse(game_date).to_s, :team_abbreviation => team_name).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries
    binding.pry
    p "#{p_index} can't find so far"
  end
end

def fillFanDuelInfo( seasons, database )
seasons.each{|season|
  ["regularseason", "playoffs"].each{|type|
    tablename = type + "_fanduelinfo"
    if !database.table_exists? tablename
      next
    end

    players = database[ :"#{tablename}" ].distinct.select(:Name).entries.reject{|p| nil == p[:Name]}
    players.each_with_index{|player,p_index|
      name = nil
      if player[:Name].match(",")
        name = player[:Name].split(", ")[1] + " " + player[:Name].split(", ")[0]
      else
        name = player[:Name]
      end

      fd_games = database[ :"#{tablename}" ].where(:Name => player[:Name]).entries.reject{|g| nil == g[:"Stat line"] or "NA" == g[:Minutes]}

      if nil == fd_games or 0 == fd_games.size
        p "No games for #{name}"
        next
      end

      begin
        team_name = convertFanDuelTeamName( fd_games[0][:Team] )
        game_date = Date.parse( fd_games[0][:Date].to_s).to_s

        boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:DATE => game_date, :team_abbreviation => team_name, :PLAYER_NAME => name).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries

        if 0 < boxscores.size
          player_id = boxscores[0][:PLAYER_ID]
          database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
          #p "#{p_index}: #{player[:Name]} matching with #{boxscores[0][:PLAYER_NAME]}"
        elsif biostats_player = bUniqueBiostatsName( database, season, name )
          player_id = biostats_player[:PLAYER_ID]
          biostats_name = biostats_player[:PLAYER_NAME]
          database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
          p "biostats match #{p_index}: #{player[:Name]} matching with #{biostats_name}"
        else
          lastname = name.split.last
          boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:DATE => game_date, :team_abbreviation => team_name).where(Sequel.like(:"PLAYER_NAME", "%#{lastname}%")).exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").entries
          if 1 == boxscores.size
            player_id = boxscores[0][:PLAYER_ID]
            database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
            p "Lastname fix...#{p_index}: #{player[:Name]} matching with #{boxscores[0][:PLAYER_NAME]}"
          elsif boxscores.size > 1
            firstname = name.split.last
            boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:DATE => game_date, :team_abbreviation => team_name).where(Sequel.like(:"PLAYER_NAME", "%#{firstname}%")).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries
            if 1 == boxscores.size
              player_id = boxscores[0][:PLAYER_ID]
              database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
              p "firstname fix...#{p_index}: #{player[:Name]} matching with #{boxscores[0][:PLAYER_NAME]}"
            elsif boxscores.size > 1
              bAllNamesSame = false
              name_first = boxscores[0][:PLAYER_NAME]
              boxscores.each{|boxscore|
                if boxscore[:PLAYER_NAME] == name_first
                  bAllNamesSame = true
                else
                  bAllNamesSame = false
                  break
                end
              }
              if true == bAllNamesSame
                player_id = boxscores[0][:PLAYER_ID]
                database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
                p "AllNamesSame *#{p_index}: #{player[:Name]} matching with #{boxscores[0][:PLAYER_NAME]}"
              else
                points, rebs = getPtsRebsFromFD( fd_games )
                matched_boxscores = boxscores.select{|boxscore| points == boxscore[:PTS] and rebs == boxscore[:REB]}
                matchFanDuelAndBoxscoreLines( database, season, type, game_date, matched_boxscores, fd_games, player, p_index, points, rebs )
              end
            end
          else
            firstname = name.split.first
            boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:DATE => game_date, :team_abbreviation => team_name).where(Sequel.like(:"PLAYER_NAME", "%#{firstname}%")).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries
            if 1 == boxscores.size
              player_id = boxscores[0][:PLAYER_ID]
              database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
              p "firstname fix...#{p_index}: #{player[:Name]} matching with #{boxscores[0][:PLAYER_NAME]}"
            elsif boxscores.size > 1
              bAllNamesSame = false
              name_first = boxscores[0][:PLAYER_NAME]
              boxscores.each{|boxscore|
                if boxscore[:PLAYER_NAME] == name_first
                  bAllNamesSame = true
                else
                  bAllNamesSame = false
                  break
                end
              }
              if true == bAllNamesSame
                player_id = boxscores[0][:PLAYER_ID]
                database[ :"#{tablename}" ].where(:Name => player[:Name]).update(:PLAYER_ID => player_id)
                p "AllNamesSame *#{p_index}: #{player[:Name]} matching with #{boxscores[0][:PLAYER_NAME]}"
              else
                points, rebs = getPtsRebsFromFD( fd_games )
                boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:DATE => game_date, :team_abbreviation => team_name, :PTS => points, :REB => rebs).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries
                matchFanDuelAndBoxscoreLines( database, season, type, game_date, boxscores, fd_games, player, p_index, points, rebs )
              end
            else
              points, rebs = getPtsRebsFromFD( fd_games )
              boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:DATE => game_date, :team_abbreviation => team_name, :PTS => points, :REB => rebs).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries
              matchFanDuelAndBoxscoreLines( database, season, type, game_date, boxscores, fd_games, player, p_index, points, rebs )
            end
          end
        end
      rescue StandardError => e
        boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:DATE => game_date, :team_abbreviation => team_name).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries
        binding.pry
        p "**#{p_index}: #{name} played no games this year.  Adjust manually"
      end

      #data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => name).entries
    }
  }
}
end
def getFanduelSalaries2016( seasons_h, start_day = nil )
  seasons_h.each{|season,dates|
    ["regularseason","playoffs"].each_with_index{|type,i|
      day = Date.parse dates[i]
      if start_day
        day = Date.parse start_day
      end
      season_end = Date.parse dates[i+1]
      dir = FileUtils::mkdir_p season + "/" + type + "/fanduelsalaries"

      while day < season_end
        doc = Nokogiri::HTML( open( "http://rotoguru1.com/cgi-bin/hyday.pl?game=fd&mon=#{day.month}&day=#{day.day}&year=#{day.year}" ) )

        csv = "Date;Pos;Name;Starter;FD Pts;FD Salary;Team;H/A;Oppt;Team Score;Oppt Score;Minutes;Stat line\n"
        table_text = ""
        doc.css("table table")[7].css("tr").each_with_index{|tr,j|
          if 9 == tr.css("td").size
            table_text = table_text + "#{day.to_s};"

            bASGame = false
            tr.css("td").each_with_index{|td,i| 
              text = td.text.gsub(/\u00a0/," ").gsub(/^\s+/,"")

              if 1 == i
                table_text = table_text + td.text.gsub(/\^/,"") + ";"
                if text.match(/\^/)
                  table_text = table_text + "1"
                else
                  table_text = table_text
                end
              elsif 5 == i
                if text.match(/^v $/) or text.match(/^@ $/)
                  bASGame = true
                  break
                end
                if "v" == text.split(" ")[0]
                  table_text = table_text + "H;"
                elsif "@" == text.split(" ")[0]
                  table_text = table_text + "A;"
                end
                  
                begin
                  table_text = table_text + text.split(" ")[1]
                rescue StandardError => e
                  binding.pry
                  p "hi"
                end
              elsif 6 == i
                begin
                  table_text = table_text + text.split("-")[0] + ";"
                  table_text = table_text + text.split("-")[1]
                rescue StandardError => e
                  table_text = table_text
                end
              else
                table_text = table_text + text#.gsub(/\u00a0/," ").gsub(/^\s+/,"")
              end

              if (tr.css("td").size-1) != i
                table_text = table_text + ";"
              end
            }
            if true == bASGame
              table_text = ""
              break
            end
            table_text = table_text + "\n"
          end
        }
        
        csv = csv + table_text

        if "" == table_text
          p "no games for #{day.to_s}, don't write a file"
        else
          File.open( season + "/" + type + "/fanduelsalaries/" + "#{day.to_s}.csv", "w" ){|f|
            f.write csv
          }
        end
        p "parsed #{day.to_s}"

        day = day + 1
      end
    }
  }
end

def getFanduelSalaries( seasons_h )
  seasons_h.each{|season,dates|
    ["regularseason","playoffs"].each_with_index{|type,i|
      day = Date.parse dates[i]
      season_end = Date.parse dates[i+1]
      dir = FileUtils::mkdir_p season + "/" + type + "/fanduelsalaries"

      while day < season_end
        doc = Nokogiri::HTML( open( "http://rotoguru1.com/cgi-bin/hyday.pl?game=fd&mon=#{day.month}&day=#{day.day}&year=#{day.year}&scsv=1" ) )
        csv = doc.at_css("pre").text

        File.open( season + "/" + type + "/fanduelsalaries/" + "#{day.to_s}.csv", "w" ){|f|
          f.write csv
        }
        p "parsed #{day.to_s}"

        day = day + 1
      end
    }
}
end
def changePlayerGamelogDates( database, season )
  season = season.gsub("-","_")
  ["regularseason", "playoffs"].each{|type|
    entries = database[:"#{season}_#{type}_player_gamelogs"].select(:game_date).distinct.entries
    entries.each{|entry|
      date = Date.parse entry[:GAME_DATE]
      date_str = date.to_s

      database[:"#{season}_#{type}_player_gamelogs"].where(:game_date => entry[:GAME_DATE]).update(:GAME_DATE => date_str)
    }
  }
end 

def fillRowData( dstRow, srcRow )
  dstRow[:player_name] = srcRow[:player_name]
  dstRow[:date] = srcRow[:date]
  dstRow[:date_of_data] = srcRow[:date_of_data]
  dstRow[:team_abbreviation] = srcRow[:team_abbreviation]
  dstRow[:opponent_against_abbr] = srcRow[:opponent_against_abbr]
end

#Calculate daily averages for every day in the past
if OPTIONS[:season] and -1 != season_index
seasons = [seasons[season_index]]
end

def convertNBATeamAbbrToFanDuelAbbr( nbaAbbr )
  if "NOP" == nbaAbbr
    return "nor"
  elsif "PHX" == nbaAbbr
    return "pho"
  else
    return nbaAbbr.downcase
  end
end

def getNBAStarterPosition( index, startPosition )
  if 0 == index and ("F" == startPosition || "SF" == startPosition)
    return "SF"
  elsif 1 == index and ("F" == startPosition || "PF" == startPosition)
    return "PF"
  elsif 2 == index and "C" == startPosition
    return "C"
  elsif 3 == index and ("G" == startPosition || "SG" == startPosition)
    return "SG"
  elsif 4 == index and ("G" == startPosition || "PG" == startPosition)
    return "PG"
  else
    binding.pry
    p "Discrepancy processing NBA start position"
  end
end

def positionToNum( position )
  if "PG" == position
    return 1
  elsif "SG" == position
    return 2
  elsif "SF" == position
    return 3
  elsif "PF" == position
    return 4
  elsif "C" == position
    return 5
  else
    return -1
  end
end

def calculateTeamOpponentStats( database, season, type, team_tables, season_end_date )
  opponents = Hash.new
  season_end_date = Date.parse( season_end_date )

  tablename = "_" + season + " " + type + " daily averages"
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym
  teams = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{team_tables[6]}" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:TEAM_ABBREVIATION).entries

  if NUM_THREADS and THREAD_INDEX
    chunk_size = (teams.size/NUM_THREADS).round
    start_index = THREAD_INDEX * chunk_size
    if THREAD_INDEX == (NUM_THREADS - 1)
      p "thread #{THREAD_INDEX} / #{NUM_THREADS}.  Processing from team[#{start_index}...#{teams.size}].  Array size: #{teams.size}"
      teams = teams[start_index...(teams.size)]
    else
      p "thread #{THREAD_INDEX} / #{NUM_THREADS}.  Processing from team[#{start_index}...#{start_index + chunk_size}].  Array size: #{teams.size}"
      teams = teams[start_index...(start_index + chunk_size)]
    end
  end

  teams.each{|team|
    opponents[ team[:TEAM_ABBREVIATION] ] = Hash.new
    ["starter", "bench"].each{|start|
      opponents[ team[:TEAM_ABBREVIATION] ][ start ] = Hash.new
      ["PG", "SG", "SF", "PF", "C"].each{|pos|
        opponents[ team[:TEAM_ABBREVIATION] ][ start ][ pos ] = SplitSet.new
      }
    }
  }
  
  teams.each_with_index{|team,i|
    #blah
    gamelogs = database[ :"#{season.gsub(/-/,"_")}_#{type}_gamelogs" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).order(:GAME_DATE).entries

    begin
      cur_date = Date.parse( gamelogs[0][:GAME_DATE] )
    rescue StandardError => e
      binding.pry
      p "date error w/ boxscore"
    end

    p "doing team #{i} / #{teams.size}"
    row_opp = Hash.new
    gamelogs.each_with_index{|gamelog,j|
      opp_player_boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:GAME_ID => gamelog[:GAME_ID]).exclude(:TEAM_ID => gamelog[:TEAM_ID]).exclude(:MIN => nil).order(:DATE).entries

      p "doing team #{i} / #{teams.size} game #{j} / #{gamelogs.size}"

      opp_player_boxscores.each_with_index{|boxscore_traditional,i|
        starting_group = nil
        if boxscore_traditional[:START_POSITION].match /F|C|G/
          starting_group = "starter"
          position = getNBAStarterPosition( i, boxscore_traditional[:START_POSITION])
          #update DB w/ "real" position
          database[:"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats"].where(:PLAYER_NAME => boxscore_traditional[:PLAYER_NAME], :GAME_ID => boxscore_traditional[:GAME_ID]).update(:START_POSITION => position)
        else
          next
          starting_group = "bench"

          data_pos = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].distinct.where(:PLAYER_ID => boxscore_traditional[:PLAYER_ID]).entries
          if data_pos and data_pos[0]
              #old gamescores use lowercase for some reason
            begin
              position = data_pos[0][:Pos] ? data_pos[0][:Pos] : data_pos[0][:pos]
            rescue StandardError => e
              binding.pry
              p "pos crashes"
            end

            p "couldn't find position for player, use nba POS: #{position}.  player: #{boxscore_traditional[:PLAYER_NAME]} player_id: #{boxscore_traditional[:PLAYER_ID]}"
          else
            p "skip boxscore: #{boxscore_traditional} data_pos is nil or empty"
            next
          end

          if nil == boxscore_traditional[:MIN] and nil == boxscore_traditional[:PLUS_MINUS]
            p "skipping player: #{boxscore_traditional}"
            next
          end
        end

        begin
          calculateBoxscoreTime( gamelog, boxscore_traditional, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ], false )
        rescue StandardError => e
          binding.pry
          p "hi"
        end

        calculateTraditionalStats( boxscore_traditional, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ] )

        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_advanced_PlayerStats"
          boxscore_advanced = database[ :"#{season.gsub(/-/,"_")}_#{type}_advanced_PlayerStats" ].where(:PLAYER_NAME => boxscore_traditional[:PLAYER_NAME]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
          calculateAdvancedStats( boxscore_advanced, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ] )
        end

        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_scoring_sqlPlayersScoring"
          boxscore_scoring = database[ :"#{season.gsub(/-/,"_")}_#{type}_scoring_sqlPlayersScoring" ].where(:PLAYER_NAME => boxscore_traditional[:PLAYER_NAME]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
          calculateScoringStats( boxscore_scoring, boxscore_traditional, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ] )
          calculateScoringDerivedStats( boxscore_scoring, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ] )
        end

        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_fourfactors_sqlPlayersFourFactors"
          boxscore_fourfactors = database[ :"#{season.gsub(/-/,"_")}_#{type}_fourfactors_sqlPlayersFourFactors" ].where(:PLAYER_NAME => boxscore_traditional[:PLAYER_NAME]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
          calculateFourFactorStats( boxscore_fourfactors, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ] )
        end

        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_usage_sqlPlayersUsage"
          boxscore_usage = database[ :"#{season.gsub(/-/,"_")}_#{type}_usage_sqlPlayersUsage" ].where(:PLAYER_NAME => boxscore_traditional[:PLAYER_NAME]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
          calculateUsageStats( boxscore_usage, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ] )
        end

        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_misc_sqlPlayersMisc"
          boxscore_misc = database[ :"#{season.gsub(/-/,"_")}_#{type}_misc_sqlPlayersMisc" ].where(:PLAYER_NAME => boxscore_traditional[:PLAYER_NAME]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
          calculateMiscStats( boxscore_misc, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ], false )
        end

        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_playertrack_PlayerTrack"
          #boxscore_tracking = database[ :"#{season.gsub(/-/,"_")}_#{type}_playertrack_PlayerTrack" ].where(:PLAYER_NAME => boxscore_traditional[:PLAYER_NAME]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
          calculateTrackingStats( boxscore_tracking, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ] )
        end

        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_playertrack_PlayerStats"
          boxscore_tracking = database[ :"#{season.gsub(/-/,"_")}_#{type}_playertrack_PlayerStats" ].where(:PLAYER_NAME => boxscore_traditional[:PLAYER_NAME]).where(:GAME_ID => boxscore_traditional[:GAME_ID]).entries[0]
          calculateTrackingStats( boxscore_tracking, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ] )
        end

        calculateDerivedTraditionalStats( boxscore_traditional, opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ] )
        calculateDerivedStats( opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ], nil, database, tablename, boxscore_traditional[:TEAM_ABBREVIATION], boxscore_traditional[:GAME_ID], false )
        calculatePerMinStats( opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ], nil, false ) 

        #jlk - we can actually add more splits here
        row_opp = opponents[ team[:TEAM_ABBREVIATION] ][ starting_group ][ position ].split.to_h

        row_opp[:average_type] = "opponent vs #{starting_group} #{position}"
        row_opp[:player_name] = boxscore_traditional[:PLAYER_NAME]
        row_opp[:date] = boxscore_traditional[:DATE]
        row_opp[:date_of_data] = boxscore_traditional[:DATE]
        row_opp[:team_abbreviation] = boxscore_traditional[:TEAM_ABBREVIATION]
        row_opp[:opponent_against_abbr] = team[:TEAM_ABBREVIATION]
        p "inner: #{starting_group} #{position} #{row_opp[:date]} #{row_opp[:player_name]} team: #{row_opp[:team_abbreviation]} opp_against: #{row_opp[:opponent_against_abbr]}"

        begin
          database[tablename].insert(row_opp)
        rescue StandardError => e
          binding.pry
        end

        cur_date = Date.parse( boxscore_traditional[:DATE] )
        end_copy_date = nil
        if (gamelogs.size-1) == j
          end_copy_date = season_end_date + 1
        else
          end_copy_date = Date.parse( gamelogs[j+1][:GAME_DATE] )
        end

        while cur_date + 1 < end_copy_date
          cur_date = cur_date + 1

          row_opp[:date] = cur_date
          p "outer: #{starting_group} #{position} #{row_opp[:date]} #{row_opp[:player_name]} team: #{row_opp[:team_abbreviation]} opp_against: #{row_opp[:opponent_against_abbr]}"
          database[tablename].insert( row_opp )
        end
      }
    }
  }
end

def addPlayerIDToFanduel( players, database )
  fanduelplayers = database[ :"fanduelinfo" ].distinct.select(:GID, :NAME).entries
  players.each{|player|
    if 2 == player[:PLAYER_NAME].split(" ").size
      first = player[:PLAYER_NAME].split(" ")[0]
      last = player[:PLAYER_NAME].split(" ")[1]
      fdname = last + ", " + first
      entries = database[:"fanduelinfo"].where(:NAME => fdname).entries
      if entries.size > 0
        p "0th: #{entries[0]}"
        database[:"fanduelinfo"].where(:NAME => fdname).update(:player_id => player[:PLAYER_ID])
      else 
        binding.pry
        p "no match"
      end
    else
      binding.pry
      p "player: #{player} name complicated, do manually"
    end
  }
  p "done players"
end

def create_daily_averages_table( database, type, tablename )
  puts "Dropping and re-creating table #{tablename}"
  database.drop_table? tablename
  database.create_table tablename do
    # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
    # primary_key :id
    # Float :price
    column :player_name, :text
    column :date, :text
    column :date_of_data, :text
    column :game_id, :text
    column :team_abbreviation, :text
    column :average_type, :text
    column :opponent_against_abbr, :text

    column :games_played, :integer
    column :wins, :integer
    column :losses, :integer
    column :ties, :integer
    column :win_pct, :decimal
    column :seconds_played_total, :decimal
    column :seconds_played_mean, :decimal
    column :seconds_played_median, :decimal
    column :FGM_total, :decimal
    column :FGM_mean, :decimal
    column :FGM_median, :decimal
    column :FGA_total, :decimal
    column :FGA_mean, :decimal
    column :FGA_median, :decimal
    column :FG_PCT_total, :decimal
    column :FG_PCT, :decimal
    column :FG3M_total, :decimal
    column :FG3A_total, :decimal
    column :FG3_PCT_total, :decimal
    column :FG3_PCT, :decimal
    column :FG2M_total, :decimal
    column :FG2M_mean, :decimal
    column :FG2M_median, :decimal
    column :FG2A_total, :decimal
    column :FG2A_mean, :decimal
    column :FG2A_median, :decimal
    column :FG2_PCT_total, :decimal
    column :FG2_PCT_mean, :decimal
    column :FG2_PCT_median, :decimal
    column :FG2_PCT, :decimal
    column :FTM_total, :decimal
    column :FTA_total, :decimal
    column :FT_PCT_total, :decimal
    column :FT_PCT, :decimal
    column :OREB_total, :decimal
    column :DREB_total, :decimal
    column :REB_total, :decimal
    column :AST_total, :decimal
    column :STL_total, :decimal
    column :BLK_total, :decimal
    column :TOV_total, :decimal
    column :PF_total, :decimal
    column :PTS_total, :decimal
    column :PLUS_MINUS_total, :decimal
    column :TS_PCT_total, :decimal
    column :TS_PCT_mean, :decimal
    column :TS_PCT_median, :decimal
    column :EFG_PCT_total, :decimal
    column :EFG_PCT_mean, :decimal
    column :EFG_PCT_median, :decimal
    column :PCT_FGA_3PT_total, :decimal
    column :PCT_FGA_3PT_mean, :decimal
    column :PCT_FGA_3PT_median, :decimal
    column :FTA_RATE_total, :decimal
    column :FTA_RATE_mean, :decimal
    column :FTA_RATE_median, :decimal
    column :OREB_PCT_total, :decimal
    column :OREB_PCT_mean, :decimal
    column :OREB_PCT_median, :decimal
    column :DREB_PCT_total, :decimal
    column :DREB_PCT_mean, :decimal
    column :DREB_PCT_median, :decimal
    column :REB_PCT_total, :decimal
    column :REB_PCT_mean, :decimal
    column :REB_PCT_median, :decimal
    column :AST_PCT_total, :decimal
    column :AST_PCT_mean, :decimal
    column :AST_PCT_median, :decimal

    column :USG_PCT_total, :decimal
    column :USG_PCT_mean, :decimal
    column :USG_PCT_median, :decimal
    column :PCT_FGM_total, :decimal # Percent of Team's Field Goals Made
    column :PCT_FGM_mean, :decimal 
    column :PCT_FGM_median, :decimal
    column :PCT_FGA_total, :decimal #Percent of Team's Field Goals Attempted
    column :PCT_FGA_mean, :decimal
    column :PCT_FGA_median, :decimal
    column :PCT_FG3M_total, :decimal #Percent of Team's 3 Point Field Goals Made
    column :PCT_FG3M_mean, :decimal
    column :PCT_FG3M_median, :decimal
    column :PCT_FG3A_total, :decimal #Percent of Team's 3 Point Field Goals Attempted
    column :PCT_FG3A_mean, :decimal
    column :PCT_FG3A_median, :decimal
    column :PCT_FTM_total, :decimal #Percent of Team's Free Throws Made
    column :PCT_FTM_mean, :decimal
    column :PCT_FTM_median, :decimal
    column :PCT_FTA_total, :decimal #Percent of Team's Free Throws Attempted
    column :PCT_FTA_mean, :decimal
    column :PCT_FTA_median, :decimal
    column :PCT_OREB_total, :decimal #Percent of Team's Offensive Rebounds -jlk todo is this on the floor? or total
    column :PCT_OREB_mean, :decimal
    column :PCT_OREB_median, :decimal
    column :PCT_DREB_total, :decimal #Percent of Team's Defensive Rebounds
    column :PCT_DREB_mean, :decimal
    column :PCT_DREB_median, :decimal
    column :PCT_REB_total, :decimal #Percent of Team's Rebounds
    column :PCT_REB_mean, :decimal
    column :PCT_REB_median, :decimal
    column :PCT_AST_total, :decimal #Percent of Team's Assists
    column :PCT_AST_mean, :decimal
    column :PCT_AST_median, :decimal
    column :PCT_TOV_total, :decimal #Percent of Team's TOV
    column :PCT_TOV_mean, :decimal
    column :PCT_TOV_median, :decimal
    column :PCT_STL_total, :decimal #Percent of Team's STL
    column :PCT_STL_mean, :decimal
    column :PCT_STL_median, :decimal
    column :PCT_BLK_total, :decimal #Percent of Team's BLK
    column :PCT_BLK_mean, :decimal
    column :PCT_BLK_median, :decimal
    column :PCT_BLKA_total, :decimal #Percent of Team's BLKA
    column :PCT_BLKA_mean, :decimal
    column :PCT_BLKA_median, :decimal
    column :PCT_PF_total, :decimal #Percent of Team's personal fouls
    column :PCT_PF_mean, :decimal
    column :PCT_PF_median, :decimal
    column :PCT_PFD_total, :decimal #percent of team's personal fouls drawn
    column :PCT_PFD_mean, :decimal
    column :PCT_PFD_median, :decimal
    column :PCT_PTS_total, :decimal #percent of team's pts
    column :PCT_PTS_mean, :decimal
    column :PCT_PTS_median, :decimal
    column :TO_PCT_total, :decimal
    column :TO_PCT_mean, :decimal
    column :TO_PCT_median, :decimal
    column :offensive_possessions_total, :decimal
    column :offensive_possessions_mean, :decimal
    column :offensive_possessions_median, :decimal
    column :defensive_possessions_total, :decimal
    column :defensive_possessions_mean, :decimal
    column :defensive_possessions_median, :decimal
    column :usage_offensive_possessions_total, :decimal
    column :usage_offensive_possessions_mean, :decimal
    column :usage_offensive_possessions_median, :decimal
    column :team_FGM_total, :decimal
    column :team_FGM_mean, :decimal
    column :team_FGM_median, :decimal
    column :team_FGA_total, :decimal
    column :team_FGA_mean, :decimal
    column :team_FGA_median, :decimal
    column :team_FG3M_total, :decimal
    column :team_FG3M_mean, :decimal
    column :team_FG3M_median, :decimal
    column :team_FG3A_total, :decimal
    column :team_FG3A_mean, :decimal
    column :team_FG3A_median, :decimal
    column :team_FTM_total, :decimal
    column :team_FTM_mean, :decimal
    column :team_FTM_median, :decimal
    column :team_FTA_total, :decimal
    column :team_FTA_mean, :decimal
    column :team_FTA_median, :decimal
    column :team_OREB_total, :decimal
    column :team_OREB_mean, :decimal
    column :team_OREB_median, :decimal
    column :team_DREB_total, :decimal
    column :team_DREB_mean, :decimal
    column :team_DREB_median, :decimal
    column :team_REB_total, :decimal
    column :team_REB_mean, :decimal
    column :team_REB_median, :decimal
    column :team_AST_total, :decimal
    column :team_AST_mean, :decimal
    column :team_AST_median, :decimal
    column :team_TOV_total, :decimal
    column :team_TOV_mean, :decimal
    column :team_TOV_median, :decimal
    column :team_STL_total, :decimal
    column :team_STL_mean, :decimal
    column :team_STL_median, :decimal
    column :team_BLK_total, :decimal
    column :team_BLK_mean, :decimal
    column :team_BLK_median, :decimal
    #column :team_BLKA_total, :decimal
    #column :team_BLKA_mean, :decimal
    #column :team_BLKA_median, :decimal
    column :team_PF_total, :decimal
    column :team_PF_mean, :decimal
    column :team_PF_median, :decimal
    #column :team_PFD_total, :decimal
    #column :team_PFD_mean, :decimal
    #column :team_PFD_median, :decimal
    column :team_offensive_PTS_total, :decimal
    column :team_offensive_PTS_mean, :decimal
    column :team_offensive_PTS_median, :decimal
    column :team_defensive_PTS_total, :decimal
    column :team_defensive_PTS_mean, :decimal
    column :team_defensive_PTS_median, :decimal

    column :OFF_RATING_total, :decimal
    column :OFF_RATING_mean, :decimal
    column :OFF_RATING_median, :decimal
    column :DEF_RATING_total, :decimal
    column :DEF_RATING_mean, :decimal
    column :DEF_RATING_median, :decimal
    column :NET_RATING_total, :decimal
    column :NET_RATING_mean, :decimal
    column :NET_RATING_median, :decimal
    column :AST_TOV_total, :decimal
    column :AST_TOV_mean, :decimal
    column :AST_TOV_median, :decimal
    column :AST_RATIO_total, :decimal
    column :AST_RATIO_mean, :decimal
    column :AST_RATIO_median, :decimal
    column :PACE_total, :decimal
    column :PACE_mean, :decimal
    column :PACE_median, :decimal
    column :PIE_total, :decimal
    column :PIE_mean, :decimal
    column :PIE_median, :decimal
    column :FG_PCT_mean, :decimal
    column :FG_PCT_median, :decimal
    column :FG3M_mean, :decimal
    column :FG3M_median, :decimal
    column :FG3A_mean, :decimal
    column :FG3A_median, :decimal
    column :FG3_PCT_mean, :decimal
    column :FG3_PCT_median, :decimal
    column :FTM_mean, :decimal
    column :FTM_median, :decimal
    column :FTA_mean, :decimal
    column :FTA_median, :decimal
    column :FT_PCT_mean, :decimal
    column :FT_PCT_median, :decimal
    column :OREB_mean, :decimal
    column :OREB_median, :decimal
    column :DREB_mean, :decimal
    column :DREB_median, :decimal
    column :REB_mean, :decimal
    column :REB_median, :decimal
    column :AST_mean, :decimal
    column :AST_median, :decimal
    column :STL_mean, :decimal
    column :STL_median, :decimal
    column :BLK_mean, :decimal
    column :BLK_median, :decimal
    column :TOV_mean, :decimal
    column :TOV_median, :decimal
    column :PF_mean, :decimal
    column :PF_median, :decimal
    column :PTS_mean, :decimal
    column :PTS_median, :decimal
    column :PLUS_MINUS_mean, :decimal
    column :PLUS_MINUS_median, :decimal
    #scoring
    #column :PCT_FGA_2PT_total, :decimal
    #column :PCT_FGA_2PT_mean, :decimal
    #column :PCT_FGA_2PT_median, :decimal
    column :PCT_PTS_2PT_total, :decimal #Percent of PTS (2-Pointers)
    column :PCT_PTS_2PT_mean, :decimal
    column :PCT_PTS_2PT_median, :decimal
    column :PCT_PTS_2PT_MR_total, :decimal #Percent of Points (Mid-Range)
    column :PCT_PTS_2PT_MR_mean, :decimal
    column :PCT_PTS_2PT_MR_median, :decimal
    column :PCT_PTS_3PT_total, :decimal #Percent of Points (3-pointers)
    column :PCT_PTS_3PT_mean, :decimal
    column :PCT_PTS_3PT_median, :decimal
    column :PCT_PTS_FB_total, :decimal #Percent of Points (Fast Break Points)
    column :PCT_PTS_FB_mean, :decimal
    column :PCT_PTS_FB_median, :decimal
    column :PCT_PTS_FT_total, :decimal #Percent of Points (FTs)
    column :PCT_PTS_FT_mean, :decimal
    column :PCT_PTS_FT_median, :decimal
    column :PCT_PTS_OFF_TOV_total, :decimal #Percent of Points (OFF_TOV)
    column :PCT_PTS_OFF_TOV_mean, :decimal
    column :PCT_PTS_OFF_TOV_median, :decimal
    column :PCT_PTS_PAINT_total, :decimal #Percent of Points (PAINT)
    column :PCT_PTS_PAINT_mean, :decimal
    column :PCT_PTS_PAINT_median, :decimal
    column :AST_2PM_total, :decimal
    column :AST_2PM_mean, :decimal
    column :AST_2PM_median, :decimal
    column :PCT_AST_2PM_total, :decimal
    column :PCT_AST_2PM_mean, :decimal
    column :PCT_AST_2PM_median, :decimal
    column :UAST_2PM_total, :decimal
    column :UAST_2PM_mean, :decimal
    column :UAST_2PM_median, :decimal
    column :PCT_UAST_2PM_total, :decimal
    column :PCT_UAST_2PM_mean, :decimal
    column :PCT_UAST_2PM_median, :decimal
    column :AST_3PM_total, :decimal
    column :AST_3PM_mean, :decimal
    column :AST_3PM_median, :decimal
    column :PCT_AST_3PM_total, :decimal
    column :PCT_AST_3PM_mean, :decimal
    column :PCT_AST_3PM_median, :decimal
    column :UAST_3PM_total, :decimal
    column :UAST_3PM_mean, :decimal
    column :UAST_3PM_median, :decimal
    column :PCT_UAST_3PM_total, :decimal
    column :PCT_UAST_3PM_mean, :decimal
    column :PCT_UAST_3PM_median, :decimal
    column :AST_FGM_total, :decimal
    column :AST_FGM_mean, :decimal
    column :AST_FGM_median, :decimal
    column :PCT_AST_FGM_total, :decimal
    column :PCT_AST_FGM_mean, :decimal
    column :PCT_AST_FGM_median, :decimal
    column :UAST_FGM_total, :decimal
    column :UAST_FGM_mean, :decimal
    column :UAST_FGM_median, :decimal
    column :PCT_UAST_FGM_total, :decimal
    column :PCT_UAST_FGM_mean, :decimal
    column :PCT_UAST_FGM_median, :decimal
    column :PCT_AST_2PM, :decimal
    column :PCT_UAST_2PM, :decimal
    column :PCT_AST_3PM, :decimal
    column :PCT_UAST_3PM, :decimal
    column :PCT_AST_FGM, :decimal
    column :PCT_UAST_FGM, :decimal
    column :PCT_FGA_2PT, :decimal
    column :PCT_PTS_2PT, :decimal
    column :PCT_PTS_2PT_MR, :decimal
    column :PCT_PTS_3PT, :decimal
    column :PCT_PTS_FB, :decimal
    column :PCT_PTS_FT, :decimal
    column :PCT_PTS_OFF_TOV, :decimal
    column :PCT_PTS_PAINT, :decimal
    #column :FGA_2PT_total, :decimal
    #column :FGA_2PT_mean, :decimal
    #column :FGA_2PT_median, :decimal
    #column :PTS_2PT_total, :decimal
    #column :PTS_2PT_mean, :decimal
    #column :PTS_2PT_median, :decimal
    column :PTS_2PT_MR_total, :decimal
    column :PTS_2PT_MR_mean, :decimal
    column :PTS_2PT_MR_median, :decimal
    #column :PTS_3PT_total, :decimal
    #column :PTS_3PT_mean, :decimal
    #column :PTS_3PT_median, :decimal
    #column :PTS_FT_total, :decimal
    #column :PTS_FT_mean, :decimal
    #column :PTS_FT_median, :decimal

    #misc
    column :PTS_OFF_TOV_total, :decimal
    column :PTS_OFF_TOV_mean, :decimal
    column :PTS_OFF_TOV_median, :decimal
    column :PTS_2ND_CHANCE_total, :decimal
    column :PTS_2ND_CHANCE_mean, :decimal
    column :PTS_2ND_CHANCE_median, :decimal
    column :PTS_FB_total, :decimal
    column :PTS_FB_mean, :decimal
    column :PTS_FB_median, :decimal
    column :PTS_PAINT_total, :decimal
    column :PTS_PAINT_mean, :decimal
    column :PTS_PAINT_median, :decimal
    column :BLKA_total, :decimal
    column :BLKA_mean, :decimal
    column :BLKA_median, :decimal
    column :PFD_total, :decimal
    column :PFD_mean, :decimal
    column :PFD_median, :decimal

    #tracking
    column :DIST_total, :decimal
    column :DIST_mean, :decimal
    column :DIST_median, :decimal
    column :ORBC_total, :decimal
    column :ORBC_mean, :decimal
    column :ORBC_median, :decimal
    column :DRBC_total, :decimal
    column :DRBC_mean, :decimal
    column :DRBC_median, :decimal
    column :RBC_total, :decimal
    column :RBC_mean, :decimal
    column :RBC_median, :decimal
    column :TCHS_total, :decimal
    column :TCHS_mean, :decimal
    column :TCHS_median, :decimal
    column :SAST_total, :decimal
    column :SAST_mean, :decimal
    column :SAST_median, :decimal
    column :FTAST_total, :decimal
    column :FTAST_mean, :decimal
    column :FTAST_median, :decimal
    column :PASS_total, :decimal
    column :PASS_mean, :decimal
    column :PASS_median, :decimal
    column :CFGM_total, :decimal
    column :CFGM_mean, :decimal
    column :CFGM_median, :decimal
    column :CFGA_total, :decimal
    column :CFGA_mean, :decimal
    column :CFGA_median, :decimal
    column :CFG_PCT_total, :decimal
    column :CFG_PCT_mean, :decimal
    column :CFG_PCT_median, :decimal
    column :UFGM_total, :decimal
    column :UFGM_mean, :decimal
    column :UFGM_median, :decimal
    column :UFGA_total, :decimal
    column :UFGA_mean, :decimal
    column :UFGA_median, :decimal
    column :UFG_PCT_total, :decimal
    column :UFG_PCT_mean, :decimal
    column :UFG_PCT_median, :decimal
    column :DFGM_total, :decimal
    column :DFGM_mean, :decimal
    column :DFGM_median, :decimal
    column :DFGA_total, :decimal
    column :DFGA_mean, :decimal
    column :DFGA_median, :decimal
    column :DFG_PCT_total, :decimal
    column :DFG_PCT_mean, :decimal
    column :DFG_PCT_median, :decimal

    #these are aggregate stats
    column :TS_PCT, :decimal
    column :EFG_PCT, :decimal
    column :PCT_FGA_3PT, :decimal
    column :FTA_RATE, :decimal
    column :possessions_total, :decimal
    column :OREB_PCT, :decimal
    column :DREB_PCT, :decimal
    column :REB_PCT, :decimal
    column :AST_PCT, :decimal
    column :USG_PCT, :decimal
    column :USG_PCT_minus_TOV, :decimal
    column :PCT_FGM, :decimal
    column :PCT_FGA, :decimal
    column :PCT_FG3M, :decimal
    column :PCT_FG3A, :decimal
    column :PCT_FTM, :decimal
    column :PCT_FTA, :decimal
    column :PCT_OREB, :decimal
    column :PCT_DREB, :decimal
    column :PCT_REB, :decimal
    column :PCT_AST, :decimal
    column :PCT_TOV, :decimal
    column :PCT_STL, :decimal
    column :PCT_BLK, :decimal
    column :PCT_BLKA, :decimal
    column :PCT_PF, :decimal
    column :PCT_PFD, :decimal
    column :PCT_PTS, :decimal
    column :TO_PCT, :decimal
    column :OFF_RATING, :decimal
    column :DEF_RATING, :decimal
    column :NET_RATING, :decimal
    column :AST_TOV, :decimal
    column :AST_RATIO, :decimal
    column :TO_RATIO, :decimal
    column :PACE, :decimal
    column :PIE, :decimal
    #per-minute stats
    column :FGM_per_min, :decimal
    column :FGA_per_min, :decimal
    column :FG2M_per_min, :decimal
    column :FG2A_per_min, :decimal
    column :FG3M_per_min, :decimal
    column :FG3A_per_min, :decimal
    column :FTM_per_min, :decimal
    column :FTA_per_min, :decimal
    column :PTS_per_min, :decimal
    column :AST_2PM_per_min, :decimal
    column :UAST_2PM_per_min, :decimal
    column :AST_3PM_per_min, :decimal
    column :UAST_3PM_per_min, :decimal
    column :AST_FGM_per_min, :decimal
    column :UAST_FGM_per_min, :decimal
    column :PTS_OFF_TOV_per_min, :decimal
    column :PTS_2ND_CHANCE_per_min, :decimal
    column :PTS_FB_per_min, :decimal
    column :PTS_PAINT_per_min, :decimal

    column :OREB_per_min, :decimal
    column :DREB_per_min, :decimal
    column :REB_per_min, :decimal
    column :AST_per_min, :decimal
    column :STL_per_min, :decimal
    column :BLK_per_min, :decimal
    column :TOV_per_min, :decimal
    column :PF_per_min, :decimal
    column :PLUS_MINUS_per_min, :decimal
    column :BLKA_per_min, :decimal
    column :PFD_per_min, :decimal
    column :DIST_per_min, :decimal
    column :ORBC_per_min, :decimal
    column :DRBC_per_min, :decimal
    column :RBC_per_min, :decimal
    column :TCHS_per_min, :decimal
    column :SAST_per_min, :decimal
    column :FTAST_per_min, :decimal
    column :PASS_per_min, :decimal
    column :CFGM_per_min, :decimal
    column :CFGA_per_min, :decimal
    column :UFGM_per_min, :decimal
    column :UFGA_per_min, :decimal
    column :DFGM_per_min, :decimal
    column :DFGA_per_min, :decimal

    column :CFG_PCT, :decimal
    column :UFG_PCT, :decimal
    column :DFG_PCT, :decimal

    #opponent stats
    column :o_seconds_played_total, :decimal
    column :o_seconds_played_mean, :decimal
    column :o_seconds_played_median, :decimal
    column :o_games_played, :integer
    column :o_wins, :integer
    column :o_losses, :integer
    column :o_ties, :integer
    column :o_win_pct, :decimal

    column :o_FGM_total, :decimal
    column :o_FGM_mean, :decimal
    column :o_FGM_median, :decimal
    column :o_FGA_total, :decimal
    column :o_FGA_mean, :decimal
    column :o_FGA_median, :decimal
    column :o_FG_PCT_total, :decimal
    column :o_FG_PCT, :decimal
    column :o_FG3M_total, :decimal
    column :o_FG3A_total, :decimal
    column :o_FG3_PCT_total, :decimal
    column :o_FG3_PCT, :decimal
    column :o_FG2M_total, :decimal
    column :o_FG2M_mean, :decimal
    column :o_FG2M_median, :decimal
    column :o_FG2A_total, :decimal
    column :o_FG2A_mean, :decimal
    column :o_FG2A_median, :decimal
    column :o_FG2_PCT_total, :decimal
    column :o_FG2_PCT_mean, :decimal
    column :o_FG2_PCT_median, :decimal
    column :o_FG2_PCT, :decimal
    column :o_FTM_total, :decimal
    column :o_FTA_total, :decimal
    column :o_FT_PCT_total, :decimal
    column :o_FT_PCT, :decimal
    column :o_OREB_total, :decimal
    column :o_DREB_total, :decimal
    column :o_REB_total, :decimal
    column :o_AST_total, :decimal
    column :o_STL_total, :decimal
    column :o_BLK_total, :decimal
    column :o_TOV_total, :decimal
    column :o_PF_total, :decimal
    column :o_PTS_total, :decimal
    column :o_PLUS_MINUS_total, :decimal
    column :o_TS_PCT_total, :decimal
    column :o_TS_PCT_mean, :decimal
    column :o_TS_PCT_median, :decimal
    column :o_EFG_PCT_total, :decimal
    column :o_EFG_PCT_mean, :decimal
    column :o_EFG_PCT_median, :decimal
    column :o_PCT_FGA_3PT_total, :decimal
    column :o_PCT_FGA_3PT_mean, :decimal
    column :o_PCT_FGA_3PT_median, :decimal
    #column :o_FG3MAr_total, :decimal
    #column :o_FG3MAr_mean, :decimal
    #column :o_FG3MAr_median, :decimal
    column :o_FTA_RATE_total, :decimal
    column :o_FTA_RATE_mean, :decimal
    column :o_FTA_RATE_median, :decimal
    column :o_OREB_PCT_total, :decimal
    column :o_OREB_PCT_mean, :decimal
    column :o_OREB_PCT_median, :decimal
    column :o_DREB_PCT_total, :decimal
    column :o_DREB_PCT_mean, :decimal
    column :o_DREB_PCT_median, :decimal
    column :o_REB_PCT_total, :decimal
    column :o_REB_PCT_mean, :decimal
    column :o_REB_PCT_median, :decimal
    column :o_AST_PCT_total, :decimal
    column :o_AST_PCT_mean, :decimal
    column :o_AST_PCT_median, :decimal
    column :o_USG_PCT_total, :decimal
    column :o_USG_PCT_mean, :decimal
    column :o_USG_PCT_median, :decimal
    column :o_PCT_FGM_total, :decimal
    column :o_PCT_FGM_mean, :decimal
    column :o_PCT_FGM_median, :decimal
    column :o_PCT_FGA_total, :decimal
    column :o_PCT_FGA_mean, :decimal
    column :o_PCT_FGA_median, :decimal
    column :o_PCT_FG3M_total, :decimal
    column :o_PCT_FG3M_mean, :decimal
    column :o_PCT_FG3M_median, :decimal
    column :o_PCT_FG3A_total, :decimal
    column :o_PCT_FG3A_mean, :decimal
    column :o_PCT_FG3A_median, :decimal
    column :o_PCT_FTM_total, :decimal
    column :o_PCT_FTM_mean, :decimal
    column :o_PCT_FTM_median, :decimal
    column :o_PCT_FTA_total, :decimal
    column :o_PCT_FTA_mean, :decimal
    column :o_PCT_FTA_median, :decimal
    column :o_PCT_OREB_total, :decimal
    column :o_PCT_OREB_mean, :decimal
    column :o_PCT_OREB_median, :decimal
    column :o_PCT_DREB_total, :decimal
    column :o_PCT_DREB_mean, :decimal
    column :o_PCT_DREB_median, :decimal
    column :o_PCT_REB_total, :decimal
    column :o_PCT_REB_mean, :decimal
    column :o_PCT_REB_median, :decimal
    column :o_PCT_AST_total, :decimal
    column :o_PCT_AST_mean, :decimal
    column :o_PCT_AST_median, :decimal
    column :o_PCT_TOV_total, :decimal
    column :o_PCT_TOV_mean, :decimal
    column :o_PCT_TOV_median, :decimal
    column :o_PCT_STL_total, :decimal
    column :o_PCT_STL_mean, :decimal
    column :o_PCT_STL_median, :decimal
    column :o_PCT_BLK_total, :decimal
    column :o_PCT_BLK_mean, :decimal
    column :o_PCT_BLK_median, :decimal
    column :o_PCT_BLKA_total, :decimal
    column :o_PCT_BLKA_mean, :decimal
    column :o_PCT_BLKA_median, :decimal
    column :o_PCT_PF_total, :decimal
    column :o_PCT_PF_mean, :decimal
    column :o_PCT_PF_median, :decimal
    column :o_PCT_PFD_total, :decimal
    column :o_PCT_PFD_mean, :decimal
    column :o_PCT_PFD_median, :decimal
    column :o_PCT_PTS_total, :decimal
    column :o_PCT_PTS_mean, :decimal
    column :o_PCT_PTS_median, :decimal
    column :o_TO_PCT_total, :decimal
    column :o_TO_PCT_mean, :decimal
    column :o_TO_PCT_median, :decimal
    column :o_offensive_possessions_total, :decimal
    column :o_offensive_possessions_mean, :decimal
    column :o_offensive_possessions_median, :decimal
    column :o_defensive_possessions_total, :decimal
    column :o_defensive_possessions_mean, :decimal
    column :o_defensive_possessions_median, :decimal
    column :o_team_FGM_total, :decimal
    column :o_team_FGM_mean, :decimal
    column :o_team_FGM_median, :decimal
    column :o_team_FGA_total, :decimal
    column :o_team_FGA_mean, :decimal
    column :o_team_FGA_median, :decimal
    column :o_team_FG3M_total, :decimal
    column :o_team_FG3M_mean, :decimal
    column :o_team_FG3M_median, :decimal
    column :o_team_FG3A_total, :decimal
    column :o_team_FG3A_mean, :decimal
    column :o_team_FG3A_median, :decimal
    column :o_team_FTM_total, :decimal
    column :o_team_FTM_mean, :decimal
    column :o_team_FTM_median, :decimal
    column :o_team_FTA_total, :decimal
    column :o_team_FTA_mean, :decimal
    column :o_team_FTA_median, :decimal
    column :o_team_OREB_total, :decimal
    column :o_team_OREB_mean, :decimal
    column :o_team_OREB_median, :decimal
    column :o_team_DREB_total, :decimal
    column :o_team_DREB_mean, :decimal
    column :o_team_DREB_median, :decimal
    column :o_team_REB_total, :decimal
    column :o_team_REB_mean, :decimal
    column :o_team_REB_median, :decimal
    column :o_team_AST_total, :decimal
    column :o_team_AST_mean, :decimal
    column :o_team_AST_median, :decimal
    column :o_team_TOV_total, :decimal
    column :o_team_TOV_mean, :decimal
    column :o_team_TOV_median, :decimal
    column :o_team_STL_total, :decimal
    column :o_team_STL_mean, :decimal
    column :o_team_STL_median, :decimal
    column :o_team_BLK_total, :decimal
    column :o_team_BLK_mean, :decimal
    column :o_team_BLK_median, :decimal
    #column :o_team_BLKA_total, :decimal
    #column :o_team_BLKA_mean, :decimal
    #column :o_team_BLKA_median, :decimal
    column :o_team_PF_total, :decimal
    column :o_team_PF_mean, :decimal
    column :o_team_PF_median, :decimal
    #column :o_team_PFD_total, :decimal
    #column :o_team_PFD_mean, :decimal
    #column :o_team_PFD_median, :decimal
    column :o_team_offensive_PTS_total, :decimal
    column :o_team_offensive_PTS_mean, :decimal
    column :o_team_offensive_PTS_median, :decimal
    column :o_team_defensive_PTS_total, :decimal
    column :o_team_defensive_PTS_mean, :decimal
    column :o_team_defensive_PTS_median, :decimal
    column :o_OFF_RATING_total, :decimal
    column :o_OFF_RATING_mean, :decimal
    column :o_OFF_RATING_median, :decimal
    column :o_DEF_RATING_total, :decimal
    column :o_DEF_RATING_mean, :decimal
    column :o_DEF_RATING_median, :decimal
    column :o_NET_RATING_total, :decimal
    column :o_NET_RATING_mean, :decimal
    column :o_NET_RATING_median, :decimal
    column :o_AST_TOV_total, :decimal
    column :o_AST_TOV_mean, :decimal
    column :o_AST_TOV_median, :decimal
    column :o_AST_RATIO_total, :decimal
    column :o_AST_RATIO_mean, :decimal
    column :o_AST_RATIO_median, :decimal
    column :o_PACE_total, :decimal
    column :o_PACE_mean, :decimal
    column :o_PACE_median, :decimal
    column :o_PIE_total, :decimal
    column :o_PIE_mean, :decimal
    column :o_PIE_median, :decimal
    column :o_FG_PCT_mean, :decimal
    column :o_FG_PCT_median, :decimal
    column :o_FG3M_mean, :decimal
    column :o_FG3M_median, :decimal
    column :o_FG3A_mean, :decimal
    column :o_FG3A_median, :decimal
    column :o_FG3_PCT_mean, :decimal
    column :o_FG3_PCT_median, :decimal
    column :o_FTM_mean, :decimal
    column :o_FTM_median, :decimal
    column :o_FTA_mean, :decimal
    column :o_FTA_median, :decimal
    column :o_FT_PCT_mean, :decimal
    column :o_FT_PCT_median, :decimal
    column :o_OREB_mean, :decimal
    column :o_OREB_median, :decimal
    column :o_DREB_mean, :decimal
    column :o_DREB_median, :decimal
    column :o_REB_mean, :decimal
    column :o_REB_median, :decimal
    column :o_AST_mean, :decimal
    column :o_AST_median, :decimal
    column :o_STL_mean, :decimal
    column :o_STL_median, :decimal
    column :o_BLK_mean, :decimal
    column :o_BLK_median, :decimal
    column :o_TOV_mean, :decimal
    column :o_TOV_median, :decimal
    column :o_PF_mean, :decimal
    column :o_PF_median, :decimal
    column :o_PTS_mean, :decimal
    column :o_PTS_median, :decimal
    column :o_PLUS_MINUS_mean, :decimal
    column :o_PLUS_MINUS_median, :decimal
    #misc
    #column :o_PTS_OFF_TOV_total, :decimal
    #column :o_PTS_OFF_TOV_mean, :decimal
    #column :o_PTS_OFF_TOV_median, :decimal
    column :o_PTS_2ND_CHANCE_total, :decimal
    column :o_PTS_2ND_CHANCE_mean, :decimal
    column :o_PTS_2ND_CHANCE_median, :decimal
    #column :o_PTS_FB_total, :decimal
    #column :o_PTS_FB_mean, :decimal
    #column :o_PTS_FB_median, :decimal
    #column :o_PTS_PAINT_total, :decimal
    #column :o_PTS_PAINT_mean, :decimal
    #column :o_PTS_PAINT_median, :decimal
    #scoring
    #column :o_PCT_FGA_2PT_total, :decimal
    #column :o_PCT_FGA_2PT_mean, :decimal
    #column :o_PCT_FGA_2PT_median, :decimal
    column :o_PCT_PTS_2PT_total, :decimal
    column :o_PCT_PTS_2PT_mean, :decimal
    column :o_PCT_PTS_2PT_median, :decimal
    column :o_PCT_PTS_2PT_MR_total, :decimal
    column :o_PCT_PTS_2PT_MR_mean, :decimal
    column :o_PCT_PTS_2PT_MR_median, :decimal
    column :o_PCT_PTS_3PT_total, :decimal
    column :o_PCT_PTS_3PT_mean, :decimal
    column :o_PCT_PTS_3PT_median, :decimal
    column :o_PCT_PTS_FB_total, :decimal
    column :o_PCT_PTS_FB_mean, :decimal
    column :o_PCT_PTS_FB_median, :decimal
    column :o_PCT_PTS_FT_total, :decimal
    column :o_PCT_PTS_FT_mean, :decimal
    column :o_PCT_PTS_FT_median, :decimal
    column :o_PCT_PTS_OFF_TOV_total, :decimal
    column :o_PCT_PTS_OFF_TOV_mean, :decimal
    column :o_PCT_PTS_OFF_TOV_median, :decimal
    column :o_PCT_PTS_PAINT_total, :decimal
    column :o_PCT_PTS_PAINT_mean, :decimal
    column :o_PCT_PTS_PAINT_median, :decimal
    column :o_AST_2PM_total, :decimal
    column :o_AST_2PM_mean, :decimal
    column :o_AST_2PM_median, :decimal
    column :o_PCT_AST_2PM_total, :decimal
    column :o_PCT_AST_2PM_mean, :decimal
    column :o_PCT_AST_2PM_median, :decimal
    column :o_UAST_2PM_total, :decimal
    column :o_UAST_2PM_mean, :decimal
    column :o_UAST_2PM_median, :decimal
    column :o_PCT_UAST_2PM_total, :decimal
    column :o_PCT_UAST_2PM_mean, :decimal
    column :o_PCT_UAST_2PM_median, :decimal
    column :o_AST_3PM_total, :decimal
    column :o_AST_3PM_mean, :decimal
    column :o_AST_3PM_median, :decimal
    column :o_PCT_AST_3PM_total, :decimal
    column :o_PCT_AST_3PM_mean, :decimal
    column :o_PCT_AST_3PM_median, :decimal
    column :o_UAST_3PM_total, :decimal
    column :o_UAST_3PM_mean, :decimal
    column :o_UAST_3PM_median, :decimal
    column :o_PCT_UAST_3PM_total, :decimal
    column :o_PCT_UAST_3PM_mean, :decimal
    column :o_PCT_UAST_3PM_median, :decimal
    column :o_AST_FGM_total, :decimal
    column :o_AST_FGM_mean, :decimal
    column :o_AST_FGM_median, :decimal
    column :o_PCT_AST_FGM_total, :decimal
    column :o_PCT_AST_FGM_mean, :decimal
    column :o_PCT_AST_FGM_median, :decimal
    column :o_UAST_FGM_total, :decimal
    column :o_UAST_FGM_mean, :decimal
    column :o_UAST_FGM_median, :decimal
    column :o_PCT_UAST_FGM_total, :decimal
    column :o_PCT_UAST_FGM_mean, :decimal
    column :o_PCT_UAST_FGM_median, :decimal

    column :o_PCT_AST_2PM, :decimal
    column :o_PCT_UAST_2PM, :decimal
    column :o_PCT_AST_3PM, :decimal
    column :o_PCT_UAST_3PM, :decimal
    column :o_PCT_AST_FGM, :decimal
    column :o_PCT_UAST_FGM, :decimal
    column :o_PCT_FGA_2PT, :decimal
    column :o_PCT_PTS_2PT, :decimal
    column :o_PCT_PTS_2PT_MR, :decimal
    column :o_PCT_PTS_3PT, :decimal
    column :o_PCT_PTS_FB, :decimal
    column :o_PCT_PTS_FT, :decimal
    column :o_PCT_PTS_OFF_TOV, :decimal
    column :o_PCT_PTS_PAINT, :decimal

    #column :o_FGA_2PT_total, :decimal
    #column :o_FGA_2PT_mean, :decimal
    #column :o_FGA_2PT_median, :decimal
    #column :o_PTS_2PT_total, :decimal
    #column :o_PTS_2PT_mean, :decimal
    #column :o_PTS_2PT_median, :decimal
    column :o_PTS_2PT_MR_total, :decimal
    column :o_PTS_2PT_MR_mean, :decimal
    column :o_PTS_2PT_MR_median, :decimal
    #column :o_PTS_3PT_total, :decimal
    #column :o_PTS_3PT_mean, :decimal
    #column :o_PTS_3PT_median, :decimal
    #column :o_PTS_FT_total, :decimal
    #column :o_PTS_FT_mean, :decimal
    #column :o_PTS_FT_median, :decimal
    column :o_PTS_FB_total, :decimal
    column :o_PTS_FB_mean, :decimal
    column :o_PTS_FB_median, :decimal
    column :o_PTS_OFF_TOV_total, :decimal
    column :o_PTS_OFF_TOV_mean, :decimal
    column :o_PTS_OFF_TOV_median, :decimal
    column :o_PTS_PAINT_total, :decimal
    column :o_PTS_PAINT_mean, :decimal
    column :o_PTS_PAINT_median, :decimal
    column :o_BLKA_total, :decimal
    column :o_BLKA_mean, :decimal
    column :o_BLKA_median, :decimal
    column :o_PFD_total, :decimal
    column :o_PFD_mean, :decimal
    column :o_PFD_median, :decimal

    #tracking
    column :o_DIST_total, :decimal
    column :o_DIST_mean, :decimal
    column :o_DIST_median, :decimal
    column :o_ORBC_total, :decimal
    column :o_ORBC_mean, :decimal
    column :o_ORBC_median, :decimal
    column :o_DRBC_total, :decimal
    column :o_DRBC_mean, :decimal
    column :o_DRBC_median, :decimal
    column :o_RBC_total, :decimal
    column :o_RBC_mean, :decimal
    column :o_RBC_median, :decimal
    column :o_TCHS_total, :decimal
    column :o_TCHS_mean, :decimal
    column :o_TCHS_median, :decimal
    column :o_SAST_total, :decimal
    column :o_SAST_mean, :decimal
    column :o_SAST_median, :decimal
    column :o_FTAST_total, :decimal
    column :o_FTAST_mean, :decimal
    column :o_FTAST_median, :decimal
    column :o_PASS_total, :decimal
    column :o_PASS_mean, :decimal
    column :o_PASS_median, :decimal
    column :o_CFGM_total, :decimal
    column :o_CFGM_mean, :decimal
    column :o_CFGM_median, :decimal
    column :o_CFGA_total, :decimal
    column :o_CFGA_mean, :decimal
    column :o_CFGA_median, :decimal
    column :o_CFG_PCT_total, :decimal
    column :o_CFG_PCT_mean, :decimal
    column :o_CFG_PCT_median, :decimal
    column :o_UFGM_total, :decimal
    column :o_UFGM_mean, :decimal
    column :o_UFGM_median, :decimal
    column :o_UFGA_total, :decimal
    column :o_UFGA_mean, :decimal
    column :o_UFGA_median, :decimal
    column :o_UFG_PCT_total, :decimal
    column :o_UFG_PCT_mean, :decimal
    column :o_UFG_PCT_median, :decimal
    column :o_DFGM_total, :decimal
    column :o_DFGM_mean, :decimal
    column :o_DFGM_median, :decimal
    column :o_DFGA_total, :decimal
    column :o_DFGA_mean, :decimal
    column :o_DFGA_median, :decimal
    column :o_DFG_PCT_total, :decimal
    column :o_DFG_PCT_mean, :decimal
    column :o_DFG_PCT_median, :decimal

    column :o_CFG_PCT, :decimal
    column :o_UFG_PCT, :decimal
    column :o_DFG_PCT, :decimal
    #these are aggregate stats
    column :o_TS_PCT, :decimal
    column :o_EFG_PCT, :decimal
    column :o_PCT_FGA_3PT, :decimal
    column :o_FTA_RATE, :decimal
    column :o_possessions_total, :decimal
    column :o_OREB_PCT, :decimal
    column :o_DREB_PCT, :decimal
    column :o_REB_PCT, :decimal
    column :o_AST_PCT, :decimal
    column :o_USG_PCT, :decimal
    column :o_PCT_FGM, :decimal
    column :o_PCT_FGA, :decimal
    column :o_PCT_FG3M, :decimal
    column :o_PCT_FG3A, :decimal
    column :o_PCT_FTM, :decimal
    column :o_PCT_FTA, :decimal
    column :o_PCT_OREB, :decimal
    column :o_PCT_DREB, :decimal
    column :o_PCT_REB, :decimal
    column :o_PCT_AST, :decimal
    column :o_PCT_TOV, :decimal
    column :o_PCT_STL, :decimal
    column :o_PCT_BLK, :decimal
    column :o_PCT_BLKA, :decimal
    column :o_PCT_PF, :decimal
    column :o_PCT_PFD, :decimal
    column :o_PCT_PTS, :decimal
    column :o_TO_PCT, :decimal
    column :o_OFF_RATING, :decimal
    column :o_DEF_RATING, :decimal
    column :o_NET_RATING, :decimal
    column :o_AST_TOV, :decimal
    column :o_AST_RATIO, :decimal
    column :o_TO_RATIO, :decimal
    column :o_PACE, :decimal
    column :o_PIE, :decimal

    #per-minute stats
    column :o_FGM_per_min, :decimal
    column :o_FGA_per_min, :decimal
    column :o_FG2M_per_min, :decimal
    column :o_FG2A_per_min, :decimal
    column :o_FG3M_per_min, :decimal
    column :o_FG3A_per_min, :decimal
    column :o_FTM_per_min, :decimal
    column :o_FTA_per_min, :decimal
    column :o_PTS_per_min, :decimal
    column :o_AST_2PM_per_min, :decimal
    column :o_UAST_2PM_per_min, :decimal
    column :o_AST_3PM_per_min, :decimal
    column :o_UAST_3PM_per_min, :decimal
    column :o_AST_FGM_per_min, :decimal
    column :o_UAST_FGM_per_min, :decimal
    column :o_PTS_OFF_TOV_per_min, :decimal
    column :o_PTS_2ND_CHANCE_per_min, :decimal
    column :o_PTS_FB_per_min, :decimal
    column :o_PTS_PAINT_per_min, :decimal

    column :o_OREB_per_min, :decimal
    column :o_DREB_per_min, :decimal
    column :o_REB_per_min, :decimal
    column :o_AST_per_min, :decimal
    column :o_STL_per_min, :decimal
    column :o_BLK_per_min, :decimal
    column :o_TOV_per_min, :decimal
    column :o_PF_per_min, :decimal
    column :o_PLUS_MINUS_per_min, :decimal
    column :o_BLKA_per_min, :decimal
    column :o_PFD_per_min, :decimal
    column :o_DIST_per_min, :decimal
    column :o_ORBC_per_min, :decimal
    column :o_DRBC_per_min, :decimal
    column :o_RBC_per_min, :decimal
    column :o_TCHS_per_min, :decimal
    column :o_SAST_per_min, :decimal
    column :o_FTAST_per_min, :decimal
    column :o_PASS_per_min, :decimal
    column :o_CFGM_per_min, :decimal
    column :o_CFGA_per_min, :decimal
    column :o_UFGM_per_min, :decimal
    column :o_UFGA_per_min, :decimal
    column :o_DFGM_per_min, :decimal
    column :o_DFGA_per_min, :decimal
    
    #_XY table
    column :b2b, :text
    column :num_games, :text
    column :num_b2b_games, :text
    column :num_non_b2b_games, :text
    column :front_b2b, :text
    column :num_front_b2b_games, :text
    column :num_non_front_b2b_games, :text
    column :opp_b2b, :text
    column :num_opp_b2b_games, :text
    column :num_opp_non_b2b_games, :text
    column :opp_front_b2b, :text
    column :num_opp_front_b2b_games, :text
    column :num_opp_non_front_b2b_games, :text
    column :num_home_games, :text
    column :num_away_games, :text
    column :threeg4d, :text
    column :num_threeg4d_games, :text
    column :extra_rest, :text
    column :num_extra_rest_games, :text
    column :opp_threeg4d, :text
    column :num_opp_threeg4d_games, :text
    column :opp_extra_rest, :text
    column :num_opp_extra_rest_games, :text
    column :location, :text
    column :starter, :text
    column :num_plus, :text

    column :pace_ratio, :text
    column :pace_ratio2, :text
    column :pace_ratio3, :text
    column :expected_PTS_pace, :text
    column :expected_PTS_pace2, :text
    column :expected_PTS_pace3, :text
    column :expected_PTS_def_rtg, :text
    column :opp_plus_def_rtg, :text

    column :non_front_b2b_OREB, :text
    column :non_front_b2b_DREB, :text
    column :non_front_b2b_BLK, :text
    column :non_front_b2b_AST, :text
    column :non_front_b2b_STL, :text
    column :non_front_b2b_TOV, :text

    column :actual_PTS, :text
      column :prev_mean_PTS, :text

      column :prev_mean_FTM, :text
      column :prev_mean_FTA, :text
      column :prev_FG3A, :decimal
      column :prev_FG3M, :decimal
      column :prev_FG3_PCT, :decimal
      column :prev_opp_o_FG3M_mean, :decimal
      column :prev_opp_o_FG3A_mean, :decimal
      column :prev_opp_o_FG3_PCT, :decimal
      column :prev_opp_o_FG3M_mean_v_position, :decimal
      column :prev_opp_o_FG3A_mean_v_position, :decimal
      column :prev_opp_o_FG3_PCT_v_position, :decimal
      column :league_average_FG3A, :decimal
      column :league_average_FG3M, :decimal
      column :league_average_FG3_PCT, :decimal
      column :league_average_FG3A_v_position, :decimal
      column :league_average_FG3M_v_position, :decimal
      column :league_average_FG3_PCT_v_position, :decimal
      column :prev_o_team_FG2M_mean, :decimal
      column :prev_o_team_FG2A_mean, :decimal
      column :prev_PFD_mean, :decimal
      column :prev_team_PFD_mean, :decimal
      column :prev_o_team_PF_mean, :decimal
      column :prev_o_team_PF_mean_v_position, :decimal
      column :league_average_PF, :decimal
      column :league_average_PFD_v_position, :decimal
      column :prev_team_FTA_mean, :decimal
      column :prev_team_FTA, :decimal
      column :prev_o_team_FTA, :decimal
      column :prev_o_team_FTA_v_position, :decimal
      column :league_average_FTA, :decimal
      column :league_average_FTA_v_position, :decimal
      column :league_average_TS_PCT, :decimal
      column :league_average_TS_PCT_v_position, :decimal
      column :prev_o_team_TS_PCT, :decimal
      column :prev_o_team_TS_PCT_v_position, :decimal
      column :prev_TS_PCT, :decimal
      column :league_average_pct_pts_2pt, :decimal
      column :league_average_pct_pts_2pt_mr, :decimal
      column :league_average_pct_pts_3pt, :decimal
      column :league_average_pct_pts_ft, :decimal
      column :point_spread_mean, :decimal
      column :est_vegas_team_PTS, :decimal
      column :est_vegas_opp_PTS, :decimal
      bookies = database[ :"#{type}_bettinglines"].distinct.select(:bookname).entries
      bookies.each{|bookie|
        book = bookie[:bookname]
        column :"point_spread_#{book}", :text
        column :"est_vegas_team_PTS_#{book}", :text
        column :"est_vegas_opp_PTS_#{book}", :text
      }

      column :prev_PTS_OFF_TOV_mean, :decimal
      column :team_PTS_OFF_TOV_mean, :decimal
      column :opp_o_PTS_OFF_TOV_mean, :decimal
      column :league_average_PTS_OFF_TOV, :decimal
      column :opp_o_PTS_OFF_TOV_mean_v_position, :decimal
      column :league_average_PTS_OFF_TOV_v_position, :decimal
      column :prev_opp_o_PCT_PTS_OFF_TOV, :decmial

      column :prev_PTS_2ND_CHANCE_mean, :decmial
      column :team_PTS_2ND_CHANCE_mean, :decmial
      column :opp_o_PTS_2ND_CHANCE_mean, :decmial
      column :league_average_PTS_2ND_CHANCE, :decmial
      column :opp_o_PTS_2ND_CHANCE_mean_v_position, :decmial
      column :league_average_PTS_2ND_CHANCE_v_position, :decmial
      column :prev_opp_o_PCT_PTS_2ND_CHANCE, :decmial

      column :prev_PTS_FB_mean, :decmial
      column :team_PTS_FB_mean, :decmial
      column :opp_o_PTS_FB_mean, :decmial
      column :league_average_PTS_FB, :decmial
      column :opp_o_PTS_FB_mean_v_position, :decmial
      column :league_average_PTS_FB_v_position, :decmial
      column :prev_opp_o_PCT_PTS_FB, :decmial

      column :prev_PTS_PAINT_mean, :decmial
      column :team_PTS_PAINT_mean, :decmial
      column :opp_o_PTS_PAINT_mean, :decmial
      column :league_average_PTS_PAINT, :decmial
      column :opp_o_PTS_PAINT_mean_v_position, :decmial
      column :league_average_PTS_PAINT_v_position, :decmial
      column :prev_opp_o_PCT_PTS_PAINT, :decmial

      column :prev_CFG_PCT_mean, :decimal
      column :prev_PCT_FGM_UFG, :decimal
      column :prev_PCT_FGM_CFG, :decimal
      column :prev_PCT_FGA_UFG, :decimal
      column :prev_PCT_FGA_CFG, :decimal
      column :mean_front_b2b_PTS, :text
      column :mean_non_front_b2b_PTS, :text
      column :mean_threeg4d_PTS, :text
      column :mean_extra_rest_PTS, :text
      column :mean_opp_b2b_PTS, :text
      column :mean_opp_non_b2b_PTS, :text
      column :mean_opp_front_b2b_PTS, :text
      column :mean_opp_non_front_b2b_PTS, :text
      column :mean_opp_threeg4d_PTS, :text
      column :mean_opp_extra_rest_PTS, :text
    column :actual_OREB, :text
      column :mean_OREB, :text
      column :team_OREB, :text
      column :e_OREB_PCT, :text
      column :e_o_DREB, :text
      column :e_o_DREB_PCT, :text
      column :league_average_OREB_v_position, :text
      column :o_OREB_PCT_v_position, :text
    column :actual_DREB, :text
      column :mean_DREB, :text
      column :team_DREB, :text
      column :e_DREB_PCT, :text
      column :e_o_OREB, :text
      column :e_o_OREB_PCT, :text
      column :o_DREB_v_position, :text
      column :league_average_DREB_v_position, :text
      column :o_DREB_PCT_v_position, :text
      column :league_average_DREB_PCT_v_position, :text
    column :actual_STL, :text
      column :mean_STL, :text
      column :mean_home_STL, :text
      column :mean_away_STL, :text
      column :mean_b2b_STL, :text
      column :mean_non_b2b_STL, :text
      column :mean_front_b2b_STL, :text
      column :mean_non_front_b2b_STL, :text
      column :mean_threeg4d_STL, :text
      column :mean_extra_rest_STL, :text
      column :mean_opp_b2b_STL, :text
      column :mean_opp_non_b2b_STL, :text
      column :mean_opp_front_b2b_STL, :text
      column :mean_opp_non_front_b2b_STL, :text
      column :mean_opp_threeg4d_STL, :text
      column :mean_opp_extra_rest_STL, :text
      column :mean_team_STL, :text
      column :opponent_o_team_STL, :text
      column :opponent_o_team_STL_v_position, :text
      column :team_PCT_STL, :text
      column :o_mean_PCT_STL, :text
      column :league_average_PCT_STL, :text
      column :mean_PCT_STL, :text
      column :o_team_STL_v_position, :text
      column :o_team_PCT_STL_v_position, :text
      column :league_average_PCT_STL_v_position, :text
    column :actual_AST, :text
      column :mean_AST, :text
      column :mean_b2b_AST, :text
      column :mean_non_b2b_AST, :text
      column :mean_front_b2b_AST, :text
      column :mean_non_front_b2b_AST, :text
      column :mean_threeg4d_AST, :text
      column :mean_extra_rest_AST, :text
      column :mean_opp_b2b_AST, :text
      column :mean_opp_non_b2b_AST, :text
      column :mean_opp_front_b2b_AST, :text
      column :mean_opp_non_front_b2b_AST, :text
      column :mean_opp_threeg4d_AST, :text
      column :mean_opp_extra_rest_AST, :text
      column :mean_team_AST, :text
      column :opponent_o_team_AST, :text
      column :team_AST_PCT, :text
      column :o_mean_AST_PCT, :text
      column :mean_AST_PCT, :text
      column :o_team_AST_v_position, :text
      column :mean_AST_RATIO, :text
    column :actual_BLK, :text
      column :mean_BLK, :text
      column :mean_home_BLK, :text
      column :mean_away_BLK, :text
      column :mean_extra_rest_BLK, :text
      column :mean_opp_b2b_BLK, :text
      column :mean_opp_non_b2b_BLK, :text
      column :mean_opp_front_b2b_BLK, :text
      column :mean_opp_non_front_b2b_BLK, :text
      column :mean_opp_threeg4d_BLK, :text
      column :mean_opp_extra_rest_BLK, :text
      column :mean_team_BLK, :text
      column :opponent_o_team_BLK, :text
      column :opponent_o_team_BLK_v_position, :text
      column :team_PCT_BLK, :text
      column :o_mean_PCT_BLK, :text
      column :league_average_PCT_BLK, :text
      column :mean_PCT_BLK, :text
      column :o_team_BLK_v_position, :text
      column :o_team_PCT_BLK_v_position, :text
      column :league_average_PCT_BLK_v_position, :text
    column :actual_TOV, :text
      column :mean_TOV, :text
      column :mean_b2b_TOV, :text
      column :mean_non_b2b_TOV, :text
      column :mean_front_b2b_TOV, :text
      column :mean_non_front_b2b_TOV, :text
      column :mean_threeg4d_TOV, :text
      column :mean_extra_rest_TOV, :text
      column :mean_opp_b2b_TOV, :text
      column :mean_opp_non_b2b_TOV, :text
      column :mean_opp_front_b2b_TOV, :text
      column :mean_opp_non_front_b2b_TOV, :text
      column :mean_opp_threeg4d_TOV, :text
      column :mean_opp_extra_rest_TOV, :text
      column :mean_team_TOV, :text
      column :opponent_o_team_TOV, :text
      column :opponent_o_team_TOV_v_position, :text
      column :team_TO_PCT, :text
      column :o_mean_TO_PCT, :text
      column :mean_TO_PCT, :text
      column :o_team_TO_v_position, :text
      column :o_team_TO_PCT_v_position, :text
    column :actual_SECONDS, :text
      column :mean_seconds, :text
      column :mean_home_seconds, :text
      column :mean_away_seconds, :text
      column :mean_b2b_seconds, :text
      column :mean_non_b2b_seconds, :text
      column :mean_front_b2b_seconds, :text
      column :mean_non_front_b2b_seconds, :text
      column :mean_threeg4d_seconds, :text
      column :mean_extra_rest_seconds, :text
      column :mean_opp_b2b_seconds, :text
      column :mean_opp_non_b2b_seconds, :text
      column :mean_opp_front_b2b_seconds, :text
      column :mean_opp_non_front_b2b_seconds, :text
      column :mean_opp_threeg4d_seconds, :text
      column :mean_opp_extra_rest_seconds, :text
      column :mean_team_SECONDS, :text
      #column :win_pct, :text
      #personal fouls
      #opp personal fouls drawn, and by position
      #opp FTs, FTrate
      #opp FTs by position
      column :opp_win_pct, :text
      column :win_pct_locale, :text
      column :opp_win_pct_locale, :text
      column :league_average_SECONDS_v_position, :text

      column :b2b_opp_avg, :text
      column :non_b2b_opp_avg, :text
      column :both_avg, :text
      column :threeg4d_avg, :text
      column :opp_threeg4d_avg, :text
      column :extra_rest_avg, :text
      column :opp_extra_rest_avg, :text
      column :threeg4d_OREB, :text
      column :opp_threeg4d_OREB, :text
      column :extra_rest_OREB, :text
      column :opp_extra_rest_OREB, :text
      column :threeg4d_DREB, :text
      column :opp_threeg4d_DREB, :text
      column :extra_rest_DREB, :text
      column :opp_extra_rest_DREB, :text
      column :threeg4d_STL, :text
      column :opp_threeg4d_STL, :text
      column :extra_rest_STL, :text
      column :opp_extra_rest_STL, :text
      column :threeg4d_AST, :text
      column :opp_threeg4d_AST, :text
      column :extra_rest_AST, :text
      column :opp_extra_rest_AST, :text
      column :threeg4d_BLK, :text
      column :opp_threeg4d_BLK, :text
      column :extra_rest_BLK, :text
      column :opp_extra_rest_BLK, :text
      column :threeg4d_TOV, :text
      column :opp_threeg4d_TOV, :text
      column :extra_rest_TOV, :text
      column :opp_extra_rest_TOV, :text
      column :threeg4d_SECONDS, :text
      column :opp_threeg4d_SECONDS, :text
      column :extra_rest_SECONDS, :text
      column :opp_extra_rest_SECONDS, :text

      column :extra_rest_PTS, :text
      column :opp_extra_rest_PTS, :text

      column :in_season_mean_PTS_v_team, :text
      column :in_season_mean_OREB_v_team, :text
      column :in_season_mean_DREB_v_team, :text
      column :in_season_mean_STL_v_team, :text
      column :in_season_mean_AST_v_team, :text
      column :in_season_mean_BLK_v_team, :text
      column :in_season_mean_TOV_v_team, :text
      column :in_season_mean_SECONDS_v_team, :text

      column :prev_year_mean_OREB_v_team, :text
      column :prev_year_mean_DREB_v_team, :text
      column :prev_year_mean_STL_v_team, :text
      column :prev_year_mean_AST_v_team, :text
      column :prev_year_mean_BLK_v_team, :text
      column :prev_year_mean_TOV_v_team, :text
      column :prev_year_mean_SECONDS_v_team, :text

      column :prev_year_2_mean_PTS_v_team, :text
      column :prev_year_2_mean_OREB_v_team, :text
      column :prev_year_2_mean_DREB_v_team, :text
      column :prev_year_2_mean_STL_v_team, :text
      column :prev_year_2_mean_AST_v_team, :text
      column :prev_year_2_mean_BLK_v_team, :text
      column :prev_year_2_mean_TOV_v_team, :text
      column :prev_year_2_mean_SECONDS_v_team, :text

      column :prev_year_3_mean_PTS_v_team, :text
      column :prev_year_3_mean_OREB_v_team, :text
      column :prev_year_3_mean_DREB_v_team, :text
      column :prev_year_3_mean_STL_v_team, :text
      column :prev_year_3_mean_AST_v_team, :text
      column :prev_year_3_mean_BLK_v_team, :text
      column :prev_year_3_mean_TOV_v_team, :text
      column :prev_year_3_mean_SECONDS_v_team, :text
  end
end

def testData( row, row_home, row_away, row_starter, row_bench, row_zero, row_one, row_two, row_three, row_six, bTeam )
  if true == bTeam
    types = ["reg", "adv", "four", "misc", "scoring", "opp", "tracking"]
  else
    types = ["reg", "adv", "misc", "scoring", "usage", "tracking"]
  end
  types.each{|type|
    puts "\n#{type}"
    printTotals( row, type, bTeam )

    puts "\nHOME"
    printTotals( row_home, type, bTeam )

    puts "\nAWAY"
    printTotals( row_away, type, bTeam )
=begin
    puts "\nprevious 5"
    if row_prev5
      row_prev5 = prev_5_splitSet.split.to_h
      if true == bTeam
        row_prev5 = row_prev5.merge( o_prev_5_splitSet.split.to_opponent_h )
      end
      row_prev5[:average_type] = "prev5"
      printTotals( row_prev5, type, bTeam )
    end
=end
    if false == bTeam
      puts "\nSTARTER"
      printTotals( row_starter, type, bTeam )

      puts "\nBENCH"
      printTotals( row_bench, type, bTeam )
    end

    puts "\n0 DAYS REST"
    printTotals( row_zero, type, bTeam )

    puts "\n1 DAYS REST"
    printTotals( row_one, type, bTeam )

    puts "\n2 DAYS REST"
    printTotals( row_two, type, bTeam )

    puts "\n3 DAYS REST"
    printTotals( row_three, type, bTeam )

    puts "\n6 DAYS REST"
    printTotals( row_six, type, bTeam )

    #puts "\n3 in 4 nights"
    #printTotals( row_three_in_four, type, bTeam )

    #puts "\n4 in 6 nights"
    #printTotals( row_four_in_six, type, bTeam )
  }
end

def createChunk( array, threadIndex, numThreads )
  ind = threadIndex
  a = Array.new

  while ind < array.size
    a.push array[ ind ][0]
    ind = ind + numThreads
  end
  
  return a
end

def calculateDailyAverages( seasons_h, season, type, database, bCalcPlayers )
  createVegasLinesTable( database, season, type )

  tablename = "_" + season + " " + type + " daily averages"
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

  options = { :headers    => true,
              :header_converters => nil,
              :converters => :all  }

  #if (nil == PLAYER_SKIP and ( not(NUM_THREADS and THREAD_INDEX) )) and !database.table_exists? tablename
  if !database.table_exists? tablename
    create_daily_averages_table( database, type, tablename )
  end

  season_str = season.gsub(/-/,"_") + "_regularseason"

  season_start = Date.parse( seasons_h[season][0] )
  season_end = Date.parse( seasons_h[season][1] )

  player_tables = [ "advanced_PlayerStats", "fourfactors_sqlPlayersFourFactors", "misc_sqlPlayersMisc", "playertrack_PlayerTrack", "scoring_sqlPlayersScoring", "traditional_PlayerStats", "usage_sqlPlayersUsage" ]
  team_tables = [ "advanced_TeamStats", "fourfactors_sqlTeamsFourFactors", "misc_sqlTeamsMisc", "playertrack_PlayerTrackTeam", "scoring_sqlTeamsScoring", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlTeamsUsage" ]

  teams = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{team_tables[6]}" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:TEAM_ABBREVIATION).entries
  players = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{player_tables[5]}" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:PLAYER_ID, :PLAYER_NAME).entries
  #addPlayerIDToFanduel( players )

  bTeam = true 

  bookies = database[ :"regularseason_bettinglines"].where( Sequel.function(:count, :bookname).distinct )

  entities = teams
  if true == bCalcPlayers
    if nil != PLAYER_SKIP
      binding.pry
      entities = players[PLAYER_SKIP...(players.size)]
      p "processing players from 0...#{players.size}"
    elsif NUM_THREADS and THREAD_INDEX
      if nil
        start_index = (0 == THREAD_INDEX) ? 0 :  getEndIndex( THREAD_INDEX - 1, NUM_THREADS, players.size )
        end_index = getEndIndex( THREAD_INDEX, NUM_THREADS, players.size )

        entities = players[ start_index...end_index ]
      else
        playerSizes = Hash.new
        players.each_with_index{|player,i|
          entries = database[:"#{season_str}_traditional_PlayerStats"].select_all.where(:PLAYER_ID => player[:PLAYER_ID]).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries;
          playerSizes[player] = entries.size;
        };
        playerSizes = playerSizes.sort_by{|k,v| v}.reverse
        entities = createChunk( playerSizes, THREAD_INDEX, NUM_THREADS)
      end
    else
      entities = players
    end
  end

  entities.each_with_index{|entity,entity_index|
    opponents = Hash.new
    teams.each{|team|
      opponents[ team[:TEAM_ABBREVIATION] ] = SplitSet.new
    }
    p "doing entity #{entity_index} / #{entities.size}.  #{entity}"

    if entity[:PLAYER_NAME]
      bTeam = false
    elsif entity[:TEAM_ABBREVIATION] == "WST" or entity[:TEAM_ABBREVIATION] == "EST"
      p "all star roster, skip"
      next
    end

    last_boxscore = Hash.new;last_boxscore_home = nil; last_boxscore_away = nil;last_boxscore_starter = nil;last_boxscore_bench = nil;last_boxscore_prev2 = nil;last_boxscore_prev5 = nil;last_boxscore_three_in_four = nil;last_boxscore_four_in_six = nil;last_boxscore_zero = nil;last_boxscore_one = nil;last_boxscore_two = nil;last_boxscore_three = nil;last_boxscore_four = nil;last_boxscore_five = nil;last_boxscore_six = nil; 
    #last_boxscore_opp = nil

    if true == bTeam
      boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStats" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).order(:DATE).entries
      boxscores_advanced = database[ :"#{season.gsub(/-/,"_")}_#{type}_advanced_TeamStats" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).order(:DATE).entries
      boxscores_fourfactors = database[ :"#{season.gsub(/-/,"_")}_#{type}_fourfactors_sqlTeamsFourFactors" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).order(:DATE).entries
      boxscores_scoring = database[ :"#{season.gsub(/-/,"_")}_#{type}_scoring_sqlTeamsScoring" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).order(:DATE).entries
      boxscores_usage = database[ :"#{season.gsub(/-/,"_")}_#{type}_usage_sqlTeamsUsage" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).order(:DATE).entries
      boxscores_misc = database[ :"#{season.gsub(/-/,"_")}_#{type}_misc_sqlTeamsMisc" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).order(:DATE).entries
      #boxscores_tracking = database[ :"#{season.gsub(/-/,"_")}_#{type}_playertrack_PlayerTrackTeam" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).order(:DATE).entries
      boxscores_tracking = database[ :"#{season.gsub(/-/,"_")}_#{type}_playertrack_TeamStats" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).order(:DATE).entries

      betting_lines = database[ :"regularseason_bettinglines" ].where(:home => entity[:TEAM_ABBREVIATION], :final => "1").or(:away => entity[:TEAM_ABBREVIATION], :final => "1").entries
      if boxscores.size != boxscores_advanced.size or boxscores.size != boxscores_advanced.size or boxscores.size != boxscores_fourfactors.size or boxscores.size != boxscores_scoring.size or boxscores.size != boxscores_usage.size or boxscores.size != boxscores_misc.size or boxscores.size != boxscores_tracking.size
        binding.pry
        p "mismatch boxscores"
      elsif 82 != boxscores.size
        binding.pry
        p "#{boxscores.size} boxscores"
      end
    else
      boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:PLAYER_NAME => entity[:PLAYER_NAME]).exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").exclude(:MIN => nil).exclude(:MIN => "0:00").order(:DATE).entries
      boxscores_advanced = database[ :"#{season.gsub(/-/,"_")}_#{type}_advanced_PlayerStats" ].where(:PLAYER_NAME => entity[:PLAYER_NAME]).exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").exclude(:MIN => nil).exclude(:MIN => "0:00").order(:DATE).entries
      boxscores_fourfactors = database[ :"#{season.gsub(/-/,"_")}_#{type}_fourfactors_sqlPlayersFourFactors" ].where(:PLAYER_NAME => entity[:PLAYER_NAME]).exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").exclude(:MIN => nil).exclude(:MIN => "0:00").order(:DATE).entries
      boxscores_scoring = database[ :"#{season.gsub(/-/,"_")}_#{type}_scoring_sqlPlayersScoring" ].where(:PLAYER_NAME => entity[:PLAYER_NAME]).exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").exclude(:MIN => nil).exclude(:MIN => "0:00").order(:DATE).entries
      boxscores_usage = database[ :"#{season.gsub(/-/,"_")}_#{type}_usage_sqlPlayersUsage" ].where(:PLAYER_NAME => entity[:PLAYER_NAME]).exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").exclude(:MIN => nil).exclude(:MIN => "0:00").order(:DATE).entries
      boxscores_misc = database[ :"#{season.gsub(/-/,"_")}_#{type}_misc_sqlPlayersMisc" ].where(:PLAYER_NAME => entity[:PLAYER_NAME]).exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").exclude(:MIN => nil).exclude(:MIN => "0:00").order(:DATE).entries
      #boxscores_tracking = database[ :"#{season.gsub(/-/,"_")}_#{type}_playertrack_PlayerTrack" ].where(:PLAYER_NAME => entity[:PLAYER_NAME]).exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").exclude(:MIN => nil).exclude(:MIN => "0:00").order(:DATE).entries
      boxscores_tracking = database[ :"#{season.gsub(/-/,"_")}_#{type}_playertrack_PlayerStats" ].where(:PLAYER_NAME => entity[:PLAYER_NAME]).exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").exclude(:MIN => nil).exclude(:MIN => "0:00").order(:DATE).entries

      if boxscores.size != boxscores_advanced.size or boxscores.size != boxscores_advanced.size or boxscores.size != boxscores_fourfactors.size or boxscores.size != boxscores_scoring.size or boxscores.size != boxscores_usage.size or boxscores.size != boxscores_misc.size or boxscores.size != boxscores_tracking.size
        binding.pry
        p "mismatch boxscores"
      end
    end

    if 0 == boxscores.size
      next # this shouldn't happen but just in case
    end

    begin
      cur_date = Date.parse boxscores[0][:DATE]
    rescue StandardError => e
      binding.pry
      p "date error w/ boxscore"
    end

    splitSets = Array.new
    o_splitSets = Array.new

    for i in 0...boxscores.size
      splitSets[i] = SplitSet.new
      o_splitSets[i] = SplitSet.new
    end

    splitSet = SplitSet.new
    o_splitSet = SplitSet.new

    rows = Array.new( boxscores.size )
    num_boxscores = boxscores.size
    boxscores.each_with_index{|boxscore_traditional,i|
      prevSplitSet = SplitSet.new;o_prevSplitSet = SplitSet.new;prev_2_splitSet = SplitSet.new;o_prev_2_splitSet = SplitSet.new;prev_5_splitSet = SplitSet.new;o_prev_5_splitSet = SplitSet.new

      splitSet.away_split.valid = 0;splitSet.home_split.valid = 0;splitSet.starter_split.valid = 0;splitSet.bench_split.valid = 0;splitSet.total_games_with_rest_split[0].valid = 0;splitSet.total_games_with_rest_split[1].valid = 0;splitSet.total_games_with_rest_split[2].valid = 0;splitSet.total_games_with_rest_split[3].valid = 0;splitSet.total_games_with_rest_split[4].valid = 0;splitSet.total_games_with_rest_split[5].valid = 0;splitSet.total_games_with_rest_split[6].valid = 0;splitSet.three_in_four_split.valid = 0;splitSet.four_in_six_split.valid = 0

      o_splitSet.away_split.valid = 0;o_splitSet.home_split.valid = 0;o_splitSet.starter_split.valid = 0;o_splitSet.bench_split.valid = 0;o_splitSet.total_games_with_rest_split[0].valid = 0;o_splitSet.total_games_with_rest_split[1].valid = 0;o_splitSet.total_games_with_rest_split[2].valid = 0;o_splitSet.total_games_with_rest_split[3].valid = 0;o_splitSet.total_games_with_rest_split[4].valid = 0;o_splitSet.total_games_with_rest_split[5].valid = 0;o_splitSet.total_games_with_rest_split[6].valid = 0;o_splitSet.three_in_four_split.valid = 0;o_splitSet.four_in_six_split.valid = 0

      game_id = boxscore_traditional[:GAME_ID]
      if nil == game_id
        game_id = boxscore_traditional[:Game_ID]
        if nil == game_id
          binding.pry
          p "err"
        end
      end

      row = Hash.new

      if true == bTeam
        team_gamelog = database[ :"#{season.gsub(/-/,"_")}_#{type}_gamelogs" ].where(:TEAM_ID => boxscore_traditional[:TEAM_ID]).where(:GAME_ID => game_id).entries[0]
        if nil == team_gamelog
          binding.pry
          p "skipping, game_id = #{boxscore_traditional[:GAME_ID]}"
          next
        end
        boxscore_advanced = boxscores_advanced[i]
        boxscore_fourfactors = boxscores_fourfactors[i]
        boxscore_scoring = boxscores_scoring[i]
        boxscore_usage = boxscores_usage[i]
        boxscore_misc = boxscores_misc[i]
        boxscore_tracking = boxscores_tracking[i]
        #boxscores_traditional_starters = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStarterBenchStats" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).where(:GAME_ID => game_id).entries[0]
        #boxscore_traditional_bench = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStarterBenchStats" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).where(:GAME_ID => game_id).entries[1]

        lines = betting_lines.select{|line|
          line[:nbaGameID] == game_id
        }
        calculateAverageOverUnder( database, season, type, lines, entity[:TEAM_ABBREVIATION] )
      else
        team_gamelog = database[ :"#{season.gsub(/-/,"_")}_#{type}_gamelogs" ].where(:TEAM_ID => boxscore_traditional[:TEAM_ID]).where(:GAME_ID => game_id).entries[0]
        boxscore_advanced = boxscores_advanced[i]
        boxscore_fourfactors = boxscores_fourfactors[i]
        boxscore_scoring = boxscores_scoring[i]
        boxscore_usage = boxscores_usage[i]
        boxscore_misc = boxscores_misc[i]
        boxscore_tracking = boxscores_tracking[i]

        row[:player_name] = entity[:PLAYER_NAME]
      end

      row[:team_abbreviation] = boxscore_traditional[:TEAM_ABBREVIATION]
      row[:date] = Date.parse( boxscore_traditional[:DATE] )
      row[:date_of_data] = Date.parse( boxscore_traditional[:DATE] )
      row[:game_id] = game_id

      #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Figure out all the spltis that should applys: home, away, back to back, etc.
      #make an array of splits, and then in the functions below, calculate_() add the current boxscores to all the splits that apply
      #jlk - we need to calculate splits for: home/away games, back-to-backs, lots of rest, 3 in 4 nights, 5 in 7, etc., recent performance (last 3g, 5g, 10g)
      opponent_abbr = nil
      year = season.split("-")[0].to_i + 1 

      if 2 == team_gamelog[:MATCHUP].split("@").size and convertBBRTeamAbbr2( team_gamelog[:MATCHUP].split("@")[0].gsub(" ",""), year ).match( boxscore_traditional[:TEAM_ABBREVIATION] )
        splitSet.away_split.valid = 1
        o_splitSet.away_split.valid = 1
        opponent_abbr = convertBBRTeamAbbr2( team_gamelog[:MATCHUP].split("@")[1].gsub(/\s/, ""), year )
      elsif 2 == team_gamelog[:MATCHUP].split("vs.").size and convertBBRTeamAbbr2( team_gamelog[:MATCHUP].split("vs.")[0].gsub(" ",""), year ).match( boxscore_traditional[:TEAM_ABBREVIATION] )
        splitSet.home_split.valid = 1
        o_splitSet.home_split.valid = 1
        opponent_abbr = convertBBRTeamAbbr2( team_gamelog[:MATCHUP].split("vs.")[1].gsub(/\s/, ""), year )
      else
        binding.pry
        p "more than 2 arguments"
      end

      if false == bTeam
        if boxscore_traditional[:START_POSITION].match /F|C|G/
          splitSet.starter_split.valid = 1
        else
          splitSet.bench_split.valid = 1
        end
      end

      if 0 == i
        days_between_games = 6
      else
        previous_game_date = Date.parse( boxscores[i-1][:DATE] )
        days_between_games = (row[:date] - previous_game_date).to_i - 1

        if days_between_games > 6
          days_between_games = 6
        end
      end

      begin
        splitSet.total_games_with_rest_split[ days_between_games ].valid = 1
        o_splitSet.total_games_with_rest_split[ days_between_games ].valid = 1
      rescue StandardError => e
        binding.pry
        p "hi"
      end

      if i > 1
        prev_1_game_date = Date.parse( boxscores[i-1][:DATE] )
        prev_2_game_date = Date.parse( boxscores[i-2][:DATE] )
        #if not a back-to-back and 3 games in 4 nights
        if ( row[:date] - prev_1_game_date > 1 ) and ( row[:date] - prev_2_game_date < 4 )
          splitSet.three_in_four_split.valid = 1
          o_splitSet.three_in_four_split.valid = 1
        end
      end

      if i > 2
        prev_1_game_date = Date.parse( boxscores[i-1][:DATE] )
        prev_2_game_date = Date.parse( boxscores[i-2][:DATE] )
        prev_3_game_date = Date.parse( boxscores[i-3][:DATE] )
        #if not a back-to-back and 3 games in 4 nights
        if ( row[:date] - prev_1_game_date > 1 ) and ( row[:date] - prev_3_game_date < 6 )
          splitSet.four_in_six_split.valid = 1
          o_splitSet.four_in_six_split.valid = 1
        end
      end

      ##OPPONENT STATS if this is a team boxscore
      opponent = ""

      boxscore_traditional_team = nil
      o_boxscore_traditional_team = nil

      calculateBoxscoreTime( team_gamelog, boxscore_traditional, splitSet, bTeam )

      calculateTraditionalStats( boxscore_traditional, splitSet )

      if boxscore_advanced
        calculateAdvancedStats( boxscore_advanced, splitSet )
      end

      if boxscore_scoring
        calculateScoringStats( boxscore_scoring, boxscore_traditional, splitSet )
      end

      if boxscore_fourfactors
        calculateFourFactorStats( boxscore_fourfactors, splitSet )
      end

      if boxscore_usage
        calculateUsageStats( boxscore_usage, splitSet )
      end

      if boxscore_misc
        calculateMiscStats( boxscore_misc, splitSet, bTeam )
      end

      if boxscore_scoring
        calculateScoringDerivedStats( boxscore_scoring, splitSet )
      end

      if boxscore_tracking
        calculateTrackingStats( boxscore_tracking, splitSet )
      end

      calculateDerivedTraditionalStats( boxscore_traditional, splitSet )

      if true == bTeam
        if boxscore_traditional 
          o_boxscore_traditional = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStats" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[0]
        end
        o_gamelog = database[ :"#{season.gsub(/-/,"_")}_#{type}_gamelogs" ].where(:TEAM_ID => o_boxscore_traditional[:TEAM_ID]).where(:GAME_ID => game_id).entries[0]
        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_advanced_TeamStats"
          o_boxscore_advanced = database[ :"#{season.gsub(/-/,"_")}_#{type}_advanced_TeamStats" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[0]
        end
        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_fourfactors_sqlTeamsFourFactors"
          o_boxscore_fourfactors = database[ :"#{season.gsub(/-/,"_")}_#{type}_fourfactors_sqlTeamsFourFactors" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[0]
        end
        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_scoring_sqlTeamsScoring"
          o_boxscore_scoring = database[ :"#{season.gsub(/-/,"_")}_#{type}_scoring_sqlTeamsScoring" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[0]
        end
        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_usage_sqlTeamsUsage"
          o_boxscore_usage = database[ :"#{season.gsub(/-/,"_")}_#{type}_usage_sqlTeamsUsage" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[0]
        end
        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_misc_sqlTeamsMisc"
          o_boxscore_misc = database[ :"#{season.gsub(/-/,"_")}_#{type}_misc_sqlTeamsMisc" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[0]
        end
        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_playertrack_PlayerTrackTeam"
          #o_boxscore_tracking = database[ :"#{season.gsub(/-/,"_")}_#{type}_playertrack_PlayerTrackTeam" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[0]
        end
        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_playertrack_TeamStats"
          o_boxscore_tracking = database[ :"#{season.gsub(/-/,"_")}_#{type}_playertrack_TeamStats" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[0]
        end
=begin
        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStarterBenchStats"
          o_boxscore_traditional_starters = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStarterBenchStats" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[0]
        end
        if database.table_exists? :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStarterBenchStats"
          o_boxscore_traditional_bench = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStarterBenchStats" ].where(:TEAM_ABBREVIATION => opponent_abbr).where(:GAME_ID => game_id).entries[1]
        end
=end

        calculateBoxscoreTime( o_gamelog, o_boxscore_traditional, o_splitSet, bTeam )

        calculateTraditionalStats( o_gamelog, o_splitSet )

        if boxscore_advanced
          calculateAdvancedStats( o_boxscore_advanced, o_splitSet )
        end

        if boxscore_scoring
          calculateScoringStats( o_boxscore_scoring, o_gamelog, o_splitSet )
        end

        if boxscore_fourfactors
          calculateFourFactorStats( o_boxscore_fourfactors, o_splitSet )
        end

        if boxscore_usage
          calculateUsageStats( o_boxscore_usage, o_splitSet )
        end

        if boxscore_misc
          calculateMiscStats( o_boxscore_misc, o_splitSet, bTeam )
        end

        if boxscore_scoring
          calculateScoringDerivedStats( o_boxscore_scoring, o_splitSet )
        end

        if boxscore_tracking
          calculateTrackingStats( o_boxscore_tracking, o_splitSet )
        end

        calculateDerivedTraditionalStats( o_gamelog, o_splitSet )
      end

      calculateDerivedStats( splitSet, o_splitSet, database, tablename, boxscore_traditional[:TEAM_ABBREVIATION], game_id, bTeam )
      
      calculatePerMinStats( splitSet, o_splitSet, bTeam ) 

      splitSets[ i ] = splitSet
      o_splitSets[ i ] = o_splitSet
      #sort hash by gameid and then recalculate median and means
      #figure out what to do /w PCT stats

      if i > 0
        #recent games, 1, 2, 5, 10
        prevSplitSet = splitSets[ i - 1 ]
        o_prevSplitSet = o_splitSets[ i - 1 ]

        #perform this addition w/ game totals instead of totals
        prev_2_splitSet.calcAvg( splitSet, 2 )
        err = false
        prev_2_splitSet.split.timeStats.seconds_played.hash.each{|k,v|
          if nil == k
            err = true
            break
          end
        }
        if true == err
          binding.pry
          p "err"
        end

        #can't add totals, bc those are cumulative.  have to add game_totals instead
        calculateDerivedTraditionalStats( boxscore_traditional, prev_2_splitSet )

        if true == bTeam
          o_prev_2_splitSet.calcAvg( o_splitSet, 2 )
          calculateDerivedTraditionalStats( o_gamelog, o_prev_2_splitSet )
        end
        
        calculateDerivedStats( prev_2_splitSet, o_prev_2_splitSet, database, tablename, boxscore_traditional[:TEAM_ABBREVIATION], game_id, bTeam )
        calculatePerMinStats( prev_2_splitSet, o_prev_2_splitSet, bTeam ) 
      else
        prev_2_splitSet = nil
        o_prev_2_splitSet = nil
      end

      if i > 3
        prev_5_splitSet.calcAvg( splitSet, 5 )
        err = false
        prev_5_splitSet.split.timeStats.seconds_played.hash.each{|k,v|
          if nil == k
            err = true
            break
          end
        }
        if true == err
          binding.pry
          p "err"
        end

        #can't add totals, bc those are cumulative.  have to add game_totals instead
        calculateDerivedTraditionalStats( boxscore_traditional, prev_5_splitSet )

        if true == bTeam
          o_prev_5_splitSet.calcAvg( o_splitSet, 5 )
          calculateDerivedTraditionalStats( o_gamelog, o_prev_5_splitSet )
        end
        
        calculateDerivedStats( prev_5_splitSet, o_prev_5_splitSet, database, tablename, boxscore_traditional[:TEAM_ABBREVIATION], game_id, bTeam )
        calculatePerMinStats( prev_5_splitSet, o_prev_5_splitSet, bTeam ) 
      else
        prev_5_splitSet = nil
        o_prev_5_splitSet = nil
      end

      #calculatePerMinStats

      row = row.merge( splitSets[ i ].split.to_h ).merge( o_splitSets[ i ].split.to_opponent_h )

      cur_date = row[:date]

      begin
        row[:opponent_against_abbr] = opponent_abbr
        database[tablename].insert(row.to_hash)
      rescue StandardError => e
        binding.pry
        p "hi"
      end

      end_copy_date = nil
      if (boxscores.size-1) == i
        end_copy_date = season_end
      else
        end_copy_date = Date.parse( boxscores[i+1][:DATE] )
      end

      if false == bTeam
        calculateBoxscoreTime( team_gamelog, boxscore_traditional, opponents[ opponent_abbr ], bTeam )
        calculateTraditionalStats( boxscore_traditional, opponents[ opponent_abbr ] )
        if boxscore_advanced
          calculateAdvancedStats( boxscore_advanced, opponents[ opponent_abbr ] )
        end
        if boxscore_scoring
          calculateScoringStats( boxscore_scoring, boxscore_traditional, opponents[ opponent_abbr ] )
        end
        if boxscore_fourfactors
          calculateFourFactorStats( boxscore_fourfactors, opponents[ opponent_abbr ] )
        end
        if boxscore_usage
          calculateUsageStats( boxscore_usage, opponents[ opponent_abbr ] )
        end
        if boxscore_misc
          calculateMiscStats( boxscore_misc, opponents[ opponent_abbr ], bTeam )
        end
        if boxscore_scoring
          calculateScoringDerivedStats( boxscore_scoring, opponents[ opponent_abbr ] )
        end
        if boxscore_tracking
          calculateTrackingStats( boxscore_tracking, opponents[ opponent_abbr ] )
        end

        calculateDerivedTraditionalStats( boxscore_traditional, opponents[ opponent_abbr ] )
        calculateDerivedStats( opponents[ opponent_abbr ], o_splitSet, database, tablename, boxscore_traditional[:TEAM_ABBREVIATION], game_id, bTeam )
        calculatePerMinStats( opponents[ opponent_abbr ], o_splitSet, bTeam ) 

        #jlk - we can actually add more splits here
        row_opp = opponents[ opponent_abbr ].split.to_h
        row_opp[:average_type] = "player vs #{opponent_abbr}"

        fillRowData( row_opp, row )
        begin
          database[tablename].insert(row_opp)
        rescue StandardError => e
          binding.pry
        end

        player_vs_cur_date = cur_date + 1
        while player_vs_cur_date < end_copy_date
          row_opp[:date] = player_vs_cur_date
          database[tablename].insert(row_opp.to_hash)

          player_vs_cur_date = player_vs_cur_date + 1
        end
      end
      
      #print splits
      row_home = splitSets[ i ].home_split.to_h.merge( o_splitSets[i].home_split.to_opponent_h )
      row_home[:average_type] = "home"
      fillRowData( row_home, row )
      begin
        #player, date, date_of_data, team_abbreviation, opponent_against_abbr
        database[tablename].insert(row_home)
      rescue StandardError => e
        binding.pry
      end

      row_away = splitSets[ i ].away_split.to_h.merge( o_splitSets[i].away_split.to_opponent_h )
      row_away[:average_type] = "away"
      fillRowData( row_away, row )
      begin
        database[tablename].insert(row_away)
      rescue StandardError => e
        binding.pry
      end

      if prev_2_splitSet
        row_prev2 = prev_2_splitSet.to_h
        fillRowData( row_prev2, row )

        if (true == bTeam) and o_prev_2_splitSet
          row_prev2 = row_prev2.merge( o_prev_2_splitSet.to_opponent_h )
        end

        row_prev2[:average_type] = "prev2"
        begin
          database[tablename].insert(row_prev2)
        rescue StandardError => e
          binding.pry
        end
      end

      if prev_5_splitSet
        row_prev5 = prev_5_splitSet.to_h
        fillRowData( row_prev5, row )

        if (true == bTeam) and o_prev_5_splitSet
          row_prev5 = row_prev5.merge( o_prev_5_splitSet.to_opponent_h )
        end

        row_prev5[:average_type] = "prev5"
        begin
          database[tablename].insert(row_prev5)
        rescue StandardError => e
          binding.pry
        end
      end

      if false == bTeam
        row_starter = splitSets[ i ].starter_split.to_h.merge( o_splitSets[i].starter_split.to_opponent_h )
        row_starter[:average_type] = "starter"
        fillRowData( row_starter, row )
        begin
          database[tablename].insert(row_starter)
        rescue StandardError => e
          binding.pry
        end

        row_bench = splitSets[ i ].bench_split.to_h.merge( o_splitSets[i].bench_split.to_opponent_h )
        row_bench[:average_type] = "bench"
        fillRowData( row_bench, row )
        begin
          database[tablename].insert(row_bench)
        rescue StandardError => e
          binding.pry
        end
      end

      row_zero = splitSets[ i ].total_games_with_rest_split[0].to_h.merge( o_splitSets[i].total_games_with_rest_split[0].to_opponent_h )
      row_zero[:average_type] = "0 rest"
      fillRowData( row_zero, row )
      begin
        database[tablename].insert(row_zero)
      rescue StandardError => e
        binding.pry
      end

      row_one = splitSets[ i ].total_games_with_rest_split[1].to_h.merge( o_splitSets[i].total_games_with_rest_split[1].to_opponent_h )
      row_one[:average_type] = "1 rest"
      fillRowData( row_one, row )
      begin
        database[tablename].insert(row_one)
      rescue StandardError => e
        binding.pry
      end

      row_two = splitSets[ i ].total_games_with_rest_split[2].to_h.merge( o_splitSets[i].total_games_with_rest_split[2].to_opponent_h )
      row_two[:average_type] = "2 rest"
      fillRowData( row_two, row )
      begin
        database[tablename].insert(row_two)
      rescue StandardError => e
        binding.pry
      end

      row_three = splitSets[ i ].total_games_with_rest_split[3].to_h.merge( o_splitSets[i].total_games_with_rest_split[3].to_opponent_h )
      row_three[:average_type] = "3 rest"
      fillRowData( row_three, row )
      begin
        database[tablename].insert(row_three)
      rescue StandardError => e
        binding.pry
      end

      row_four = splitSets[ i ].total_games_with_rest_split[4].to_h.merge( o_splitSets[i].total_games_with_rest_split[4].to_opponent_h )
      row_four[:average_type] = "4 rest"
      fillRowData( row_four, row )
      begin
        database[tablename].insert(row_four)
      rescue StandardError => e
        binding.pry
      end

      row_five = splitSets[ i ].total_games_with_rest_split[5].to_h.merge( o_splitSets[i].total_games_with_rest_split[5].to_opponent_h )
      row_five[:average_type] = "5 rest"
      fillRowData( row_five, row )
      begin
        database[tablename].insert(row_five)
      rescue StandardError => e
        binding.pry
      end

      row_six = splitSets[ i ].total_games_with_rest_split[6].to_h.merge( o_splitSets[i].total_games_with_rest_split[6].to_opponent_h )
      row_six[:average_type] = "6 rest"
      fillRowData( row_six, row )
      begin
        database[tablename].insert(row_six)
      rescue StandardError => e
        binding.pry
      end

      row_three_in_four = splitSets[ i ].three_in_four_split.to_h.merge( o_splitSets[i].three_in_four_split.to_opponent_h )
      row_three_in_four[:average_type] = "34"
      fillRowData( row_three_in_four, row )
      begin
        database[tablename].insert(row_three_in_four)
      rescue StandardError => e
        binding.pry
      end

      row_four_in_six = splitSets[ i ].four_in_six_split.to_h.merge( o_splitSets[i].four_in_six_split.to_opponent_h )
      row_four_in_six[:average_type] = "46"
      fillRowData( row_four_in_six, row )
      begin
        database[tablename].insert(row_four_in_six)
      rescue StandardError => e
        binding.pry
      end

      #For dates without games, repeat previous game's entries for easier post-processing
      #cur_date + 1 b/c we are going to update cur_date in this iteration
      cur_date = cur_date + 1
      while cur_date < end_copy_date
        row[:date] = cur_date
        database[tablename].insert(row.to_hash)

        if row_home 
          row_home[:date] = cur_date
          database[tablename].insert(row_home.to_hash)
        end
=begin
        if row_opp 
          row_opp[:date] = cur_date
          database[tablename].insert(row_opp.to_hash)
        end
=end
        if row_away 
          row_away[:date] = cur_date
          database[tablename].insert(row_away.to_hash)
        end
        if row_starter 
          row_starter[:date] = cur_date
          database[tablename].insert(row_starter.to_hash)
        end
        if row_bench 
          row_bench[:date] = cur_date
          database[tablename].insert(row_bench.to_hash)
        end
        if row_prev2 
          row_prev2[:date] = cur_date
          database[tablename].insert(row_prev2.to_hash)
        end
        if row_prev5 
          row_prev5[:date] = cur_date
          database[tablename].insert(row_prev5.to_hash)
        end
        if row_three_in_four 
          row_three_in_four[:date] = cur_date
          database[tablename].insert(row_three_in_four.to_hash)
        end
        if row_four_in_six 
          row_four_in_six[:date] = cur_date
          database[tablename].insert(row_four_in_six.to_hash)
        end
        if row_zero 
          row_zero[:date] = cur_date
          database[tablename].insert(row_zero.to_hash)
        end
        if row_one 
          row_one[:date] = cur_date
          database[tablename].insert(row_one.to_hash)
        end
        if row_two 
          row_two[:date] = cur_date
          database[tablename].insert(row_two.to_hash)
        end
        if row_three 
          row_three[:date] = cur_date
          database[tablename].insert(row_three.to_hash)
        end
        if row_four 
          row_four[:date] = cur_date
          database[tablename].insert(row_four.to_hash)
        end
        if row_five 
          row_five[:date] = cur_date
          database[tablename].insert(row_five.to_hash)
        end
        if row_six 
          row_six[:date] = cur_date
          database[tablename].insert(row_six.to_hash)
        end
        cur_date = cur_date + 1
      end
=begin
      if ((num_boxscores - 1) == i) #and (bTeam == false)
        testData( row, row_home, row_away, row_starter, row_bench, row_zero, row_one, row_two, row_three, row_six, bTeam )
        binding.pry
        #p "#{total_home_games} home and #{total_away_games} away games"
        #p "hi"
      end
=end
      #p "#{i + 1} / #{boxscores.size} boxscores done"
      #loopend
    }
    p "entity #{entity} done w #{num_boxscores} games in #{season} season. #{entity_index} / #{entities.size} done"
  }
end

if 1
def calculateXYvalues( database, seasons_h, season, type )
  players = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:PLAYER_ID, :PLAYER_NAME).entries

  if NUM_THREADS and THREAD_INDEX
    playerSizes = Hash.new
    players.each_with_index{|player,i|
      entries = database[:"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats"].select_all.where(:PLAYER_ID => player[:PLAYER_ID]).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").entries;
      playerSizes[player] = entries.size;
    };
    playerSizes = playerSizes.sort_by{|k,v| v}.reverse
    players = createChunk( playerSizes, THREAD_INDEX, NUM_THREADS)
  end

  players.each_with_index{|player,i|
    puts "#{player[:PLAYER_NAME]} (#{i}/#{players.size}):"
    begin
      if "regularseason" == type 
        season_index = 0
      else
        season_index = 1
      end
      calculateAveragePoints( database, seasons_h, season, type, player, seasons_h[season][ season_index ] )
    rescue StandardError => e
      binding.pry
      p "hi"
    end
  }
end

def initWorkHash
  workHash = Hash.new

  File.open("clientIPs.txt", "r") do |f|
    f.each_line.with_index do |line,i|
      workHash[ i ] = line.chomp
    end
  end

  if (NUM_THREADS) != workHash.size
    binding.pry
    p "malformed clientIPs.txt file"
  end
  return workHash
end

def getServerIP
  serverIP = nil
  File.open("clientIPs.txt", "r") do |f|
    f.each_line.with_index do |line,i|
      serverIP = line.chomp
      break
    end
  end

  return serverIP
end

def syncServers( database, season, partNumber )
  season = season.gsub(/-/,"_")
  newDatabase = nil
  if nil == NUM_THREADS or nil == THREAD_INDEX
    return
  end
  if 0 == THREAD_INDEX

    workHash = initWorkHash()
    workHash = workHash.delete_if{|k,v| 0 == k}#Get rid of the server, just process clients

    while workHash.size > 0
      workHash.each{|k,v|
        rs_filename = "#{season}_#{k}_rs_part#{partNumber}.csv"
        #po_filename = "#{season}_#{k}_po_part#{partNumber}.csv"
        if File.exists? rs_filename #and File.exists? po_filename
          #p "#{rs_filename} and #{po_filename} exist, copying rows.." 
          p "#{rs_filename} exists, copying rows.." 

          distinct_players = database[ :"_#{season.gsub(/-/,"_")}_regularseason_daily_averages" ].select(:PLAYER_NAME).distinct.entries
          p "Master DB has #{distinct_players.size} distinct RS players"
          #distinct_players = database[ :"_#{season.gsub(/-/,"_")}_playoffs_daily_averages" ].select(:PLAYER_NAME).distinct.entries
          #p "Master DB has #{distinct_players.size} distinct PO players"

          `rm -f do.sql`
            File.open( "do.sql", "w" ){|f|
              f << ".separator ,\n"
              f << ".import #{rs_filename} _#{season}_regularseason_daily_averages\n"
              #f << ".import #{po_filename} _#{season}_playoffs_daily_averages\n"
            }
          `sqlite3 #{season}.db < do.sql`

          distinct_players = database[ :"_#{season.gsub(/-/,"_")}_regularseason_daily_averages" ].select(:PLAYER_NAME).distinct.entries
          p "Master DB now has #{distinct_players.size} distinct RS players"

          #distinct_players = database[ :"_#{season.gsub(/-/,"_")}_playoffs_daily_averages" ].select(:PLAYER_NAME).distinct.entries
          #p "Master DB now has #{distinct_players.size} distinct PO players"

          p "removing: thread #{k}: #{v} from workHash"
          workHash = workHash.delete_if{|key,val| k == key}
          `rm -f #{rs_filename}`
          #`rm -f #{po_filename}`
        else
          #p "#{rs_filename} doesn't exist"
        end
      }
    end
    p "all threads have finished part #{partNumber}.  Sending new db over to clients"

    rows_updated = database[:"_#{season.gsub(/-/,"_")}_regularseason_daily_averages"].where(:average_type => "").update(:average_type => nil)
    p "#{rows_updated} rows changed from '' to null average_type"                            
    #rows_updated = database[:"_#{season.gsub(/-/,"_")}_playoffs_daily_averages"].where(:average_type => "").update(:average_type => nil)
    #p "#{rows_updated} rows changed from '' to null average_type"                            

    if partNumber < 2
      workHash = initWorkHash()
      workHash = workHash.delete_if{|k,v| 0 == k}#Get rid of the server, just process clients

      workHash.each{|key,clientIP|
        #`scp -o 'StrictHostKeyChecking no' #{season}.db rails@#{clientIP}:~/#{season}_part#{partNumber}.db`
        `scp -o 'StrictHostKeyChecking no' clientIPs.txt rails@#{clientIP}:~/#{season}_part#{partNumber}.txt`
      }

      while workHash.size > 0
        workHash.each{|key,clientIP|
          if File.exists? "#{key}_part#{partNumber}.done"
            `rm -f #{key}_part#{partNumber}.done`
            p "removing: thread #{key}: #{clientIP} from workHash"
            workHash = workHash.delete_if{|k,v| k == key}
          end
        }
      end
    end
  else
    if 0 == partNumber
      `sqlite3 -csv #{season}.db 'select * from _#{season}_regularseason_daily_averages where player_name is not null;' > #{season}_#{THREAD_INDEX}_rs_part#{partNumber}.csv`
      #`sqlite3 -csv #{season}.db 'select * from _#{season}_playoffs_daily_averages where player_name is not null;' > #{season}_#{THREAD_INDEX}_po_part#{partNumber}.csv`
    elsif 1 == partNumber
      `sqlite3 -csv #{season}_part#{partNumber}.db 'select * from _#{season}_regularseason_daily_averages where average_type like "opp%";' > #{season}_#{THREAD_INDEX}_rs_part#{partNumber}.csv`
      #`sqlite3 -csv #{season}_part#{partNumber}.db 'select * from _#{season}_playoffs_daily_averages where average_type like "opp%";' > #{season}_#{THREAD_INDEX}_po_part#{partNumber}.csv`
    elsif 2 == partNumber
      `sqlite3 -csv #{season}_part#{partNumber}.db 'select * from _#{season}_regularseason_daily_averages where player_name is not null and average_type is null and date = date_of_data and actual_pts is not null and date_of_actual_stats != "" and date_of_actual_stats is not null;' > #{season}_#{THREAD_INDEX}_rs_part#{partNumber}.csv`
      #`sqlite3 -csv #{season}_part#{partNumber}.db 'select * from _#{season}_playoffs_daily_averages where player_name is not null and average_type is null and date = date_of_data and actual_pts is not null and date_of_actual_stats != "" and date_of_actual_stats is not null;' > #{season}_#{THREAD_INDEX}_po_part#{partNumber}.csv`
      #`sqlite3 -csv #{season}.db 'select * from _#{season}_regularseason_daily_averages where player_name is not null and average_type is null and date = date_of_data and actual_pts is not null;' > #{season}_#{THREAD_INDEX}_rs_part#{partNumber}.csv`
      #`sqlite3 -csv #{season}.db 'select * from _#{season}_playoffs_daily_averages where player_name is not null and average_type is null and date = date_of_data and actual_pts is not null;' > #{season}_#{THREAD_INDEX}_po_part#{partNumber}.csv`
    end

    serverIP = getServerIP()
    `scp -o 'StrictHostKeyChecking no' #{season}_#{THREAD_INDEX}_rs_part#{partNumber}.csv rails@#{serverIP}:~`
    #`scp -o 'StrictHostKeyChecking no' #{season}_#{THREAD_INDEX}_po_part#{partNumber}.csv rails@#{serverIP}:~`
    p "sent completed db back to server"
    while true
      if File.exists? "#{season}_part#{partNumber}.txt" #functions as ACK flag that file has finished transferring
      #if File.exists? "#{season}_part#{partNumber}.db" #functions as ACK flag that file has finished transferring
        #change DB global var
        `scp -o 'StrictHostKeyChecking no' rails@#{serverIP}:~/#{season}.db #{season}_part#{partNumber+1}.db`
        newDatabase = getDatabase("#{season}_part#{partNumber+1}.db")
        p "#{season}_part#{partNumber+1}.db arrived"
        if 0 == partNumber
          `rm -f "#{season}.db"`
        else
          `rm -f "#{season}_part#{partNumber}.db"`
        end

        `scp -o 'StrictHostKeyChecking no' #{season}_part#{partNumber}.txt rails@#{serverIP}:~/#{THREAD_INDEX}_part#{partNumber}.done`
        `rm -f "#{season}_part#{partNumber}.txt"`
        break
      end
    end
  end

  if newDatabase
    database = newDatabase
  end

  return database
end

def previousAverageN( arrSeconds, num_games )
  if arrSeconds.size < num_games
    return 0.0
  else
    sum = 0
    e = arrSeconds.size
    arrSeconds[(e-num_games)...e ].each{|seconds|
      sum = sum + seconds
    }
    return (sum.to_f/num_games.to_f)
  end
end

def exploreSecondsPlayed( database, season, type, season_start_date )
  database[:"#{season.gsub("-","_")}_#{type}_traditional_PlayerStats"]
  players = database[ :"#{season.gsub("-","_")}_#{type}_traditional_PlayerStats" ].distinct.select(:PLAYER_ID, :PLAYER_NAME).entries
    all_games = database[:"_#{season.gsub("-","_")}_#{type}_daily_averages"].where(:date => :date_of_data, :average_type => nil).select(:date_of_actual_stats,:player_name,:actual_PTS, :prev_mean_PTS, :b2b, :front_b2b, :mean_b2b_PTS, :mean_front_b2b_PTS, :mean_non_b2b_PTS,:location, :mean_home_PTS, :mean_away_PTS, :expected_PTS_pace, :expected_PTS_pace2, :expected_PTS_pace3, :expected_PTS_def_rtg, :league_average_def_rtg,:league_average_def_rtg_v_position, :league_average_PTS_v_position, :o_team_def_rtg, :o_team_def_rtg_v_position, :o_team_PTS_v_position,:extra_rest, :mean_extra_rest_PTS, :opp_b2b, :mean_opp_b2b_PTS, :mean_opp_non_b2b_PTS,:opp_front_b2b, :mean_opp_front_b2b_PTS, :opp_extra_rest, :mean_opp_extra_rest_PTS,:prev_pts_fb_mean,:opp_o_pts_fb_mean,:team_pts_fb_mean,:prev_pts_paint_mean,:opp_o_pts_paint_mean,:team_pts_paint_mean,:prev_pts_2nd_chance_mean,:opp_o_pts_2nd_chance_mean,:team_pts_2nd_chance_mean,:prev_pts_off_tov_mean,:opp_o_pts_off_tov_mean,:team_pts_off_tov_mean, :actual_SECONDS, :mean_seconds, :seconds_played_prev_game, :seconds_played_prev_prev_game, :mean_home_seconds, :mean_away_seconds, :mean_b2b_seconds, :mean_non_b2b_seconds, :mean_front_b2b_seconds, :mean_non_front_b2b_seconds, :mean_threeg4d_seconds, :mean_extra_rest_seconds, :mean_opp_b2b_seconds, :mean_opp_non_b2b_seconds, :mean_opp_front_b2b_seconds, :mean_opp_non_front_b2b_seconds, :mean_opp_threeg4d_seconds, :mean_opp_extra_rest_seconds, :mean_team_SECONDS, :team_game_number, :win_pct,:opp_win_pct, :win_pct_locale, :opp_win_pct_locale, :opponent_o_team_SECONDS_v_position, :league_average_SECONDS_v_position ).order(:date).order(:player_name).entries

  cutoff_date = Date.parse( "2012-01-15")
  allGames = Array.new
  players.each_with_index{|player,player_i|
    p "player #{player_i} / #{players.size}"

    games = all_games.select{|g| g[:player_name] == player[:PLAYER_NAME] and g[:date_of_actual_stats] != ""};
    games = games.uniq{|g| g[:date_of_actual_stats]};
    p "#{player[:PLAYER_NAME]} has #{games.size} games"

    arrSeconds = Array.new
    total_seconds = 0.0
    games.each_with_index{|data,ind|
      game_date = Date.parse( data[:date_of_actual_stats] )
      if game_date < cutoff_date
        next
      end

      if ind > 0 and 0.0 >= data[:league_average_def_rtg_v_position].to_f  
        binding.pry
        p "leagueaverage_def_rtg is: #{data[:league_average_def_rtg_v_position].to_f}"
      end

    if "" == data[:b2b] or nil == data[:b2b]
      data[:b2b] = "0"
    end
    if "" == data[:extra_rest] or nil == data[:extra_rest]
      data[:extra_rest] = "0"
    end
    if "" == data[:opp_b2b] or nil == data[:opp_b2b]
      data[:opp_b2b] = "0"
    end
    if "" == data[:opp_extra_rest] or nil == data[:opp_extra_rest]
      data[:opp_extra_rest] = "0"
    end
      mean_seconds = total_seconds / (ind+1).to_f
      data[:mean_seconds] = mean_seconds

      prev_seconds = previousAverageN( arrSeconds, 1 )
      prev2_seconds = previousAverageN( arrSeconds, 2 )
      prev5_seconds = previousAverageN( arrSeconds, 5 )

      arrSeconds.push data[:actual_SECONDS].to_f
      total_seconds = total_seconds + data[:actual_SECONDS].to_f
      ratio = 0.0
      if 0.0 != data[:mean_seconds].to_f 
        ratio = data[:actual_SECONDS].to_f / data[:mean_seconds].to_f
      end
      allGames.push [ player[:PLAYER_NAME], data[:date_of_actual_stats], data[:mean_seconds].to_f, data[:b2b], data[:extra_rest], prev_seconds, prev2_seconds, prev5_seconds, data[:actual_SECONDS].to_f, ratio ]
    }
  }
  binding.pry
  p "hi"

end

#jlk - to transform this function, calculate location, b2b,front_b2b,extra_rest,oppb2b,oppfrontb2b,oppextra_rest, get rid of -1 average day, and keep everything else
def outputPointsCsv( database, season, type, season_start_date )

  database[:"#{season.gsub("-","_")}_#{type}_traditional_PlayerStats"]
  players = database[ :"#{season.gsub("-","_")}_#{type}_traditional_PlayerStats" ].distinct.select(:PLAYER_ID, :PLAYER_NAME).entries

  p "getting all games"
  all_games = database[:"_#{season.gsub("-","_")}_#{type}_daily_averages"].where(:date => :date_of_data, :average_type => nil).select(:date_of_actual_stats,:player_name,:actual_PTS, :prev_mean_PTS, :b2b, :front_b2b, :mean_b2b_PTS, :mean_front_b2b_PTS, :mean_non_b2b_PTS,:location, :mean_home_PTS, :mean_away_PTS, :expected_PTS_pace, :expected_PTS_pace2, :expected_PTS_pace3, :expected_PTS_def_rtg, :league_average_def_rtg,:league_average_def_rtg_v_position, :league_average_PTS_v_position, :o_team_def_rtg, :o_team_def_rtg_v_position, :o_team_PTS_v_position,:extra_rest, :mean_extra_rest_PTS, :opp_b2b, :mean_opp_b2b_PTS, :mean_opp_non_b2b_PTS,:opp_front_b2b, :mean_opp_front_b2b_PTS, :opp_extra_rest, :mean_opp_extra_rest_PTS,:prev_pts_fb_mean,:opp_o_pts_fb_mean,:team_pts_fb_mean,:prev_pts_paint_mean,:opp_o_pts_paint_mean,:team_pts_paint_mean,:prev_pts_2nd_chance_mean,:opp_o_pts_2nd_chance_mean,:team_pts_2nd_chance_mean,:prev_pts_off_tov_mean,:opp_o_pts_off_tov_mean,:team_pts_off_tov_mean, :actual_SECONDS, :mean_seconds, :seconds_played_prev_game, :seconds_played_prev_prev_game, :mean_home_seconds, :mean_away_seconds, :mean_b2b_seconds, :mean_non_b2b_seconds, :mean_front_b2b_seconds, :mean_non_front_b2b_seconds, :mean_threeg4d_seconds, :mean_extra_rest_seconds, :mean_opp_b2b_seconds, :mean_opp_non_b2b_seconds, :mean_opp_front_b2b_seconds, :mean_opp_non_front_b2b_seconds, :mean_opp_threeg4d_seconds, :mean_opp_extra_rest_seconds, :mean_team_SECONDS, :team_game_number, :win_pct,:opp_win_pct, :win_pct_locale, :opp_win_pct_locale, :opponent_o_team_SECONDS_v_position, :league_average_SECONDS_v_position, :starter, :prev_mean_FTM,:prev_mean_FTA,:prev_mean_PFD,:prev_PFD_mean,:prev_team_PFD_mean,:prev_o_team_PF_mean,:prev_o_team_PF_mean_v_position,:league_average_PF,:league_average_PFD_v_position,:prev_team_FTA_mean,:prev_o_team_FTA,:prev_o_team_FTA_v_position,:league_average_FTA,:league_average_FTA_v_position, :actual_OREB, :mean_OREB, :team_OREB,:opponent_o_team_OREB, :league_average_DREB, :league_average_OREB_PCT, :e_OREB_PCT, :e_o_DREB, :e_o_DREB_PCT, :mean_b2b_OREB,:mean_b2b_OREB_PCT,:mean_extra_rest_OREB,:mean_extra_rest_OREB_PCT,:mean_opp_b2b_OREB,:mean_opp_b2b_OREB_PCT,:o_OREB_v_position,:league_average_OREB_v_position,:o_OREB_PCT_v_position,:league_average_OREB_PCT_v_position,:actual_DREB, :mean_DREB, :team_DREB,:opponent_o_team_DREB, :league_average_OREB, :league_average_DREB_PCT, :e_DREB_PCT, :e_o_OREB, :e_o_OREB_PCT, :mean_b2b_DREB,:mean_b2b_DREB_PCT,:mean_extra_rest_DREB,:mean_extra_rest_DREB_PCT,:mean_opp_b2b_DREB,:mean_opp_b2b_DREB_PCT,:o_DREB_v_position,:league_average_DREB_v_position,:o_DREB_PCT_v_position,:league_average_DREB_PCT_v_position,:actual_STL,:mean_STL,:mean_home_STL,:mean_away_STL,:mean_b2b_STL,:mean_non_b2b_STL,:mean_extra_rest_STL,:mean_opp_b2b_STL,:mean_opp_extra_rest_STL,:mean_team_STL,:opponent_o_team_STL,:league_average_STL,:opponent_o_team_STL,:league_average_STL,:opponent_o_team_STL_v_position,:team_PCT_STL,:o_mean_PCT_STL,:league_average_PCT_STL,:mean_PCT_STL,:o_team_STL_v_position,:o_team_PCT_STL_v_position,:league_average_PCT_STL_v_position,:actual_AST,:mean_AST,:mean_home_AST,:mean_away_AST,:mean_b2b_AST,:mean_non_b2b_AST,:mean_front_b2b_AST,:mean_extra_rest_AST,:mean_opp_b2b_AST,:mean_opp_extra_rest_AST,:mean_team_AST,:opponent_o_team_AST,:league_average_AST,:opponent_o_team_AST_v_position,:league_average_AST_v_position,:team_AST_PCT,:o_mean_AST_PCT,:league_average_AST_PCT,:mean_AST_PCT,:o_team_AST_v_position,:o_team_AST_PCT_v_position,:league_average_AST_PCT_v_position,:league_average_AST_RATIO,:mean_AST_RATIO,:o_team_AST_RATIO_v_position,:league_average_AST_RATIO_V_position,:actual_BLK, :mean_BLK, :mean_home_BLK, :mean_away_BLK, :mean_b2b_BLK, :mean_front_b2b_BLK, :mean_extra_rest_BLK, :mean_opp_b2b_BLK, :mean_opp_front_b2b_BLK, :mean_opp_extra_rest_BLK, :mean_team_BLK, :opponent_o_team_BLK, :league_average_BLK, :opponent_o_team_BLK_v_position, :league_average_BLK_v_position, :team_PCT_BLK, :o_mean_PCT_BLK, :league_average_PCT_BLK, :mean_PCT_BLK, :o_team_BLK_v_position, :o_team_PCT_BLK_v_position, :league_average_PCT_BLK_v_position,:actual_TOV, :mean_TOV, :mean_home_TOV, :mean_away_TOV, :mean_b2b_TOV, :mean_front_b2b_TOV, :mean_extra_rest_TOV, :mean_opp_b2b_TOV, :mean_opp_front_b2b_TOV, :mean_opp_extra_rest_TOV, :mean_team_TOV, :opponent_o_team_TOV, :league_average_TO, :opponent_o_team_TOV_v_position, :league_average_TO_v_position, :team_TO_PCT, :o_mean_TO_PCT, :league_average_TO_PCT, :mean_TO_PCT, :o_team_TO_v_position, :o_team_TO_PCT_v_position, :league_average_TO_PCT_v_position,:prev_mean_FG3M,:prev_mean_FG2M,:prev_PCT_CFGM,:prev_PCT_UFGM,:opp_o_PCT_CFGA,:prev_PCT_CFGA,:opp_o_PCT_CFGA_v_position,:prev_CFGA_mean,:opp_o_CFG_PCT,:opp_o_CFG_PCT_v_position,:prev_CFG_PCT,:opp_o_PCT_UFGA,:prev_PCT_UFGA,:opp_o_PCT_UFGA_v_position,:prev_UFGA_mean,:opp_o_UFG_PCT,:opp_o_UFG_PCT_v_position,:prev_UFG_PCT).order(:date).order(:player_name).entries
    
     
  p "got all games"


  #:opp_o_PCT_CFGA,:opp_o_PCT_CFGA_v_position,:opp_o_CFG_PCT,:opp_o_CFG_PCT_v_position,:opp_o_PCT_UFGA,:opp_o_PCT_UFGA_v_position,:opp_o_UFG_PCT,:opp_o_UFG_PCT_v_position,
  _org_averages = database[:"_#{season.gsub("-","_")}_#{type}_daily_averages"].exclude(:player_name => nil).where(:average_type => nil).select(:player_name, :date, :date_of_data, :date_of_actual_stats, :OREB_PCT, :DREB_PCT, :PCT_STL, :STL_mean,:AST_mean,:AST_PCT,:BLK_mean,:PCT_BLK,:TOV_mean,:TO_PCT,:DRBC_mean,:ORBC_mean,:CFGA_mean,:CFG_PCT,:UFGA_mean,:UFG_PCT).order(:player_name).order(:date).entries
  p "got org_ averages games"

  _org_team_averages = database[:"_#{season.gsub("-","_")}_#{type}_daily_averages"].where(:player_name => nil, :average_type => nil).select(:team_abbreviation, :date, :date_of_data, :date_of_actual_stats, :OREB_mean,:DREB_mean,:OREB_PCT, :DREB_PCT, :PCT_STL, :STL_mean,:AST_mean,:AST_PCT,:BLK_mean,:PCT_BLK,:TOV_mean,:TO_PCT,:DRBC_mean,:ORBC_mean,:CFGA_mean,:CFG_PCT,:UFGA_mean,:UFG_PCT,:FGA_mean,:FGM_mean,:FTA_mean,:FTM_mean,:FG3A_mean,:FG3M_mean,:FG2A_mean,:FG2M_mean,:FTA_mean,:FTM_mean,:FT_PCT,:FG_PCT,:FG2_PCT,:FG3_PCT).order(:player_name).order(:date).entries

  p "got org_team averages games"
  _org_opp_pos_averages = database[:"_#{season.gsub("-","_")}_#{type}_daily_averages"].exclude(:opponent_against_abbr => nil).where(Sequel.like(:average_type, "opponent vs starter%")).select(:average_type,:opponent_against_abbr, :date, :date_of_data, :date_of_actual_stats, :OREB_mean,:DREB_mean,:OREB_PCT, :DREB_PCT, :PCT_STL, :STL_mean,:AST_mean,:AST_PCT,:BLK_mean,:PCT_BLK,:TOV_mean,:TO_PCT,:DRBC_mean,:ORBC_mean,:CFGA_mean,:CFG_PCT,:UFGA_mean,:UFG_PCT,:FGA_mean,:FGM_mean,:FTA_mean,:FTM_mean,:FG3A_mean,:FG3M_mean,:FG2A_mean,:FG2M_mean,:FTA_mean,:FTM_mean,:FT_PCT,:FG_PCT,:FG2_PCT,:FG3_PCT).order(:player_name).order(:date).entries


  p "got org_pos averages games"
  league_position_averages = Hash.new
  
  #league_average_fta, league_average_fta_v_pos
  #e_o_DREB, e_o_DREB_PCT, mean_b2b_oreb_pct
  #league_average_oreb_v_pos
  #league_average_oreb_pct_v_pos
  #e_o_DREB, e_o_DREB_PCT, mean_b2b_oreb_pct
  #league_average_oreb_v_pos
  #league_average_oreb_pct_v_pos
  #
  ["PG","SG","SF","PF","C"].each{|pos|
    league_position_averages[ pos ] = Hash.new
    entries = database[:"_#{season.gsub("-","_")}_#{type}_daily_team_averages"].where(:average_type => "opponent vs starter #{pos}").order(:date).entries
    entries.each{|h|
      league_position_averages[ pos ][ h[:date] ] = h#[:team_mean_SECONDS]
    }
  }
  p "got league_pos averages games"

  File.open("points_#{season.gsub("-","_")}.csv", "w") { |f| }
  File.open("orebs_#{season.gsub("-","_")}.csv", "w") { |f| }
  File.open("drebs_#{season.gsub("-","_")}.csv", "w") { |f| }
  File.open("steals_#{season.gsub("-","_")}.csv", "w") { |f| }
  File.open("assists_#{season.gsub("-","_")}.csv", "w") { |f| }
  File.open("blocks_#{season.gsub("-","_")}.csv", "w") { |f| }
  File.open("turnovers_#{season.gsub("-","_")}.csv", "w") { |f| }
  File.open("points_per_min_#{season.gsub("-","_")}.csv", "w") { |f| }
  File.open("seconds_#{season.gsub("-","_")}.csv", "w") { |f| }
  positions = Hash.new
  database[:"#{season.gsub("-","_")}_bioinfo"].select(:player,:pos,:PLAYER_ID).entries.each{|bio|
    positions[ bio[:PLAYER_ID] ] = bio[:pos]
  }
  #jlk todo front_b2b bGameTomorrow() not working
  
  cutoff_date = Date.parse( "2012-01-14" )
  players.each_with_index{|player,player_i|
    #if player[:PLAYER_NAME] == "Chris Johnson" or player[:PLAYER_NAME] == "Tony Mitchell" or player[:PLAYER_NAME] == "Quincy Pondexter" or player[:PLAYER_NAME] == "Dorell Wright"
    if player[:PLAYER_NAME] == "John Holland" or player[:PLAYER_NAME] == "Willy Hernangomez"
      next
      p "skipping chris johnson"
    end
    p "player #{player_i} / #{players.size}"

    games = all_games.select{|g| g[:player_name] == player[:PLAYER_NAME] and g[:date_of_actual_stats] != ""};
    games = games.uniq{|g| g[:date_of_actual_stats]};
    boxscores = database[ :"#{season.gsub("-","_")}_#{type}_traditional_PlayerStats" ].where(:PLAYER_ID => player[:PLAYER_ID]).entries
    boxscores = boxscores.select{|b| b[:MIN]}
    if games.size != boxscores.size
      binding.pry
      p "boxscores size #{boxscores.size} #{games.size}"
    end
    if 0 == games.size or 0 == boxscores.size
	    p "no games for : #{player}"
	    next
    end

    position = ""
    bio_entries = database[:"#{season.gsub(/-/,"_")}_bioinfo"].select_all.where(:player_id => player[:PLAYER_ID]).entries
    if bio_entries and bio_entries.size > 0 and bio_entries.first[:pos]
      position = bio_entries.first[:pos]
    elsif bio_entries and bio_entries.size > 0 and bio_entries.first[:Pos]
      position = bio_entries.first[:Pos]
    else
      binding.pry
      p "no bio for #{player[:PLAYER_ID]}"
      next
    end

    opponents_str = ""
    boxscores.each_with_index{|boxscore,i|
      if (boxscores.size - 1) == i 
        opponents_str = opponents_str + "(GAME_ID = '#{boxscores[i][:GAME_ID]}' and (not (team_abbreviation = '#{boxscores[i][:TEAM_ABBREVIATION]}')))"
      else
        opponents_str = opponents_str + "(GAME_ID = '#{boxscores[i][:GAME_ID]}' and (not (team_abbreviation = '#{boxscores[i][:TEAM_ABBREVIATION]}'))) or "
      end
    }
    begin
      opponents = database[:"#{season.gsub("-","_")}_#{type}_traditional_TeamStats"].where(Sequel.lit(opponents_str)).order(:DATE).entries
    rescue StandardError => e
	    binding.pry
	    p 'hi'
    end
    if opponents.size != games.size
      p "opponents size #{opponents.size} #{games.size}"
    end

    team_boxscores = database[:"#{season.gsub("-","_")}_#{type}_traditional_TeamStats"].where(:team_abbreviation => boxscores.first[:TEAM_ABBREVIATION]).order(:DATE).entries
    team_season_start_date = team_boxscores.first[:DATE]

    opp_boxscores = database[:"#{season.gsub("-","_")}_#{type}_traditional_TeamStats"].where(:team_abbreviation => opponents.first[:TEAM_ABBREVIATION]).order(:DATE).entries
    opp_season_start_date = opp_boxscores.first[:DATE]

    date_str = ""
    team_date_str = ""
    opp_date_str = ""
    opp_pos_date_str = ""
    player_date_hash = {}
    date_hash = {}
    opp_date_hash = {}
    opp_pos_date_hash = {}
    _tmp_opp_pos_averages = _org_opp_pos_averages.select{|a| a[:average_type] == "opponent vs starter #{position}"}
    games.each_with_index{|game,i|
      begin
        #jlk!!!!!!!!!!! change the -1 to 0 to get prediction?
      player_date_hash[ (Date.parse(game[:date_of_actual_stats]) - 1).to_s ] = boxscores[i][:PLAYER_NAME]
      rescue StandardError => e
        binding.pry
        p 'duplicate player problem'
        next
      end
      date_hash[ (Date.parse(game[:date_of_actual_stats]) - 1).to_s ] = boxscores[i][:TEAM_ABBREVIATION]
      opp_date_hash[ (Date.parse(game[:date_of_actual_stats]) - 1).to_s ] = opponents[i][:TEAM_ABBREVIATION]
      opp_pos_date_hash[ (Date.parse(game[:date_of_actual_stats]) - 1).to_s ] = opponents[i][:TEAM_ABBREVIATION]
      if (games.size - 1) == i 
        date_str = date_str + "date = '#{(Date.parse(game[:date_of_actual_stats])-1).to_s}'"
        team_date_str = team_date_str + "(team_abbreviation = '#{boxscores[i][:TEAM_ABBREVIATION]}' and date = '#{(Date.parse(game[:date_of_actual_stats])-1).to_s}')"
        opp_date_str = opp_date_str + "((not (team_abbreviation = '#{boxscores[i][:TEAM_ABBREVIATION]}')) and date = '#{(Date.parse(game[:date_of_actual_stats])-1).to_s}')"
        opp_pos_date_str = opp_pos_date_str + "(( (opponent_against_abbr = '#{opponents[i][:TEAM_ABBREVIATION]}')) and date = '#{(Date.parse(game[:date_of_actual_stats])-1).to_s}')"
      else
        date_str = date_str + "date = '#{(Date.parse(game[:date_of_actual_stats])-1).to_s}' or "
        team_date_str = team_date_str + "(team_abbreviation = '#{boxscores[i][:TEAM_ABBREVIATION]}' and date = '#{(Date.parse(game[:date_of_actual_stats])-1).to_s}') or "
        opp_date_str = opp_date_str + "((not (team_abbreviation = '#{boxscores[i][:TEAM_ABBREVIATION]}')) and date = '#{(Date.parse(game[:date_of_actual_stats])-1).to_s}') or "
        opp_pos_date_str = opp_pos_date_str + "(( (opponent_against_abbr = '#{opponents[i][:TEAM_ABBREVIATION]}')) and date = '#{(Date.parse(game[:date_of_actual_stats])-1).to_s}') or "
      end
    }

    org_averages = _org_averages.select{|g| player_date_hash[ g[:date] ] and player_date_hash[ g[:date] ] == g[:player_name]};
    #org_averages = database[:"_#{season.gsub("-","_")}_#{type}_daily_averages"].where(:player_name => player[:PLAYER_NAME], :average_type => nil).where(Sequel.lit(date_str)).order(:date).entries
    org_averages = org_averages.uniq{|o| o[:date]}
    #org_averages.unshift({} )
    averages = {}
    org_averages.each{|avg|
      averages[ avg[:date] ] = avg
    }

    org_team_averages2 = _org_team_averages.select{|g| date_hash[ g[:date] ] and date_hash[ g[:date ] ] == g[:team_abbreviation]};
    #org_team_averages = database[:"_#{season.gsub("-","_")}_#{type}_daily_averages"].where(:average_type => nil, :player_name => nil).where(Sequel.lit(team_date_str)).order(:date).entries
    #org_team_averages = org_team_averages.uniq{|o| o[:date]}
    #if games.first[:date_of_actual_stats] == team_season_start_date
      #org_team_averages.unshift({} )
    #end
    team_averages = {}
    org_team_averages2.each{|avg|
      team_averages[ avg[:date] ] = avg
    }
    #org_opp_averages = database[:"_#{season.gsub("-","_")}_#{type}_daily_averages"].where(:average_type => nil).where(Sequel.lit(opp_date_str)).order(:date).entries
    org_opp_averages = _org_team_averages.select{|g| opp_date_hash[ g[:date ] ] and opp_date_hash[ g[:date ] ] == g[:team_abbreviation]};
    #org_opp_averages = _org_opp_averages.uniq{|o| o[:date]}
    #if games.first[:date_of_actual_stats] == opp_season_start_date
      #org_opp_averages.unshift({} )
    #end
    opp_averages = {}
    org_opp_averages.each{|avg|
      opp_averages[ avg[:date] ] = avg
    }
    #org_opp_pos_averages = database[:"_#{season.gsub("-","_")}_#{type}_daily_averages"].where(:average_type => "opponent vs starter #{position}").where(Sequel.lit(opp_pos_date_str)).order(:date).entries
    tmp_opp_pos_averages = _tmp_opp_pos_averages.select{|g| opp_pos_date_hash[ g[:date] ] and opp_pos_date_hash[ g[:date ] ] == g[:opponent_against_abbr]};
    #org_opp_pos_averages = org_opp_pos_averages.uniq{|o| o[:date]}
    #if games.first[:date_of_actual_stats] == opp_season_start_date
      #org_opp_pos_averages.unshift({} )
    #end
    opp_pos_averages = {}
    tmp_opp_pos_averages.each{|avg|
      opp_pos_averages[ avg[:date] ] = avg
    }
=begin
    org_league_averages = database[:"_#{season.gsub("-","_")}_#{type}_daily_team_averages"].where(:average_type => nil).where(Sequel.lit(date_str)).order(:date).entries
    org_league_averages = org_league_averages.uniq{|o| o[:date]}
    if games.first[:date_of_actual_stats] == season_start_date
      org_league_averages.unshift({} )
    end
    league_averages = {}
    org_league_averages.each{|avg|
      league_averages[ avg[:date] ] = avg
    }

    org_league_pos_averages = database[:"_#{season.gsub("-","_")}_#{type}_daily_team_averages"].where(:average_type => "opponent vs starter #{position}").where(Sequel.lit(date_str)).order(:date).entries
    org_league_pos_averages = org_league_pos_averages.uniq{|o| o[:date]}
    if games.first[:date_of_actual_stats] == season_start_date
      org_league_pos_averages.unshift({} )
    end
    league_pos_averages = {}
    org_league_pos_averages.each{|avg|
      league_pos_averages[ avg[:date] ] = avg
    }
=end
    league_pos_averages = league_position_averages[ position ]
    league_pos_averages = league_pos_averages.select{|k,v| date_hash[ v[:date] ] };

    #if games.size != org_averages.size or games.size != org_team_averages.size or games.size != org_opp_averages.size or games.size != league_averages.size or games.size != league_pos_averages.size
    #if games.size != org_averages.size or games.size != tmp_opp_pos_averages.size or games.size != league_pos_averages.size
    if games.size != (averages.size+1) or games.size != opp_pos_averages.size or games.size != league_pos_averages.size
      p "games sizes are mismatched!! #{games.size} #{averages.size} #{opp_pos_averages.size} #{league_pos_averages.size}"
      #binding.pry
      #p 'games sizes are mismatched!!'
    end

    p "#{player[:PLAYER_NAME]} has #{games.size} games"

    arrSeconds = Array.new
    arrPoints = Array.new
    arrOREBs = Array.new
    arrDREBs = Array.new
    arrASTs = Array.new
    arrTOVs = Array.new
    arrBLKs = Array.new
    arrSteals = Array.new
    total_seconds = 0.0
    total_points = 0.0
    total_orebs = 0.0
    total_drebs = 0.0
    total_assists = 0.0
    total_turnovers = 0.0
    total_blocks = 0.0
    total_steals = 0.0
    total_starter_seconds = 0.0
    total_starter_points = 0.0
    total_starter_orebs = 0.0
    total_starter_drebs = 0.0
    total_starter_assists = 0.0
    total_starter_turnovers = 0.0
    total_starter_blocks = 0.0
    total_starter_steals = 0.0
    total_bench_seconds = 0.0
    total_bench_points = 0.0
    total_bench_orebs = 0.0
    total_bench_drebs = 0.0
    total_bench_assists = 0.0
    total_bench_turnovers = 0.0
    total_bench_blocks = 0.0
    total_bench_steals = 0.0
    starter_games = 0
    bench_games = 0

    games.each_with_index{|data,ind|
      average_date = (Date.parse(data[:date_of_actual_stats]) - 1).to_s
=begin
      if ind > 0 and data[:date_of_actual_stats] != (Date.parse(averages[average_date][:date])+1).to_s
        binding.pry
        p 'hi'
      end
=end
      if cutoff_date > Date.parse(data[:date_of_actual_stats]) or ind < 4 or data[:prev_mean_PTS].to_f < 10.0
        #next
      end
      if cutoff_date > Date.parse(data[:date_of_actual_stats]) or ind < 4 or data[:mean_OREB].to_f < 0.5
        #next
      end
      #if cutoff_date > Date.parse(data[:date_of_actual_stats]) or ind < 4 #or data[:mean_DREB].to_f < 2.0
      if ind < 4
        next
      end
      if ind > 0 and 0.0 >= data[:league_average_def_rtg_v_position].to_f  
        binding.pry
        p "leagueaverage_def_rtg is: #{data[:league_average_def_rtg_v_position].to_f}"
      end

      if "" == data[:b2b] or nil == data[:b2b]
        data[:b2b] = "0"
      end
      if "" == data[:extra_rest] or nil == data[:extra_rest]
        data[:extra_rest] = "0"
      end
      if "" == data[:opp_b2b] or nil == data[:opp_b2b]
        data[:opp_b2b] = "0"
      end
      if "" == data[:opp_extra_rest] or nil == data[:opp_extra_rest]
        data[:opp_extra_rest] = "0"
      end
      if nil == data[:prev_mean_PTS] or "" == data[:prev_mean_PTS]
        data[:prev_mean_PTS] = "0"
      end

      mean_seconds = total_seconds / (ind+1).to_f
      data[:mean_seconds] = mean_seconds
      mean_starter_seconds = divide(total_starter_seconds, starter_games)
      mean_starter_pts = divide(total_starter_points, starter_games)
      mean_starter_oreb = divide(total_starter_orebs, starter_games)
      mean_starter_dreb = divide(total_starter_drebs, starter_games)
      mean_starter_ast = divide(total_starter_assists, starter_games)
      mean_starter_tov = divide(total_starter_turnovers, starter_games)
      mean_starter_blk = divide(total_starter_blocks, starter_games)
      mean_starter_stl = divide(total_starter_steals, starter_games)
      mean_bench_seconds = divide(total_bench_seconds, bench_games)
      mean_bench_pts = divide(total_bench_points, bench_games)
      mean_bench_oreb = divide(total_bench_orebs, bench_games)
      mean_bench_dreb = divide(total_bench_drebs, bench_games)
      mean_bench_ast = divide(total_bench_assists, bench_games)
      mean_bench_tov = divide(total_bench_turnovers, bench_games)
      mean_bench_blk = divide(total_bench_blocks, bench_games)
      mean_bench_stl = divide(total_bench_steals, bench_games)

      mean_starterbench_seconds = 0.0
      mean_starterbench_pts = 0.0
      mean_starterbench_oreb = 0.0
      mean_starterbench_dreb = 0.0
      mean_starterbench_ast = 0.0
      mean_starterbench_tov = 0.0
      mean_starterbench_blk = 0.0
      mean_starterbench_stl = 0.0
      if "1" == data[:starter]
        total_starter_seconds = total_starter_seconds + data[:actual_SECONDS].to_f
        total_starter_points = total_starter_points + data[:actual_PTS].to_f
        total_starter_orebs = total_starter_orebs + data[:actual_OREB].to_f
        total_starter_drebs = total_starter_drebs + data[:actual_DREB].to_f
        total_starter_assists = total_starter_assists + data[:actual_AST].to_f
        total_starter_turnovers = total_starter_turnovers + data[:actual_TOV].to_f
        total_starter_blocks = total_starter_blocks + data[:actual_BLK].to_f
        total_starter_steals = total_starter_steals + data[:actual_STL].to_f
        starter_games = starter_games + 1
        mean_starterbench_seconds = mean_starter_seconds
        mean_starterbench_pts = mean_starter_pts
        mean_starterbench_oreb = mean_starter_oreb
        mean_starterbench_dreb = mean_starter_dreb
        mean_starterbench_ast = mean_starter_ast
        mean_starterbench_tov = mean_starter_tov
        mean_starterbench_blk = mean_starter_blk
        mean_starterbench_stl = mean_starter_stl
      elsif "0" == data[:starter]
        total_bench_seconds = total_bench_seconds + data[:actual_SECONDS].to_f
        total_bench_points = total_bench_points + data[:actual_PTS].to_f
        total_bench_orebs = total_bench_orebs + data[:actual_OREB].to_f
        total_bench_drebs = total_bench_drebs + data[:actual_DREB].to_f
        total_bench_assists = total_bench_assists + data[:actual_AST].to_f
        total_bench_turnovers = total_bench_turnovers + data[:actual_TOV].to_f
        total_bench_blocks = total_bench_blocks + data[:actual_BLK].to_f
        total_bench_steals = total_bench_steals + data[:actual_STL].to_f
        bench_games = bench_games + 1
        mean_starterbench_seconds = mean_bench_seconds
        mean_starterbench_pts = mean_bench_pts
        mean_starterbench_oreb = mean_bench_oreb
        mean_starterbench_dreb = mean_bench_dreb
        mean_starterbench_ast = mean_bench_ast
        mean_starterbench_tov = mean_bench_tov
        mean_starterbench_blk = mean_bench_blk
        mean_starterbench_stl = mean_bench_stl
      else
        binding.pry
        p "hi"
      end

=begin
      column :point_spread_abs_3_or_less, :decimal
      column :point_spread_abs_6_or_less, :decimal
      column :point_spread_abs_9_or_less, :decimal
      column :point_spread_abs_12_or_less, :decimal
      column :point_spread_abs_over_9, :decimal
      column :point_spread_abs_over_12, :decimal
      column :point_spread_pos_3_or_less, :decimal
      column :point_spread_pos_6_or_less, :decimal
      column :point_spread_pos_9_or_less, :decimal
      column :point_spread_pos_12_or_less, :decimal
      column :point_spread_pos_over_9, :decimal
      column :point_spread_pos_over_12, :decimal
      column :point_spread_neg_3_or_less, :decimal
      column :point_spread_neg_6_or_less, :decimal
      column :point_spread_neg_9_or_less, :decimal
      column :point_spread_neg_12_or_less, :decimal
      column :point_spread_neg_over_9, :decimal
      column :point_spread_neg_over_12, :decimal
      column :est_team_PTS_ratio, :decimal
      column :est_opp_PTS_ratio, :decimal
      column :est_PACE_ratio, :decimal
=end
      starterbench_pts_effect = mean_starterbench_pts - data[:prev_mean_PTS].to_f
      starterbench_oreb_effect = mean_starterbench_oreb - data[:mean_OREB].to_f
      starterbench_dreb_effect = mean_starterbench_dreb - data[:mean_DREB].to_f
      starterbench_ast_effect = mean_starterbench_ast - data[:mean_AST].to_f
      starterbench_tov_effect = mean_starterbench_tov - data[:mean_TOV].to_f
      starterbench_blk_effect = mean_starterbench_blk - data[:mean_BLK].to_f
      starterbench_stl_effect = mean_starterbench_stl - data[:mean_STL].to_f
      prev_seconds = previousAverageN( arrSeconds, 1 )
      prev2_seconds = previousAverageN( arrSeconds, 2 )
      prev5_seconds = previousAverageN( arrSeconds, 5 )
      prev_points = previousAverageN( arrPoints, 1 )
      prev2_points = previousAverageN( arrPoints, 2 )
      prev5_points = previousAverageN( arrPoints, 5 )
      prev_orebs = previousAverageN( arrOREBs, 1 )
      prev2_orebs = previousAverageN( arrOREBs, 2 )
      prev5_orebs = previousAverageN( arrOREBs, 5 )
      prev_drebs = previousAverageN( arrDREBs, 1 )
      prev2_drebs = previousAverageN( arrDREBs, 2 )
      prev5_drebs = previousAverageN( arrDREBs, 5 )
      prev_assists = previousAverageN( arrASTs, 1 )
      prev2_assists = previousAverageN( arrASTs, 2 )
      prev5_assists = previousAverageN( arrASTs, 5 )
      prev_turnovers = previousAverageN( arrTOVs, 1 )
      prev2_turnovers = previousAverageN( arrTOVs, 2 )
      prev5_turnovers = previousAverageN( arrTOVs, 5 )
      prev_blocks = previousAverageN( arrBLKs, 1 )
      prev2_blocks = previousAverageN( arrBLKs, 2 )
      prev5_blocks = previousAverageN( arrBLKs, 5 )
      prev_steals = previousAverageN( arrSteals, 1 )
      prev2_steals = previousAverageN( arrSteals, 2 )
      prev5_steals = previousAverageN( arrSteals, 5 )

      prev_seconds_delta = prev_seconds - mean_seconds
      prev2_seconds_delta = prev2_seconds - mean_seconds
      prev5_seconds_delta = prev5_seconds - mean_seconds
      prev_points_delta = prev_points - data[:prev_mean_PTS].to_f
      prev2_points_delta = prev2_points - data[:prev_mean_PTS].to_f
      prev5_points_delta = prev5_points - data[:prev_mean_PTS].to_f
      prev_orebs_delta = prev_orebs - data[:mean_OREB].to_f
      prev2_orebs_delta = prev2_orebs - data[:mean_OREB].to_f
      prev5_orebs_delta = prev5_orebs - data[:mean_OREB].to_f
      prev_drebs_delta = prev_drebs - data[:mean_DREB].to_f
      prev2_drebs_delta = prev2_drebs - data[:mean_DREB].to_f
      prev5_drebs_delta = prev5_drebs - data[:mean_DREB].to_f
      prev_assists_delta = prev_assists - data[:mean_AST].to_f
      prev2_assists_delta = prev2_assists - data[:mean_AST].to_f
      prev5_assists_delta = prev5_assists - data[:mean_AST].to_f
      prev_turnovers_delta = prev_turnovers - data[:mean_TOV].to_f
      prev2_turnovers_delta = prev2_turnovers - data[:mean_TOV].to_f
      prev5_turnovers_delta = prev5_turnovers - data[:mean_TOV].to_f
      prev_blocks_delta = prev_blocks - data[:mean_BLK].to_f
      prev2_blocks_delta = prev2_blocks - data[:mean_BLK].to_f
      prev5_blocks_delta = prev5_blocks - data[:mean_BLK].to_f
      prev_steals_delta = prev_steals - data[:mean_STL].to_f
      prev2_steals_delta = prev2_steals - data[:mean_STL].to_f
      prev5_steals_delta = prev5_steals - data[:mean_STL].to_f

      arrSeconds.push data[:actual_SECONDS].to_f
      arrPoints.push data[:actual_PTS].to_f
      arrOREBs.push data[:actual_OREB].to_f
      arrDREBs.push data[:actual_DREB].to_f
      arrASTs.push data[:actual_AST].to_f
      arrTOVs.push data[:actual_TOV].to_f
      arrBLKs.push data[:actual_BLK].to_f
      arrSteals.push data[:actual_STL].to_f
      total_seconds = total_seconds + data[:actual_SECONDS].to_f
      total_points = total_points + data[:actual_PTS].to_f
      total_orebs = total_orebs + data[:actual_OREB].to_f
      total_drebs = total_drebs + data[:actual_DREB].to_f
      total_assists = total_assists + data[:actual_AST].to_f
      total_turnovers = total_turnovers + data[:actual_TOV].to_f
      total_blocks = total_blocks + data[:actual_BLK].to_f
      total_steals = total_steals + data[:actual_STL].to_f

      mean_pts_per_min = divide(60*data[:prev_mean_PTS].to_f, data[:mean_seconds].to_f)

      rest_pts = data[:prev_mean_PTS].to_f
      rest_orebs = data[:mean_OREB].to_f
      rest_drebs = data[:mean_DREB].to_f
      rest_assists = data[:mean_AST].to_f
      rest_turnovers = data[:mean_TOV].to_f
      rest_blocks = data[:mean_BLK].to_f
      rest_steals = data[:mean_STL].to_f
      rest_pts_per_min = divide(60*data[:prev_mean_PTS].to_f, data[:mean_seconds].to_f)
      rest_seconds = data[:mean_seconds].to_f
      if "1" == data[:b2b]
        rest_pts = data[:mean_b2b_PTS].to_f
        rest_orebs = data[:mean_b2b_OREB].to_f
        rest_drebs = data[:mean_b2b_DREB].to_f
        rest_assists = data[:mean_b2b_AST].to_f
        rest_turnovers = data[:mean_b2b_TOV].to_f
        rest_blocks = data[:mean_b2b_BLK].to_f
        rest_steals = data[:mean_b2b_STL].to_f
        rest_pts_per_min = divide(60*data[:mean_b2b_PTS].to_f, data[:mean_b2b_seconds].to_f)
        rest_seconds = data[:mean_b2b_seconds].to_f
=begin
      elsif "1" == data[:front_b2b]
        if "1" == data[:extra_rest]
          rest_pts = (data[:mean_front_b2b_PTS].to_f + data[:mean_extra_rest_PTS].to_f) / 2
          rest_orebs = data[:mean_front_b2b_OREB].to_f
          rest_drebs = data[:mean_front_b2b_DREB].to_f
          rest_assists = data[:mean_front_b2b_AST].to_f
          rest_turnovers = data[:mean_front_b2b_TOV].to_f
          rest_blocks = data[:mean_front_b2b_BLK].to_f
          rest_pts_per_min = divide(60*(data[:mean_front_b2b_PTS].to_f, data[:mean_front_b2b_seconds].to_f + data[:mean_extra_rest_PTS].to_f / data[:mean_extra_rest_seconds].to_f) / 2)
        else
          rest_pts = data[:mean_front_b2b_PTS].to_f
          rest_pts_per_min = divide(60*data[:mean_front_b2b_PTS].to_f, data[:mean_front_b2b_seconds].to_f)
        end
=end
      elsif "1" == data[:extra_rest]
        rest_pts = data[:mean_extra_rest_PTS].to_f
        rest_orebs = data[:mean_extra_rest_OREB].to_f
        rest_drebs = data[:mean_extra_rest_DREB].to_f
        rest_assists = data[:mean_extra_rest_AST].to_f
        rest_turnovers = data[:mean_extra_rest_TOV].to_f
        rest_blocks = data[:mean_extra_rest_BLK].to_f
        rest_steals = data[:mean_extra_rest_STL].to_f
        rest_pts_per_min = divide(60*data[:mean_extra_rest_PTS].to_f, data[:mean_extra_rest_seconds].to_f)
        rest_seconds = data[:mean_extra_rest_seconds].to_f
      else # "1" == data[:non_b2b]
        #rest_pts = data[:mean_non_b2b_PTS].to_f
      end
      rest_effect = rest_pts - data[:prev_mean_PTS].to_f
      rest_oreb_effect = rest_orebs - data[:mean_OREB].to_f
      rest_dreb_effect = rest_drebs - data[:mean_DREB].to_f
      rest_assist_effect = rest_assists - data[:mean_AST].to_f
      rest_turnover_effect = rest_turnovers - data[:mean_TOV].to_f
      rest_block_effect = rest_blocks - data[:mean_BLK].to_f
      rest_steal_effect = rest_steals - data[:mean_STL].to_f
      rest_effect_per_min = rest_pts_per_min - mean_pts_per_min
      rest_effect_seconds = rest_seconds - data[:mean_seconds].to_f

      opp_rest_pts = data[:prev_mean_PTS].to_f
      opp_rest_orebs = data[:mean_OREB].to_f
      opp_rest_drebs = data[:mean_DREB].to_f
      opp_rest_assists = data[:mean_AST].to_f
      opp_rest_turnovers = data[:mean_TOV].to_f
      opp_rest_blocks = data[:mean_BLK].to_f
      opp_rest_steals = data[:mean_STL].to_f
      opp_rest_pts_per_min = divide(60*data[:prev_mean_PTS].to_f, data[:mean_seconds].to_f)
      opp_rest_seconds = data[:mean_seconds].to_f
      if "1" == data[:opp_b2b]
        opp_rest_pts = data[:mean_opp_b2b_PTS].to_f
        opp_rest_orebs = data[:mean_opp_b2b_OREB].to_f
        opp_rest_drebs = data[:mean_opp_b2b_DREB].to_f
        opp_rest_assists = data[:mean_opp_b2b_AST].to_f
        opp_rest_turnovers = data[:mean_opp_b2b_TOV].to_f
        opp_rest_blocks = data[:mean_opp_b2b_BLK].to_f
        opp_rest_steals = data[:mean_opp_b2b_STL].to_f
        opp_rest_pts_per_min = divide(60*data[:mean_opp_b2b_PTS].to_f, data[:mean_opp_b2b_seconds].to_f)
        opp_rest_seconds = data[:mean_opp_b2b_seconds].to_f
=begin
      elsif "1" == data[:opp_front_b2b]
        if "1" == data[:opp_extra_rest]
          opp_rest_pts = (data[:mean_opp_front_b2b_PTS].to_f + data[:mean_opp_extra_rest_PTS].to_f) / 2
          opp_rest_pts_per_min = 60*(divide(data[:mean_opp_front_b2b_PTS].to_f, data[:mean_opp_front_b2b_seconds].to_f) + divide(data[:mean_opp_extra_rest_PTS].to_f, data[:mean_opp_extra_rest_seconds].to_f)) / 2
        else
          opp_rest_pts = data[:mean_opp_front_b2b_PTS].to_f
          opp_rest_pts_per_min = divide(60*data[:mean_opp_front_b2b_PTS].to_f, data[:mean_opp_front_b2b_seconds].to_f)
        end
=end
      elsif "1" == data[:opp_extra_rest]
        opp_rest_pts = data[:mean_opp_extra_rest_PTS].to_f
        opp_rest_orebs = data[:mean_opp_extra_rest_OREB].to_f
        opp_rest_drebs = data[:mean_opp_extra_rest_DREB].to_f
        opp_rest_assists = data[:mean_opp_extra_rest_AST].to_f
        opp_rest_turnovers = data[:mean_opp_extra_rest_TOV].to_f
        opp_rest_blocks = data[:mean_opp_extra_rest_BLK].to_f
        opp_rest_steals = data[:mean_opp_extra_rest_STL].to_f
        opp_rest_pts_per_min = divide(60*data[:mean_opp_extra_rest_PTS].to_f, data[:mean_opp_extra_rest_seconds].to_f)
        opp_rest_seconds = data[:mean_opp_extra_rest_seconds].to_f
      else
        #opp_rest_pts = data[:mean_opp_non_b2b_PTS].to_f
      end
      opp_rest_effect = opp_rest_pts - data[:prev_mean_PTS].to_f
      opp_rest_oreb_effect = opp_rest_orebs - data[:mean_OREB].to_f
      opp_rest_dreb_effect = opp_rest_drebs - data[:mean_DREB].to_f
      opp_rest_assist_effect = opp_rest_assists - data[:mean_AST].to_f
      opp_rest_turnover_effect = opp_rest_turnovers - data[:mean_TOV].to_f
      opp_rest_block_effect = opp_rest_blocks - data[:mean_BLK].to_f
      opp_rest_steal_effect = opp_rest_steals - data[:mean_STL].to_f
      opp_rest_effect_per_min = opp_rest_pts_per_min - mean_pts_per_min
      opp_rest_effect_seconds = opp_rest_seconds - data[:mean_seconds].to_f

      location_pts = ("home" == data[:location]) ? data[:mean_home_PTS].to_f : data[:mean_away_PTS].to_f
      location_orebs = ("home" == data[:location]) ? data[:mean_home_OREB].to_f : data[:mean_away_OREB].to_f
      location_drebs = ("home" == data[:location]) ? data[:mean_home_DREB].to_f : data[:mean_away_DREB].to_f
      location_assists = ("home" == data[:location]) ? data[:mean_home_AST].to_f : data[:mean_away_AST].to_f
      location_turnovers = ("home" == data[:location]) ? data[:mean_home_TOV].to_f : data[:mean_away_TOV].to_f
      location_blocks = ("home" == data[:location]) ? data[:mean_home_BLK].to_f : data[:mean_away_BLK].to_f
      location_steals = ("home" == data[:location]) ? data[:mean_home_STL].to_f : data[:mean_away_STL].to_f
      location_pts_per_min = ("home" == data[:location]) ? 60*divide(data[:mean_home_PTS].to_f, data[:mean_home_seconds].to_f) : divide(60*data[:mean_away_PTS].to_f, data[:mean_away_seconds].to_f)
      location_seconds = ("home" == data[:location]) ? data[:mean_home_seconds].to_f : data[:mean_away_seconds].to_f
      location_effect = location_pts - data[:prev_mean_PTS].to_f
      location_oreb_effect = location_orebs - data[:mean_OREB].to_f
      location_dreb_effect = location_drebs - data[:mean_DREB].to_f
      location_assist_effect = location_assists - data[:mean_AST].to_f
      location_turnover_effect = location_turnovers - data[:mean_TOV].to_f
      location_block_effect = location_blocks - data[:mean_BLK].to_f
      location_steal_effect = location_steals - data[:mean_STL].to_f
      location_effect_per_min = location_pts_per_min - mean_pts_per_min
      location_effect_seconds = location_seconds - data[:mean_seconds].to_f

      r[:expected_PACE] = expected_pace = (team_average[:PACE].to_f + opp_average[:PACE].to_f) / 2
      r[:pace_ratio] = pace_ratio = divide( expected_pace, team_average[:PACE].to_f )

      r[:expected_PACE2] = expected_pace2 = (team_average[:PACE].to_f - league_average_pace) + (opp_average[:PACE].to_f - league_average_pace) + league_average_pace
      r[:pace_ratio2] = pace_ratio2 = divide( expected_pace2, team_average[:PACE].to_f )

      r[:projected_PACE3] = expected_pace3 = 1 / ( ( 1 / team_average[:PACE].to_f ) + ( 1 / opp_average[:PACE].to_f ) - ( 1 / league_average_pace ) )
      r[:pace_ratio3] = pace_ratio3 = divide( expected_pace3, team_average[:PACE].to_f )

      r[:expected_PTS_pace] = expected_PTS_pace[i] = previous_average[:PTS_mean].to_f * pace_ratio

      #team_average[:PACE] + opp_average[:PACE]
      expected_PTS_pace = 0 #jlk previous_average[:
      pts_pace_effect = expected_pts_pace - data[:prev_mean_PTS].to_f
      pts_pace_effect_per_min = divide(60*pts_pace_effect, data[:mean_seconds].to_f)

      expected_pts_pace2 = 
      pts_pace_effect2 = expected_pts_pace2 - data[:prev_mean_PTS].to_f
      pts_pace_effect_per_min2 = divide(60*pts_pace_effect2, data[:mean_seconds].to_f)

      expected_pts_pace3 = ( team_average[:PACE].to_f + opp_average[:PACE].to_f ) / 2
      pts_pace_effect3 = expected_pts_pace3 - data[:prev_mean_PTS].to_f
      pts_pace_effect_per_min3 = divide(60*pts_pace_effect3, data[:mean_seconds].to_f)

      expected_PTS_def_rtg_v_position = divide(data[:prev_mean_PTS].to_f * data[:league_average_def_rtg_v_position].to_f, data[:o_team_def_rtg_v_position].to_f)
      def_rtg_v_position_effect = expected_PTS_def_rtg_v_position - data[:prev_mean_PTS].to_f
      def_rtg_v_position_effect_per_min = divide(60*def_rtg_v_position_effect, data[:mean_seconds].to_f)

      expected_PTS_def_rtg = divide(data[:prev_mean_PTS].to_f * data[:league_average_def_rtg].to_f, data[:o_team_def_rtg].to_f)
      def_rtg_effect = expected_PTS_def_rtg - data[:prev_mean_PTS].to_f
      def_rtg_effect_per_min = divide(60*def_rtg_effect, data[:mean_seconds].to_f)

      expected_PTS_opp_PTS = divide(data[:prev_mean_PTS].to_f * data[:league_average_PTS_v_position].to_f, data[:o_team_PTS_v_position].to_f)
      expected_PTS_opp_PTS_effect = expected_PTS_opp_PTS - data[:prev_mean_PTS].to_f
      expected_PTS_opp_PTS_effect_per_min = divide(60*expected_PTS_opp_PTS_effect, data[:mean_seconds].to_f)

      fb_effect = data[:prev_PTS_FB_mean].to_f * divide(data[:opp_o_PTS_FB_mean].to_f, data[:team_PTS_FB_mean].to_f) - data[:prev_PTS_FB_mean].to_f
      fb_effect_per_min = divide(60*fb_effect, data[:mean_seconds].to_f)
      #data[:league_average_PTS_FB]
      #data[:opp_o_PTS_FB_mean_v_position]
      #data[:league_average_PTS_FB_v_position]
      #data[:prev_PCT_PTS_FB]
      #data[:prev_opp_o_PCT_PTS_FB]
      pts_paint_effect = data[:prev_PTS_PAINT_mean].to_f * divide(data[:opp_o_PTS_PAINT_mean].to_f, data[:team_PTS_PAINT_mean].to_f) - data[:prev_PTS_PAINT_mean].to_f
      pts_paint_effect_per_min = divide(60*pts_paint_effect, data[:mean_seconds].to_f)

      pts_2ndchance_effect = data[:prev_PTS_2ND_CHANCE_mean].to_f * divide(data[:opp_o_PTS_2ND_CHANCE_mean].to_f, data[:team_PTS_2ND_CHANCE_mean].to_f) - data[:prev_PTS_2ND_CHANCE_mean].to_f
      pts_2ndchance_effect_per_min = divide(60*pts_2ndchance_effect, data[:mean_seconds].to_f)

      pts_off_tov_effect = data[:prev_PTS_OFF_TOV_mean].to_f * divide(data[:opp_o_PTS_OFF_TOV_mean].to_f, data[:team_PTS_OFF_TOV_mean].to_f) - data[:prev_PTS_OFF_TOV_mean].to_f
      pts_off_tov_effect_per_min = divide(60*pts_off_tov_effect, data[:mean_seconds].to_f)

      fg_pts = data[:prev_mean_PTS].to_f - data[:prev_mean_FTM].to_f
      pts_per_fg = divide(fg_pts, data[:prev_mean_FG3M].to_f + data[:prev_mean_FG2M].to_f)
      cfg_pts = pts_per_fg * data[:prev_PCT_CFGM].to_f
      ufg_pts = pts_per_fg * data[:prev_PCT_UFGM].to_f

      pct_cfga_ratio = divide(data[:opp_o_PCT_CFGA].to_f, data[:prev_PCT_CFGA].to_f) 
      pct_cfga_pos_ratio = divide(data[:opp_o_PCT_CFGA_v_position].to_f, data[:prev_PCT_CFGA].to_f) 
      pct_cfga_mixed_ratio = (pct_cfga_ratio + pct_cfga_pos_ratio) / 2
      adjusted_CFGA = data[:prev_CFGA_mean].to_f * pct_cfga_mixed_ratio 

      adjusted_opp_cfg_pct = divide(data[:opp_o_CFG_PCT].to_f + data[:opp_o_CFG_PCT_v_position].to_f, 2)
      adjusted_cfg_pct = (data[:prev_CFG_PCT].to_f + adjusted_opp_cfg_pct) / 2
      adjusted_cfgm = adjusted_CFGA * adjusted_cfg_pct

      adjusted_cfg_pts = adjusted_cfgm * pts_per_fg

      pct_ufga_ratio = divide(data[:opp_o_PCT_UFGA].to_f, data[:prev_PCT_UFGA].to_f) 
      pct_ufga_pos_ratio = divide(data[:opp_o_PCT_UFGA_v_position].to_f, data[:prev_PCT_UFGA].to_f) 
      pct_ufga_mixed_ratio = (pct_ufga_ratio + pct_ufga_pos_ratio) / 2
      adjusted_UFGA = data[:prev_UFGA_mean].to_f * pct_ufga_mixed_ratio 

      adjusted_opp_ufg_pct = divide(data[:opp_o_UFG_PCT].to_f + data[:opp_o_UFG_PCT_v_position].to_f, 2)
      adjusted_ufg_pct = (data[:prev_UFG_PCT].to_f + adjusted_opp_ufg_pct) / 2
      adjusted_ufgm = adjusted_UFGA * adjusted_cfg_pct

      adjusted_ufg_pts = adjusted_ufgm * pts_per_fg
      adjusted_fg_pts = adjusted_ufg_pts + adjusted_cfg_pts

      cfg_effect = adjusted_fg_pts - fg_pts
      cfg_effect_per_min = 0
      cfg_pts_per_min = 0

      pace_effect = data[:expected_pts_pace].to_f - data[:prev_mean_PTS].to_f
      pace_effect2 = data[:expected_pts_pace2].to_f - data[:prev_mean_PTS].to_f
      pace_effect3 = data[:expected_pts_pace3].to_f - data[:prev_mean_PTS].to_f
      pace_effect_per_min = divide(60*pace_effect, data[:mean_seconds].to_f)
      if pace_effect != pts_pace_effect or pace_effect2 != pts_pace_effect2 or pace_effect3 != pts_pace_effect3
        binding.pry
        p 'hi'
      end

      def_rtg_delta = data[:o_team_def_rtg].to_f - data[:league_average_def_rtg].to_f
      def_rtg_v_position_delta = data[:o_team_def_rtg_v_position].to_f - data[:league_average_def_rtg_v_position].to_f
      o_pts_delta = data[:o_team_PTS_v_position].to_f - data[:league_average_PTS_v_position].to_f

      lpa_OREB = 0;lpa_OREB_PCT = 0;lpa_DREB = 0;lpa_DREB_PCT = 0;lpa_STL = 0;lpa_PCT_STL = 0;
      lpa_AST = 0;lpa_AST_PCT = 0;
      lpa_BLK = 0;lpa_PCT_BLK = 0;
      lpa_TOV = 0;lpa_TO_PCT = 0;
      lpa_seconds = 0;
      if league_pos_averages[average_date]
        lpa_OREB = league_pos_averages[average_date][:team_mean_OREB].to_f
        lpa_OREB_PCT = league_pos_averages[average_date][:team_mean_OREB_PCT].to_f
        lpa_DREB = league_pos_averages[average_date][:team_mean_DREB].to_f
        lpa_DREB_PCT = league_pos_averages[average_date][:team_mean_DREB_PCT].to_f
        lpa_STL = league_pos_averages[average_date][:team_mean_STL].to_f
        lpa_PCT_STL = league_pos_averages[average_date][:team_mean_PCT_STL].to_f
        lpa_AST = league_pos_averages[average_date][:team_mean_AST].to_f
        lpa_AST_PCT = league_pos_averages[average_date][:team_mean_AST_PCT].to_f
        lpa_BLK = league_pos_averages[average_date][:team_mean_BLK].to_f
        lpa_PCT_BLK = league_pos_averages[average_date][:team_mean_PCT_BLK].to_f
        lpa_TOV = league_pos_averages[average_date][:team_mean_TOV].to_f
        lpa_TO_PCT = league_pos_averages[average_date][:team_mean_TOV_PCT].to_f
        lpa_seconds = league_pos_averages[average_date][:team_mean_SECONDS].to_f
      end
      a_OREB_PCT = 0;a_DREB_PCT = 0;a_STL = 0;a_PCT_STL = 0;a_AST_PCT = 0;a_PCT_BLK = 0;
      a_TO_PCT = 0;
      a_DRBC_mean = 0;
      a_ORBC_mean = 0;
      if averages[average_date]
        a_OREB_PCT = averages[average_date][:OREB_PCT].to_f
        a_DREB_PCT = averages[average_date][:DREB_PCT].to_f
        a_DRBC_mean = averages[average_date][:DRBC_mean].to_f
        a_ORBC_mean = averages[average_date][:ORBC_mean].to_f
        a_PCT_STL = averages[average_date][:PCT_STL].to_f
        a_STL = averages[average_date][:STL_mean].to_f
        a_AST_PCT = averages[average_date][:AST_PCT].to_f
        a_PCT_BLK = averages[average_date][:PCT_BLK].to_f
        a_TO_PCT = averages[average_date][:TO_PCT].to_f
      end

      #o_oreb_delta = data[:o_OREB_v_position].to_f - lpa_OREB
      #o_oreb_pct_delta = data[:o_OREB_PCT_v_position].to_f - lpa_OREB_PCT
      o_oreb_delta = data[:o_OREB_v_position].to_f - data[:mean_OREB].to_f
      o_oreb_pct_delta = data[:o_OREB_PCT_v_position].to_f - a_OREB_PCT
      o_dreb_delta = data[:o_DREB_v_position].to_f - lpa_DREB
      o_dreb_pct_delta = data[:o_DREB_PCT_v_position].to_f - lpa_DREB_PCT
      #binding.pry
      
      opa_STL = 0;opa_PCT_STL = 0;opa_AST = 0;opa_BLK = 0;opa_TOV = 0;opa_TO_PCT = 0;
      opa_DREB = 0; opa_DREB_PCT = 0;
      opa_OREB = 0; opa_OREB_PCT = 0;
      opa_DRBC_mean = 0;
      opa_ORBC_mean = 0;
      if opp_pos_averages[average_date]
        opa_STL = opp_pos_averages[average_date][:STL_mean].to_f 
        opa_PCT_STL = opp_pos_averages[average_date][:PCT_STL].to_f 
        opa_AST = opp_pos_averages[average_date][:AST_mean].to_f 
        opa_BLK = opp_pos_averages[average_date][:BLK_mean].to_f 
        opa_TOV = opp_pos_averages[average_date][:TOV_mean].to_f 
        opa_TO_PCT = opp_pos_averages[average_date][:TO_PCT].to_f 
        opa_OREB = opp_pos_averages[average_date][:OREB_mean].to_f 
        opa_OREB_PCT = opp_pos_averages[average_date][:OREB_PCT].to_f
        opa_DREB = opp_pos_averages[average_date][:DREB_mean].to_f 
        opa_DREB_PCT = opp_pos_averages[average_date][:DREB_PCT].to_f 
        opa_DRBC_mean = opp_pos_averages[average_date][:DRBC_mean].to_f
        opa_ORBC_mean = opp_pos_averages[average_date][:ORBC_mean].to_f
      end

      o_ast_delta = opa_AST - lpa_AST
      o_ast_pct_delta = data[:o_team_AST_PCT_v_position].to_f - lpa_AST_PCT
      o_tov_delta = opa_TOV - lpa_TOV
      o_tov_pct_delta = data[:o_team_TO_PCT_v_position].to_f - lpa_TO_PCT
      o_blk_delta = opa_BLK - lpa_BLK
      o_blk_pct_delta = data[:o_team_PCT_BLK_v_position].to_f - lpa_PCT_BLK
      o_stl_delta = opa_STL - lpa_STL
      o_stl_pct_delta = opa_PCT_STL - lpa_PCT_STL

      oa_OREB = 0;oa_OREB_PCT = 0;oa_DREB = 0;oa_DREB_PCT = 0;oa_TOV = 0;
      oa_DRBC_mean = 0;oa_ORBC_mean = 0;
      oa_misses = 0;oa_3p_misses = 0;oa_ft_misses = 0;oa_2pmisses = 0;
      oa_FT_PCT = 0;oa_FG_PCT = 0; oa_FG3_PCT = 0; oa_FG2_PCT = 0;
      if opp_averages[average_date]
        oa_OREB = opp_averages[average_date][:OREB_mean].to_f
        oa_DREB = opp_averages[average_date][:DREB_mean].to_f
        oa_OREB_PCT = opp_averages[average_date][:OREB_PCT].to_f / 100.0
        oa_DREB_PCT = opp_averages[average_date][:DREB_PCT].to_f / 100.0
        oa_TOV = opp_averages[average_date][:TOV_mean].to_f
        oa_DRBC_mean = opp_averages[average_date][:DRBC_mean].to_f
        oa_ORBC_mean = opp_averages[average_date][:ORBC_mean].to_f
        oa_ft_misses = opp_averages[average_date][:FTA_mean].to_f - opp_averages[average_date][:FTM_mean].to_f
        oa_3p_misses = opp_averages[average_date][:FG3A_mean].to_f - opp_averages[average_date][:FG3M_mean].to_f
        oa_2p_misses = opp_averages[average_date][:FG2A_mean].to_f - opp_averages[average_date][:FG2M_mean].to_f
        oa_misses = opp_averages[average_date][:FGA_mean].to_f - opp_averages[average_date][:FGM_mean].to_f
        oa_FT_PCT = opp_averages[average_date][:FT_PCT].to_f
        oa_FG_PCT = opp_averages[average_date][:FG_PCT].to_f
        oa_FG3_PCT = opp_averages[average_date][:FG3_PCT].to_f
        oa_FG2_PCT = opp_averages[average_date][:FG2_PCT].to_f
      end
      team_DRBC_mean = 0;team_ORBC_mean = 0;
      team_misses = 0;team_3p_misses = 0;team_ft_misses = 0;team_2pmisses = 0;
      team_FT_PCT = 0;team_FG_PCT = 0; team_FG3_PCT = 0; team_FG2_PCT = 0;
      if team_averages[average_date]
        team_DRBC_mean = team_averages[average_date][:DRBC_mean].to_f
        team_ORBC_mean = team_averages[average_date][:ORBC_mean].to_f
        team_ft_misses = opp_averages[average_date][:FTA_mean].to_f - opp_averages[average_date][:FTM_mean].to_f
        team_3p_misses = opp_averages[average_date][:FG3A_mean].to_f - opp_averages[average_date][:FG3M_mean].to_f
        team_2p_misses = opp_averages[average_date][:FG2A_mean].to_f - opp_averages[average_date][:FG2M_mean].to_f
        team_misses = opp_averages[average_date][:FGA_mean].to_f - opp_averages[average_date][:FGM_mean].to_f
        team_FT_PCT = opp_averages[average_date][:FT_PCT].to_f
        team_FG_PCT = opp_averages[average_date][:FG_PCT].to_f
        team_FG3_PCT = opp_averages[average_date][:FG3_PCT].to_f
        team_FG2_PCT = opp_averages[average_date][:FG2_PCT].to_f
      end

      pos = positions[ player[:PLAYER_ID] ]
      
      o_pts_delta_per_min = 60* (divide(data[:o_team_PTS_v_position].to_f, data[:opponent_o_team_SECONDS_v_position].to_f) - divide(data[:league_average_PTS_v_position].to_f, lpa_seconds))

      actual_pts_per_min = divide(60*data[:actual_PTS].to_f, data[:actual_SECONDS].to_f)
      actual_mins = data[:actual_SECONDS].to_f / 60.0

      location = nil
      if "home" == data[:location]
        location = 1
      else
        location = 0
      end
      #expected_FTA_v_position = data[:prev_mean_FTA].to_f * divide( data[:prev_team_FTA_mean].to_f, data[:prev_o_team_FTA].to_f ) 
      ft_pct = divide( data[:prev_mean_FTM].to_f, data[:prev_mean_FTA].to_f )
      expected_FTA = data[:prev_mean_FTA].to_f * divide( data[:prev_team_FTA_mean].to_f, data[:prev_o_team_FTA].to_f ) 
      expected_FTM = ft_pct * expected_FTA
      extra_FTA = expected_FTA - data[:prev_mean_FTA].to_f
      ft_effect = ft_pct * extra_FTA
      

      expected_OREB = data[:mean_OREB].to_f * divide(data[:opponent_o_team_OREB].to_f, data[:team_OREB].to_f)
      expected_OREB_effect = data[:mean_OREB].to_f * divide(data[:opponent_o_team_OREB].to_f, data[:team_OREB].to_f) - data[:mean_OREB].to_f
      expected_DREB = data[:mean_DREB].to_f * divide(data[:opponent_o_team_DREB].to_f, data[:team_DREB].to_f)
      expected_DREB_effect = data[:mean_DREB].to_f * divide(data[:opponent_o_team_DREB].to_f, data[:team_DREB].to_f) - data[:mean_DREB].to_f
      #binding.pry#look at dreb team 
      expected_AST = data[:mean_AST].to_f * divide(data[:opponent_o_team_AST].to_f, data[:mean_team_AST].to_f)
      expected_AST_effect = data[:mean_AST].to_f * divide(data[:opponent_o_team_AST].to_f, data[:mean_team_AST].to_f) - data[:mean_AST].to_f
      expected_TOV = data[:mean_TOV].to_f * divide(oa_TOV, data[:mean_team_TOV].to_f)
      expected_TOV_effect = data[:mean_TOV].to_f * divide(oa_TOV, data[:mean_team_TOV].to_f) - data[:mean_TOV].to_f
      expected_BLK = data[:mean_BLK].to_f * divide(data[:opponent_o_team_BLK].to_f, data[:mean_team_BLK].to_f)
      expected_BLK_effect = data[:mean_BLK].to_f * divide(data[:opponent_o_team_BLK].to_f, data[:mean_team_BLK].to_f) - data[:mean_BLK].to_f
      expected_STL = data[:mean_STL].to_f * divide(data[:opponent_o_team_STL].to_f, data[:mean_team_STL].to_f)
      expected_STL_effect = data[:mean_STL].to_f * divide(data[:opponent_o_team_STL].to_f, data[:mean_team_STL].to_f) - data[:mean_STL].to_f

      scaled_oreb_pct = a_OREB_PCT * divide(data[:o_OREB_PCT_v_position].to_f, lpa_OREB_PCT)
      scaled_oreb_pct_effect = scaled_oreb_pct - a_OREB_PCT
      scaled_dreb_pct = a_DREB_PCT * divide(data[:o_DREB_PCT_v_position].to_f, lpa_DREB_PCT)
      scaled_dreb_pct_effect = scaled_dreb_pct - a_DREB_PCT
      scaled_assist_pct = a_AST_PCT * divide(data[:o_team_AST_PCT_v_position].to_f, lpa_AST_PCT)
      scaled_assist_pct_effect = scaled_assist_pct - data[:mean_AST_PCT].to_f
      scaled_turnover_pct = a_TO_PCT * divide(data[:o_team_TO_PCT_v_position].to_f, lpa_TO_PCT)
      scaled_turnover_pct_effect = scaled_turnover_pct - data[:mean_TO_PCT].to_f
      scaled_block_pct = a_PCT_BLK * divide(data[:o_team_PCT_BLK_v_position].to_f, lpa_PCT_BLK)
      scaled_block_pct_effect = scaled_block_pct - data[:mean_PCT_BLK].to_f
      scaled_pct_stl = a_PCT_STL * divide(data[:o_team_PCT_STL_v_position].to_f, lpa_PCT_STL)
      scaled_pct_stl_effect = scaled_pct_stl - a_PCT_STL

      scaled_oreb = data[:mean_OREB].to_f * divide(data[:o_OREB_v_position].to_f, lpa_OREB)
      scaled_oreb_effect = scaled_oreb - data[:mean_OREB].to_f
      scaled_dreb = data[:mean_DREB].to_f * divide(data[:o_DREB_v_position].to_f, lpa_DREB)
      scaled_dreb_effect = scaled_dreb - data[:mean_DREB].to_f
      scaled_assist = data[:mean_AST].to_f * divide(opa_AST, lpa_AST)
      scaled_assist_effect = scaled_assist - data[:mean_AST].to_f
      scaled_turnover = data[:mean_TOV].to_f * divide(opa_TOV, lpa_TOV)
      scaled_turnover_effect = scaled_turnover - data[:mean_TOV].to_f
      scaled_block = data[:mean_BLK].to_f * divide(opa_BLK, lpa_BLK)
      scaled_block_effect = scaled_block - data[:mean_BLK].to_f
      scaled_steal = a_STL * divide(opa_STL, lpa_STL)
      scaled_steal_effect = scaled_steal - a_STL

      modded_oreb = (data[:mean_OREB].to_f + lpa_DREB) / 2
      modded_oreb_effect = modded_oreb - data[:mean_OREB].to_f
      modded_dreb = (data[:mean_DREB].to_f + lpa_DREB) / 2
      modded_dreb_effect = modded_dreb - data[:mean_DREB].to_f
      modded_assist = (data[:mean_AST].to_f + lpa_AST) / 2
      modded_assist_effect = modded_assist - data[:mean_AST].to_f
      modded_turnover = (data[:mean_TOV].to_f + lpa_TOV) / 2
      modded_turnover_effect = modded_turnover - data[:mean_TOV].to_f
      modded_block = (data[:mean_BLK].to_f + lpa_BLK) / 2
      modded_block_effect = modded_block - data[:mean_BLK].to_f
      modded_steal = (a_STL + lpa_STL) / 2
      modded_steal_effect = modded_steal - a_STL

      #Make buckets of these
      pt_spread = team_averages[average_date][:point_spread_mean].to_f
      point_spread_abs_3_or_less = (abs(pt_spread) <= 3) ? 1 : 0
      point_spread_abs_6_or_less = (abs(pt_spread) > 3 and abs(pt_spread) <= 6) ? 1 : 0
      point_spread_abs_9_or_less = (abs(pt_spread) > 6 and abs(pt_spread) <= 9) ? 1 : 0
      point_spread_abs_12_or_less = (abs(pt_spread) > 9 and abs(pt_spread) <= 12) ? 1 : 0
      point_spread_abs_over_9 = (abs(pt_spread) > 9) ? 1 : 0
      point_spread_abs_over_12 = (abs(pt_spread) > 12) ? 1 : 0

      point_spread_3_or_less = (pt_spread <= 3) ? 1 : 0
      point_spread_6_or_less = (pt_spread > 3 and pt_spread <= 6) ? 1 : 0
      point_spread_9_or_less = (pt_spread > 6 and pt_spread <= 9) ? 1 : 0
      point_spread_12_or_less = (pt_spread > 9 and pt_spread <= 12) ? 1 : 0
      point_spread_over_9 = (pt_spread > 9) ? 1 : 0
      point_spread_over_12 = (pt_spread > 12) ? 1 : 0

      point_spread_neg_3_or_less = (pt_spread >= -3) ? 1 : 0
      point_spread_neg_6_or_less = (pt_spread < -3 and pt_spread >= -6) ? 1 : 0
      point_spread_neg_9_or_less = (pt_spread < -6 and pt_spread >= -9) ? 1 : 0
      point_spread_neg_12_or_less = (pt_spread < -9 and pt_spread >= -12) ? 1 : 0
      point_spread_neg_over_9 = (pt_spread < -9) ? 1 : 0
      point_spread_neg_over_12 = (pt_spread < -12) ? 1 : 0

      pinnacle_pt_spread = team_averages[average_date][:point_spread_Pinnacle].to_f
      point_spread_abs_3_or_less = (abs(pinnacle_pt_spread) <= 3) ? 1 : 0
      point_spread_abs_6_or_less = (abs(pinnacle_pt_spread) > 3 and abs(pinnacle_pt_spread) <= 6) ? 1 : 0
      point_spread_abs_9_or_less = (abs(pinnacle_pt_spread) > 6 and abs(pinnacle_pt_spread) <= 9) ? 1 : 0
      point_spread_abs_12_or_less = (abs(pinnacle_pt_spread) > 9 and abs(pinnacle_pt_spread) <= 12) ? 1 : 0
      point_spread_abs_over_9 = (abs(pinnacle_pt_spread) > 9) ? 1 : 0
      point_spread_abs_over_12 = (abs(pinnacle_pt_spread) > 12) ? 1 : 0

      point_spread_3_or_less = (pinnacle_pt_spread <= 3) ? 1 : 0
      point_spread_6_or_less = (pinnacle_pt_spread > 3 and pinnacle_pt_spread <= 6) ? 1 : 0
      point_spread_9_or_less = (pinnacle_pt_spread > 6 and pinnacle_pt_spread <= 9) ? 1 : 0
      point_spread_12_or_less = (pinnacle_pt_spread > 9 and pinnacle_pt_spread <= 12) ? 1 : 0
      point_spread_over_9 = (pinnacle_pt_spread > 9) ? 1 : 0
      point_spread_over_12 = (pinnacle_pt_spread > 12) ? 1 : 0

      point_spread_neg_3_or_less = (pinnacle_pt_spread >= -3) ? 1 : 0
      point_spread_neg_6_or_less = (pinnacle_pt_spread < -3 and pinnacle_pt_spread >= -6) ? 1 : 0
      point_spread_neg_9_or_less = (pinnacle_pt_spread < -6 and pinnacle_pt_spread >= -9) ? 1 : 0
      point_spread_neg_12_or_less = (pinnacle_pt_spread < -9 and pinnacle_pt_spread >= -12) ? 1 : 0
      point_spread_neg_over_9 = (pinnacle_pt_spread < -9) ? 1 : 0
      point_spread_neg_over_12 = (pinnacle_pt_spread < -12) ? 1 : 0

      team_pts_ratio_pinnacle = team_averages[average_date][:est_vegas_team_PTS_Pinnacle].to_f / team_averages[average_date][:PTS_mean].to_f
      team_pts_ratio = team_averages[average_date][:est_vegas_team_PTS].to_f / team_averages[average_date][:PTS_mean].to_f
      vegas_ratio_pts = data[:prev_mean_PTS].to_f * team_pts_ratio
      vegas_ratio_pts_pinnacle = data[:prev_mean_PTS].to_f * team_pts_ratio_pinnacle
      vegas_ratio_pts_effect = vegas_ratio_pts - data[:prev_mean_PTS]
      vegas_ratio_pts_pinnacle_effect = vegas_ratio_pts_pinnacle - data[:prev_mean_PTS]

      #buckets for minutes
      avg_mins = data[:mean_seconds].to_f / 60
      avg_mins_10_or_less = (avg_mins <= 10) ? 1 : 0
      avg_mins_20_or_less = (avg_mins > 10 and avg_mins <= 20) ? 1 : 0
      avg_mins_30_or_less = (avg_mins > 20 and avg_mins <= 30) ? 1 : 0
      avg_mins_over_30 = (avg_mins > 30) ? 1 : 0

      merged_rating = (data[:OFF_RTG].to_f + data[:o_DEF_RTG].to_f) / 2

      

      #jlk todo do something with this
      opp_pts_ratio = team_averages[average_date][:est_vegas_opp_PTS].to_f / team_averages[average_date][:o_PTS_mean].to_f
      opp_pts_ratio_pinnacle = team_averages[average_date][:est_vegas_opp_PTS_Pinnacle].to_f / team_averages[average_date][:o_PTS_mean].to_f

      #average_team pts
      #average opp pts
      #% jump in team pts
      #% jump in opp pts
      #abs jump in team pts
      #abs jump in opp pts
      #abs team pts
      #abs opp pts
      #factor in odds for vegas lines later
      #change def_rtg_v_pos to include neighbor positions
      
      features_seconds = [ data[:mean_seconds].to_f, data[:b2b], data[:extra_rest], location, rest_effect_seconds, location_effect_seconds, prev_seconds, prev2_seconds, prev5_seconds, data[:starter], mean_starter_seconds, mean_bench_seconds, mean_starterbench_seconds, data[:actual_SECONDS].to_f ].to_csv
      features_seconds_delta = [ data[:mean_seconds].to_f, data[:b2b], data[:extra_rest], location, rest_effect_seconds, location_effect_seconds, prev_seconds_delta, prev2_seconds_delta, prev5_seconds_delta, data[:actual_SECONDS].to_f ].to_csv
      #features = [ data[:prev_mean_PTS], data[:expected_pts_pace].to_f, location_pts, rest_pts, opp_rest_pts, expected_PTS_def_rtg_v_position, fb_effect, pts_paint_effect, pts_2ndchance_effect, pts_off_tov_effect, cfg_pts, data[:actual_PTS] ].to_csv
      #features_per_min = [ mean_pts_per_min, 60*data[:expected_pts_pace].to_f / data[:mean_seconds].to_f, location_pts_per_min, rest_pts_per_min, opp_rest_pts_per_min, expected_PTS_def_rtg_v_position, fb_effect_per_min, pts_paint_effect_per_min, pts_2ndchance_effect_per_min, pts_off_tov_effect_per_min, cfg_pts_per_min, actual_pts_per_min, actual_mins ].to_csv

      #p "starters.#{mean_starter_pts} #{mean_bench_pts} #{starterbench_pts_effect} #{mean_starterbench_pts} #{mean_starterbench_pts}"
      features_points = [ data[:prev_mean_PTS], def_rtg_delta, def_rtg_v_position_delta, o_pts_delta, data[:b2b], data[:opp_b2b], data[:extra_rest], data[:opp_extra_rest], location, location_effect, rest_effect, pace_effect, pace_effect2, pace_effect3, pts_paint_effect,pts_off_tov_effect, fb_effect, pts_2ndchance_effect, data[:USG_PCT]*100, data[:USG_PCT_minus_TOV]*100, location_pts, rest_pts, opp_rest_pts, expected_pts_pace, pts_pace_effect, expected_pts_pace2, pts_pace2_effect, expected_pts_pace3, pts_pace3_effect, expected_PTS_def_rtg, def_rtg_effect, expected_PTS_def_rtg_v_position, mean_starter_pts, mean_bench_pts, starterbench_pts_effect, data[:starter], mean_starterbench_pts, prev_points, prev2_points, prev5_points, ft_effect, expected_FTM, vegas_ratio_pts, vegas_ratio_pts_effect, vegas_ratio_pts_pinnacle, vegas_ratio_pts_pinnacle_effect, point_spread_abs_3_or_less, point_spread_abs_6_or_less, point_spread_abs_9_or_less, point_spread_abs_12_or_less, point_spread_abs_over_9, point_spread_abs_over_12, point_spread_3_or_less, point_spread_6_or_less, point_spread_9_or_less, point_spread_12_or_less, point_spread_over_9, point_spread_over_12, point_spread_neg_3_or_less, point_spread_neg_6_or_less, point_spread_neg_9_or_less, point_spread_neg_12_or_less, point_spread_neg_over_9, point_spread_neg_over_12, data[:mean_seconds].to_f/60, mean_starter_seconds/60, mean_bench_seconds/60, mean_starterbench_seconds/60, prev_seconds/60, prev2_seconds/60, prev5_seconds/60, data[:actual_PTS] ].to_csv
      features_points_per_min = [ mean_pts_per_min, def_rtg_delta, def_rtg_v_position_delta, o_pts_delta_per_min, data[:b2b], data[:opp_b2b], data[:extra_rest], data[:opp_extra_rest], location, location_effect_per_min, rest_effect_per_min, pace_effect_per_min,pts_paint_effect_per_min,pts_off_tov_effect_per_min, fb_effect_per_min, pts_2ndchance_effect_per_min, expected_pts_pace_per_min, pts_pace_effect_per_min, expected_pts_pace2_per_min, pts_pace2_effect_per_min, expected_pts_pace3_per_min, pts_pace3_effect_per_min, def_rtg_effect_per_min, def_rtg_effect_per_min, expected_PTS_def_rtg_v_position_per_min, def_rtg_v_position_effect_per_min, expected_PTS_def_rtg_v_position_per_min, mean_starter_pts_per_min, mean_bench_pts_per_min, starterbench_pts_effect_per_min, prev_points_per_min, prev2_points_per_min, prev5_points_per_min, ft_effect_per_min, expected_FTM_per_min, vegas_ratio_pts_per_min, vegas_ratio_pts_effect_per_min, vegas_ratio_pts_pinnacle_per_min, vegas_ratio_pts_pinnacle_effect_per_min, point_spread_abs_3_or_less, point_spread_abs_6_or_less, point_spread_abs_9_or_less, point_spread_abs_12_or_less, point_spread_abs_over_9, point_spread_abs_over_12, point_spread_3_or_less, point_spread_6_or_less, point_spread_9_or_less, point_spread_12_or_less, point_spread_over_9, point_spread_over_12, point_spread_neg_3_or_less, point_spread_neg_6_or_less, point_spread_neg_9_or_less, point_spread_neg_12_or_less, point_spread_neg_over_9, point_spread_neg_over_12, data[:mean_seconds].to_f, data[:b2b], data[:extra_rest], location, rest_effect_seconds, location_effect_seconds, prev_seconds, prev2_seconds, prev5_seconds, data[:starter], mean_starter_seconds, mean_bench_seconds, mean_starterbench_seconds, actual_pts_per_min, actual_mins, data[:actual_SECONDS].to_f ].to_csv
      #team oreb_pct
      #mean_starter_oreb,mean_bench_oreb,mean_starterbench_oreb,prev_orebs,prev2_orebs,prev5_orebs,rest_orebs,rest_oreb_effect,opp_rest_orebs,opp_rest_oreb_effect,location_orebs,location_oreb_effect,expected_OREB,expected_OREB_effect,scaled_oreb_pct,scaled_oreb_pct_effect,scaled_oreb,scaled_oreb_pct_effect,modded_oreb,modded_oreb_effect
      features_orebs = [ data[:mean_OREB].to_f, a_OREB_PCT,oa_DREB_PCT, oa_DREB,o_oreb_delta, data[:b2b],data[:opp_b2b],data[:extra_rest],data[:opp_extra_rest],location,location_oreb_effect,rest_oreb_effect,location_orebs,rest_orebs,opp_rest_orebs,data[:starter],mean_starter_oreb,mean_bench_oreb,starterbench_oreb_effect,mean_starterbench_oreb,prev_orebs,prev2_orebs,prev5_orebs,o_oreb_pct_delta,expected_OREB_effect,scaled_oreb_pct_effect, scaled_oreb_effect, modded_oreb_effect,opa_OREB,opa_OREB_PCT,oa_OREB_PCT,data[:mean_seconds].to_f/60, mean_starter_seconds/60, mean_bench_seconds/60, mean_starterbench_seconds/60, prev_seconds/60, prev2_seconds/60, prev5_seconds/60, team_misses,team_3p_misses,team_2p_misses,team_ft_misses,team_FT_PCT,team_FG_PCT,team_FG3_PCT,team_FG2_PCT,data[:actual_OREB].to_f].to_csv
      features_drebs = [ data[:mean_DREB].to_f, a_DREB_PCT,oa_OREB_PCT,oa_OREB,o_dreb_delta, data[:b2b],data[:opp_b2b],data[:extra_rest],data[:opp_extra_rest],location,location_dreb_effect,rest_dreb_effect,location_drebs,rest_drebs,opp_rest_drebs,data[:starter],mean_starter_dreb,mean_bench_dreb,starterbench_dreb_effect,mean_starterbench_dreb,prev_drebs,prev2_drebs,prev5_drebs,o_dreb_pct_delta,expected_DREB_effect,scaled_dreb_pct_effect, scaled_dreb_effect,modded_dreb_effect,opa_DREB,opa_DREB_PCT,oa_DREB_PCT,data[:mean_seconds].to_f/60, mean_starter_seconds/60, mean_bench_seconds/60, mean_starterbench_seconds/60, prev_seconds/60, prev2_seconds/60, prev5_seconds/60, oa_misses,oa_3p_misses,oa_2p_misses,oa_ft_misses,oa_FT_PCT,oa_FG_PCT,oa_FG3_PCT,oa_FG2_PCT,data[:actual_DREB].to_f].to_csv
      features_assists = [ data[:mean_AST].to_f, a_AST_PCT,data[:o_mean_AST_PCT].to_f,o_ast_delta, data[:b2b],data[:opp_b2b],data[:extra_rest],data[:opp_extra_rest],location,location_assist_effect,rest_assist_effect,location_assists,rest_assists,opp_rest_assists,data[:starter],mean_starter_ast,mean_bench_ast,starterbench_ast_effect,mean_starterbench_ast,prev_assists,prev2_assists,prev5_assists,o_ast_pct_delta,expected_AST_effect,scaled_assist_pct_effect, scaled_assist_effect,modded_assist_effect,data[:actual_AST].to_f].to_csv
      features_turnovers = [ data[:mean_TOV].to_f, a_TO_PCT,data[:o_mean_TO_PCT].to_f,o_tov_delta, data[:b2b],data[:opp_b2b],data[:extra_rest],data[:opp_extra_rest],location,location_turnover_effect,rest_turnover_effect,location_turnovers,rest_turnovers,opp_rest_turnovers,data[:starter],mean_starter_tov,mean_bench_tov,starterbench_tov_effect,mean_starterbench_tov,prev_turnovers,prev2_turnovers,prev5_turnovers,o_tov_pct_delta,expected_TOV_effect,scaled_turnover_pct_effect, scaled_turnover_effect,modded_turnover_effect,data[:actual_TOV].to_f].to_csv
      features_blocks = [ data[:mean_BLK].to_f, a_PCT_BLK,data[:o_mean_PCT_BLK].to_f,o_blk_delta, data[:b2b],data[:opp_b2b],data[:extra_rest],data[:opp_extra_rest],location,location_block_effect,rest_block_effect,location_blocks,rest_blocks,opp_rest_blocks,data[:starter],mean_starter_blk,mean_bench_blk,starterbench_blk_effect,mean_starterbench_blk,prev_blocks,prev2_blocks,prev5_blocks,o_blk_pct_delta,expected_BLK_effect,scaled_block_pct_effect, scaled_block_effect,modded_block_effect,data[:actual_BLK].to_f].to_csv
      features_steals = [ data[:mean_STL].to_f, a_PCT_STL,data[:o_mean_PCT_STL].to_f,o_stl_delta, data[:b2b],data[:opp_b2b],data[:extra_rest],data[:opp_extra_rest],location,location_steal_effect,rest_steal_effect,location_steals,rest_steals,opp_rest_steals,data[:starter],mean_starter_stl,mean_bench_stl,starterbench_stl_effect,mean_starterbench_stl,prev_steals,prev2_steals,prev5_steals,o_stl_pct_delta,expected_STL_effect,scaled_steal_effect,scaled_pct_stl_effect,modded_steal_effect,data[:actual_STL].to_f].to_csv
      #expected_OREB, data[:e_o_DREB_PCT].to_f, data[:b2b].to_f, data[:mean_b2b_OREB].to_f, data[:mean_b2b_OREB_PCT].to_f, data[:mean_extra_rest_OREB].to_f, data[:mean_extra_rest_OREB_PCT].to_f, data[:mean_opp_b2b

      File.open("points_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_points ) }
      File.open("orebs_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_orebs ) }
      File.open("drebs_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_drebs ) }
      File.open("steals_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_steals ) }
      File.open("assists_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_assists) }
      File.open("blocks_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_blocks) }
      File.open("turnovers_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_turnovers) }
      File.open("points_per_min_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_points_per_min ) }
      File.open("seconds_#{season.gsub("-","_")}.csv", "a") { |f| f.write( features_seconds ) }
    }
  }
end

seasons_h.each{|season|
  if OPTIONS[:fanduelsalaryfile]
    filename = OPTIONS[:fanduelsalaryfile]
    addDateToFanDuelSalaryFile( filename )
    populateData_worker( database, "fanduel_regularseason_daily", filename )
  end

  today = Date.today
    if OPTIONS[:startyesterday]
      season[1][0] = (today - 1).to_s
    end

    if today >= Date.parse( season[1][0]) and today < Date.parse( season[1][1] )
      season[1][1] = today.to_s

      if OPTIONS[:includetodaygames]
        season[1][1] = (today + 1).to_s
        season[1][0] = today.to_s
      end
    end

    #parseSeason( season[0], database )
    #binding.pry
#=begin 
  #tables = tables[6..7]
  #binding.pry
    #populate just parses .csv and doesn't download
    #populateAllData( seasons_h.keys, seasontypes, tables, database, true )
  
  ####this is for dynamic
    ##get_box: put in manual dates
    ##populatealldata, change bRecreate to false
    ##populateAllData( seasons_h.keys, seasontypes, tables, database, false )
    ##calculatedailyaverages: write fns to reload all the splitSets from the db.  need to load all the totals, and the number of games for each splitset
    ##calculateteamoppo: write fns to reload all the splitSets from the db.  need to load all the totals, and the number of games for each splitset
    ##calculatedailyteamav: just pass in the date to start from so we don't overwrite, everything else can remain same
    ##calxyvalues can keep same for now, but shoudl rewrite it and unit test
  
    #getGamelogs( database, seasons_h )#jlk - have to handle edge case when some games are not completed yet
    #populateGamelogs( database, seasons_h )#if we already have the gamelog .csv but not in DB
    
    #fillRosters( seasons_h.keys, database )
    #fillBBallReferenceBioinfo( seasons_h.keys, database )
    #fillNBABioStats( seasons_h.keys, database )
    
    if season[0].split("-")[0].to_i > 2013
      if season[0].split("-")[0].to_i < 2016
        #getFanduelSalaries( seasons_h )
        #getFanduelSalaries2016( seasons_h )
      else
        #binding.pry
        #getFanduelSalaries2016( seasons_h )
        #getFanduelSalaries2016( seasons_h, start_day(string) )
      end
      #populateFanDuelInfo( seasons_h.keys, database )
      #fillFanDuelInfo( seasons_h.keys, database )
    end
#=end
=begin
    dir = FileUtils::mkdir_p season + "/regularseason"
    day = season[1][0]
    regseason_end = season[1][1]
    getBettingLines( season, "regularseason", dir, day, regseason_end, true )
    addNBAGameIDToBettingLineTable( season, "regularseason" )
    
    playoffs_end = season[1][2]
    dir = FileUtils::mkdir_p season + "/playoffs"
    getBettingLines( season, "playoffs", dir, regseason_end, playoffs_end, true )
    addNBAGameIDToBettingLineTable( season, "playoffs" )
    #parseSeason( season, dir, regseason_end, playoffs_end )
    #addDatesToBoxscores( season, dir, regseason_end, playoffs_end )
=end
    #binding.pry
    season = season[0]
    #Do this for teams
    bCalcPlayers = false
    #calculateDailyAverages( seasons_h, season, seasontypes[0], database, bCalcPlayers )
    #calculateDailyAverages( seasons_h, season, seasontypes[1], database, bCalcPlayers )

    #binding.pry
    #Do this for players
    bCalcPlayers = true
    calculateDailyAverages( seasons_h, season, seasontypes[0], database, bCalcPlayers )
    #calculateDailyAverages( seasons_h, season, seasontypes[1], database, bCalcPlayers )
    database = syncServers( database, season, 0 )

    calculateTeamOpponentStats( database, season, seasontypes[0], team_tables, seasons_h[ season ][ 1 ] )
    #calculateTeamOpponentStats( database, season, seasontypes[1], team_tables, seasons_h[ season ][ 2 ] )
    database = syncServers( database, season, 1 )

    calculateDailyTeamAverages( database, season, seasontypes[0], seasons_h[season][0], seasons_h[season][1] )
    #calculateDailyTeamAverages( database, season, seasontypes[1], seasons_h[season][1], seasons_h[season][2] )

    calculateXYvalues( database, seasons_h, season, seasontypes[0] )
    #calculateXYvalues( database, seasons_h, season, seasontypes[1] )
    
    database = syncServers( database, season, 2 )

    #outputPointsCsv( database, season, seasontypes[0], seasons_h.values[0][0] )

    #exploreSecondsPlayed( database, season, seasontypes[0], seasons_h.values[0][0] )

    p "done season: #{season}"
}
end

launchSqliteConsole() if OPTIONS[:sqlite_console] 
launchConsole(database) if OPTIONS[:irb_console] || ! OPTIONS[:output]

__END__
