#!/usr/bin/env ruby
require 'rubygems'
require 'pry'
require 'csv'
require 'sequel'
require 'sqlite3'
require 'optimist'
require 'date'
require 'nokogiri'
require 'open-uri'
require 'json'
require 'net/http'

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
  #populateAllData() just parses .csv and doesn't download
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

  #fixVegasTable( seasons_h, season, seasontypes[0], database, bCalcPlayers )

  calculateXYvalues( database, seasons_h, season, seasontypes[0] )
  #calculateXYvalues( database, seasons_h, season, seasontypes[1] )

  database = syncServers( database, season, 2 )

  outputPointsCsv( database, season, seasontypes[0], seasons_h.values[0][0] )

  #exploreSecondsPlayed( database, season, seasontypes[0], seasons_h.values[0][0] )

  p "done season: #{season}"
}
