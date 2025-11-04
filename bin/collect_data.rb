
def getEndIndex( thread_index, num_threads, arr_size )
  end_index = arr_size

  if thread_index == (num_threads - 1)
    end_index = arr_size
  else
    quotient = arr_size / num_threads
    remainder = arr_size % num_threads
    big_chunk_size = quotient + 1
    small_chunk_size = quotient

    thread_index = thread_index + 1

    num_big_chunks = (thread_index - remainder) > 0 ? remainder : thread_index
    num_small_chunks = (thread_index - remainder ) > 0 ? (thread_index - remainder) : 0
    end_index = num_big_chunks * big_chunk_size + num_small_chunks * small_chunk_size
  end

  return end_index
end

def getPlayerGamelogs2( database, seasons_h, thread_index = nil, num_threads = nil )
  seasontypes = [ ["regularseason", "Regular+Season"], ["playoffs", "Playoffs"] ]
  season = seasons_h.keys[0]
  player_tables = ["traditional_PlayerStats"]
  if database.table_exists? :"#{season}_regularseason_advanced_PlayerStats"
    player_tables = player_tables + "advanced_PlayerStats"
  end
  if database.table_exists? :"#{season}_regularseason_fourfactors_sqlPlayersFourFactors"
    player_tables = player_tables + "fourfactors_sqlPlayersFourFactors"
  end
  if database.table_exists? :"#{season}_regularseason_misc_sqlPlayersMisc"
    player_tables = player_tables + "misc_sqlPlayersMisc"
  end
  if database.table_exists? :"#{season}_regularseason_playertrack_PlayerTrack"
    player_tables = player_tables + "arseason_playertrack_PlayerTrack"
  end
  if database.table_exists? :"#{season}_regularseason_scoring_sqlPlayersScoring"
    player_tables = player_tables + "scoring_sqlPlayersScoring"
  end
  if database.table_exists? :"#{season}_regularseason_usage_sqlPlayersUsage"
    player_tables = player_tables + "usage_sqlPlayersUsage"
  end

  seasons_h.keys.each{|season|
    date_index = 0
    seasontypes.each{|seasontype|
      tablename = season + " " + seasontype[0] + " player gamelogs"
      tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

      players = database[ :"#{season.gsub(/-/,"_")}_#{seasontype[0]}_traditional_PlayerStats" ].select(:PLAYER_ID,:PLAYER_NAME).distinct.entries

      if thread_index and num_threads and thread_index >= 0 and num_threads > 0
        start_index = (0 == THREAD_INDEX) ? 0 :  getEndIndex( THREAD_INDEX - 1, NUM_THREADS, players.size )
        end_index = getEndIndex( THREAD_INDEX, NUM_THREADS, players.size )

        players = players[ start_index...end_index ]
      end 

      dir = FileUtils::mkdir_p season + "/" + seasontype[0] + "/playergamelogs/"

      players.each_with_index{|player,i|

        player_boxscores = database[ :"#{season.gsub(/-/,"_")}_#{seasontype[0]}_traditional_PlayerStats" ].where(:PLAYER_ID => player[:PLAYER_ID]).exclude(:TEAM_ABBREVIATION => "EST").exclude( :TEAM_ABBREVIATION => "WST").order(:GAME_ID).entries

        lastGameDate = Date.parse( seasons_h.values.first[date_index] ) - 1 #rewind to 1 day before start of season

        if Dir["#{season}/playoffs/playergamelogs/#{player[:PLAYER_ID]}*.csv"].length > 0
          arr = Dir["#{season}/playoffs/playergamelogs/#{player[:PLAYER_ID]}*.csv"].sort_by{|str| str.split("_")[1]}
          lastGameDate = Date.parse( arr.last.split("_")[1].split(".csv")[0] )
        else
          if Dir["#{season}/regularseason/playergamelogs/#{player[:PLAYER_ID]}*.csv"].length > 0
            arr = Dir["#{season}/regularseason/playergamelogs/#{player[:PLAYER_ID]}*.csv"].sort_by{|str| str.split("_")[1]}
            lastGameDate = Date.parse( arr.last.split("_")[1].split(".csv")[0] )
          end
        end

        player_boxscores.each_with_index{|box,box_index|
          date = Date.parse( box[:DATE] )

          if date and box[:MIN] and (date > lastGameDate) #skip this boxscore otherwise
            begin
              #url = "http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID=#{player[:PLAYER_ID]}&Season=#{season}&SeasonType=#{seasontype[1]}"
              date_str = "#{date.month}%2F#{date.day}%2F#{date.year}"
              url = "http://stats.nba.com/stats/playergamelog?DateFrom=#{date_str}&DateTo=#{date_str}&LeagueID=00&PlayerID=#{player[:PLAYER_ID]}&Season=#{season}&SeasonType=#{seasontype[1]}"
              p "#{i} / #{players.size} #{date_str}"
              begin
                doc = `curl '#{url}' -H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/game/' -H 'Connection: keep-alive' --compressed`

                json = JSON.parse( doc )
              rescue StandardError => e
                binding.pry
                p e
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
                  column "PLAYER_NAME", String
                end
              end

              json["resultSets"].each{|resultSet|
                csv = resultSet["headers"].to_csv
                resultSet["rowSet"].each{|row|
                  csv = csv + row.to_csv

                  File.open( season + "/" + seasontype[0] + "/playergamelogs" + "/" + player[:PLAYER_ID] + "_" + "#{date.strftime("%Y-%m-%d")}" + ".csv", "w" ){|f|
                    f.write csv
                  }

                  str = season + "/" + seasontype[0] + "/playergamelogs" + "/" + player[:PLAYER_ID] + "_" + "#{date.strftime("%Y-%m-%d")}" + ".csv"
                  p str

                  h = Hash.new
                  row.each_with_index{|item,i|
                    if 3 == i
                      date = Date.parse item
                      h[ json["resultSets"][0]["headers"][i] ] = date.to_s
                    else
                      h[ json["resultSets"][0]["headers"][i] ] = item
                    end

                    h["PLAYER_NAME"] = player[:PLAYER_NAME]
                    #h["PLAYER_ID"] = player[:PLAYER_ID]
                    #h["TEAM_ID"] = player[:TEAM_ID]
                    #h["TEAM_ABBREVIATION"] = player[:TEAM_ABBREVIATION]
                    #h["TEAM_CITY"] = player[:TEAM_CITY]

                  }
                  database[tablename].insert( h )
                }
              }
            rescue StandardError => e
              binding.pry
              p e
            end
          end
        }
      }
    }
  }
end

def getPlayerGamelogs( database, seasons_h )
  #seasontypes = [ ["regularseason", "Regular+Season"], ["playoffs", "Playoffs"] ]
  seasontypes = [ ["regularseason", "Regular+Season"] ]
  player_tables = [ "advanced_PlayerStats", "fourfactors_sqlPlayersFourFactors", "misc_sqlPlayersMisc", "playertrack_PlayerTrack", "scoring_sqlPlayersScoring", "traditional_PlayerStats", "usage_sqlPlayersUsage" ]

  seasons_h.keys.each{|season|
    date_index = 0
    seasontypes.each{|seasontype|
      tablename = season + " " + seasontype[0] + " player gamelogs"
      tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

      players = database[ :"#{season.gsub(/-/,"_")}_#{seasontype[0]}_#{player_tables[5]}" ].select(:PLAYER_ID,:PLAYER_NAME).distinct.entries

      dir = FileUtils::mkdir_p season + "/" + seasontype[0] + "/playergamelogs/"

      players.each_with_index{|player,i|

        lastGameDate = nil

        if Dir["#{season}/playoffs/playergamelogs/#{player[:PLAYER_ID]}*.csv"].length > 0
          arr = Dir["#{season}/playoffs/playergamelogs/#{player[:PLAYER_ID]}*.csv"].sort_by{|str| str.split("_")[1]}
          lastGameDate = arr.last.split("_")[1].split(".csv")[0]
        else
          if Dir["#{season}/regularseason/playergamelogs/#{player[:PLAYER_ID]}*.csv"].length > 0
            arr = Dir["#{season}/regularseason/playergamelogs/#{player[:PLAYER_ID]}*.csv"].sort_by{|str| str.split("_")[1]}
            lastGameDate = arr.last.split("_")[1].split(".csv")[0]
          end
        end

        if nil == lastGameDate
          date = Date.parse( seasons_h.values.first[date_index] )
        else
          date = Date.parse( lastGameDate ) + 1
        end
        end_date = Date.parse( seasons_h.values.first[date_index + 1] )

        while date < end_date
          begin
            #url = "http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID=#{player[:PLAYER_ID]}&Season=#{season}&SeasonType=#{seasontype[1]}"
            date_str = "#{date.month}%2F#{date.day}%2F#{date.year}"
            url = "http://stats.nba.com/stats/playergamelog?DateFrom=#{date_str}&DateTo=#{date_str}&LeagueID=00&PlayerID=#{player[:PLAYER_ID]}&Season=#{season}&SeasonType=#{seasontype[1]}"
            p "#{i} / #{players.size} #{date_str}"
            begin
              doc = `curl '#{url}' -H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/game/' -H 'Connection: keep-alive' --compressed`

              json = JSON.parse( doc )
            rescue StandardError => e
              binding.pry
              p e
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
                column "PLAYER_NAME", String
              end
            end
            
            json["resultSets"].each{|resultSet|
              csv = resultSet["headers"].to_csv
              resultSet["rowSet"].each{|row|
                csv = csv + row.to_csv

                File.open( season + "/" + seasontype[0] + "/playergamelogs" + "/" + player[:PLAYER_ID] + "_" + "#{date.strftime("%Y-%m-%d")}" + ".csv", "w" ){|f|
                  f.write csv
                }

                str = season + "/" + seasontype[0] + "/playergamelogs" + "/" + player[:PLAYER_ID] + "_" + "#{date.strftime("%Y-%m-%d")}" + ".csv"
                p str


                h = Hash.new
                row.each_with_index{|item,i|
                  h[ json["resultSets"][0]["headers"][i] ] = item

                  h["PLAYER_NAME"] = player[:PLAYER_NAME]
                  #h["PLAYER_ID"] = player[:PLAYER_ID]
                  #h["TEAM_ID"] = player[:TEAM_ID]
                  #h["TEAM_ABBREVIATION"] = player[:TEAM_ABBREVIATION]
                  #h["TEAM_CITY"] = player[:TEAM_CITY]
                }
                database[tablename].insert( h )
              }
            }

          rescue StandardError => e
            binding.pry
            p e
          end

          date = date + 1
        end
      }
    }
  }
end
def populatePlayerGamelogs( database, seasons_h )
  seasontypes = [ ["regularseason", "Regular+Season"], ["playoffs", "Playoffs"] ]

  seasons_h.keys.each{|season|
    seasontypes.each{|seasontype|
      Dir.glob( season + "/" + seasontype[0] + "/playergamelogs/*.csv").each_with_index{|filename,i|
      tablename = season + " " + seasontype[0] + " player gamelogs"
      #tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

        #only recreate for 1st file, even w/ bRecreate flag
=begin
        if (true == bRecreate) and (0 == i)
          bRecreate = true
        else
          bRecreate = false
        end

        populateData_worker( database, tablename, filename, bRecreate )
=end
        populateData_worker( database, tablename, filename )
      }
    }
  }
end

#measure_types = [ "Base", "Advanced", "Misc", "Four%20Factors", "Scoring", "Opponent", "Usage" ]
#measure_types1 = [ "Base", "Advanced", "Misc", "Scoring", "Usage" ]
=begin
measure_types1 = [ "Advanced" ]
measure_types2 = [ "Base" ] 
#splits = [ ["generalsplits", measure_types1], ["opponent", measure_types1], ["lastngames", measure_types1], ["gamesplits", measure_types1], ["shootingsplits", measure_types2] ]
if stat_type and ( "tracking" == stat_type )
  if "player" == entity
    splits = [ ["shots", ["Base"]], ["reb", ["Base"]], ["pass", ["Base"]], ["shotdefend", ["Base"]] ]
  elsif "team" == entity
    splits = [ ["shots", ["Base"]], ["reb", ["Base"]], ["pass", ["Base"]] ]
  else
    binding.pry
    p "error"
  end
else
  splits = [ ["generalsplits", measure_types1], ["opponent", measure_types1], ["shootingsplits", measure_types2] ]
end

#url = "http://stats.nba.com/stats/playerdashboardby#{split}?DateFrom=#{season_start.month}/#{season_start.day}/#{season_start.year}&DateTo=#{cur_date.month}/#{cur_date.day}/#{cur_date.year}&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=#{measure_type}&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerID=#{player_id}&PlusMinus=N&Rank=N&Season=#{season}&SeasonSegment=&SeasonType=#{season_type}&ShotClockRange=&VsConference=&VsDivision="

#=begin

# Set a finite number of simultaneous worker threads that can run

thread_count = NUM_THREADS
p "thread_count: #{thread_count}"

threads = Array.new(thread_count)

# Create a work queue for the producer to give work to the consumer
work_queue = SizedQueue.new(thread_count)

# Add a monitor so we can notify when a thread finishes and we can schedule a new one
threads.extend(MonitorMixin)

# Add a condition variable on the monitored array to tell the consumer to check the thread array
threads_available = threads.new_cond

# Add a variable to tell the consumer that we are done producing work
sysexit = false

#http://stats.nba.com/stats/playerdashptshots?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=PerGame&Period=0&PlayerID=202699&Season=2015-16&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=
#http://stats.nba.com/stats/playerdashptreb?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=PerGame&Period=0&PlayerID=202699&Season=2015-16&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=
#http://stats.nba.com/stats/playerdashptpass?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=PerGame&Period=0&PlayerID=202699&Season=2015-16&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=
#http://stats.nba.com/stats/playerdashptshotdefend?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=PerGame&Period=0&PlayerID=202699&Season=2015-16&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=


#http://stats.nba.com/stats/teamdashptshots?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=PerGame&Period=0&PlusMinus=N&Rank=N&Season=2015-16&SeasonSegment=&SeasonType=Regular+Season&TeamID=1610612737&VsConference=&VsDivision=
#http://stats.nba.com/stats/teamdashptreb?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=PerGame&Period=0&PlusMinus=N&Rank=N&Season=2015-16&SeasonSegment=&SeasonType=Regular+Season&TeamID=1610612737&VsConference=&VsDivision=
#http://stats.nba.com/stats/teamdashptpass?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=PerGame&Period=0&PlusMinus=N&Rank=N&Season=2015-16&SeasonSegment=&SeasonType=Regular+Season&TeamID=1610612737&VsConference=&VsDivision=

def downloadTrackingData( task_parameters )
  split_name = task_parameters[0]
  season_start = task_parameters[1]
  day = task_parameters[2]
  measure_type = task_parameters[3]
  team = player = task_parameters[4]
  season = task_parameters[5]
  season_type = task_parameters[6]
  entity = task_parameters[7]

  csv = ""

  if "player" == entity
    url = "http://stats.nba.com/stats/playerdashpt#{split_name}?DateFrom=#{season_start.month}/#{season_start.day}/#{season_start.year}&DateTo=#{day.month}/#{day.day}/#{day.year}&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=PerGame&Period=0&PlayerID=#{player[:PLAYER_ID]}&Season=#{season}&SeasonSegment=&SeasonType=#{season_type[0]}&TeamID=0&VsConference=&VsDivision="
    curl_str = "'#{url}' -H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/player/' -H 'Cookie: ug=56a3f9430a9b8c0a3c6b801cc102b680; ugs=1; _gat=1; _ga=GA1.2.154703133.1453595386; s_cc=true; s_fid=5E33AEE20319BA02-2185E5CD6C4816E7; s_sq=%5B%5BB%5D%5D; s_vi=[CS]v1|2B51FC9E05195A61-6000060920000119[CE]' -H 'Connection: keep-alive' -H 'Cache-Control: max-age=0' --compressed"


  elsif "team" == entity
    url = "http://stats.nba.com/stats/teamdashpt#{split_name}?DateFrom=#{season_start.month}/#{season_start.day}/#{season_start.year}&DateTo=#{day.month}/#{day.day}/#{day.year}&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=PerGame&Period=0&PlusMinus=N&Rank=N&Season=#{season}&SeasonSegment=&SeasonType=#{season_type[0]}&TeamID=#{team[:TEAM_ID]}&VsConference=&VsDivision="

    curl_str = "'#{url}' -H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/team/' -H 'Cookie: ug=56a3f9430a9b8c0a3c6b801cc102b680; ugs=1; _ga=GA1.2.154703133.1453595386; _gat=1; s_cc=true; s_fid=5E33AEE20319BA02-2185E5CD6C4816E7; s_sq=%5B%5BB%5D%5D; s_vi=[CS]v1|2B51FC9E05195A61-6000060920000119[CE]' -H 'Connection: keep-alive' -H 'Cache-Control: max-age=0' --compressed"
  else
    binding.pry
    p "need an entity name"
  end

  begin
    for i in 0...5
      begin
  #      doc = Nokogiri::HTML( open( url ) )
        resp = `curl -s #{curl_str}`
        binding.pry
        json = JSON.parse( resp )
        if json
          break
        end
      rescue StandardError => e
        p "error..trying again. #{url}"
      end
    end
  rescue StandardError => e
    binding.pry
    p "error"
  end

  if "player" == entity
    json["resultSets"].each{|resultSet|
      rows = ""
      resultSet["rowSet"].each{|row|
        rows = rows + row.to_csv
      }
      File.open( season + "/" + season_type[1] + "/teamtrackingstats/#{player[:PLAYER_ID]}_#{day.year}#{day.month}#{day.day}_#{split_name}_#{measure_type}_#{resultSet["name"]}.csv", "w" ){|f|
        f.write resultSet["headers"].to_csv + rows
      }
    }

  elsif "team" == entity
    begin
      json["resultSets"].each{|resultSet|
        rows = ""
        resultSet["rowSet"].each{|row|
          rows = rows + row.to_csv
        }
        File.open( season + "/" + season_type[1] + "/teamtrackingstats/#{team[:TEAM_ID]}_#{day.year}#{day.month}#{day.day}_#{split_name}_#{measure_type}_#{resultSet["name"]}.csv", "w" ){|f|
          f.write resultSet["headers"].to_csv + rows
        }
      }
    rescue StandardError => e
      binding.pry
      p "hi"
    end
  end
end
def downloadNbaData( task_parameters )
  split_name = task_parameters[0]
  season_start = task_parameters[1]
  day = task_parameters[2]
  measure_type = task_parameters[3]
  team = player = task_parameters[4]
  season = task_parameters[5]
  season_type = task_parameters[6]
  entity = task_parameters[7]

  csv = ""

  if "player" == entity
    url = "http://stats.nba.com/stats/playerdashboardby#{split_name}?DateFrom=#{season_start.month}/#{season_start.day}/#{season_start.year}&DateTo=#{day.month}/#{day.day}/#{day.year}&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=#{measure_type}&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerID=#{player[:PLAYER_ID]}&PlusMinus=N&Rank=N&Season=#{season}&SeasonSegment=&SeasonType=#{season_type[0]}&ShotClockRange=&VsConference=&VsDivision="
    curl_str = "'#{url}' -H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/player/' -H 'Cookie: ug=56a3f9430a9b8c0a3c6b801cc102b680; ugs=1; _gat=1; s_vi=[CS]v1|2B51FC9E05195A61-6000060920000119[CE]; _ga=GA1.2.154703133.1453595386; s_cc=true; s_fid=5E33AEE20319BA02-2185E5CD6C4816E7; s_sq=%5B%5BB%5D%5D' -H 'Connection: keep-alive' -H 'Cache-Control: max-age=0' --compressed"

  elsif "team" == entity
    url = "http://stats.nba.com/stats/teamdashboardby#{split_name}?DateFrom=#{season_start.month}/#{season_start.day}/#{season_start.year}&DateTo=#{day.month}/#{day.day}/#{day.year}&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=#{measure_type}&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlusMinus=N&Rank=N&Season=#{season}&SeasonSegment=&SeasonType=#{season_type[0]}&ShotClockRange=&TeamID=#{team[:TEAM_ID]}&VsConference=&VsDivision="

    curl_str = "'#{url}' -H 'DNT: 1' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://stats.nba.com/team/' -H 'Cookie: ug=56a3f9430a9b8c0a3c6b801cc102b680; ugs=1; _ga=GA1.2.154703133.1453595386; _gat=1; s_cc=true; s_fid=5E33AEE20319BA02-2185E5CD6C4816E7; s_sq=%5B%5BB%5D%5D; s_vi=[CS]v1|2B51FC9E05195A61-6000060920000119[CE]' -H 'Connection: keep-alive' -H 'Cache-Control: max-age=0' --compressed"
  else
    binding.pry
    p "need an entity name"
  end

  begin
    for i in 0...5
      begin
  #      doc = Nokogiri::HTML( open( url ) )
        resp = `curl -s #{curl_str}`
      binding.pry
        json = JSON.parse( resp )
        if json
          break
        end
      rescue StandardError => e
        p "error..trying again. #{url}"
      end
    end
  rescue StandardError => e
    binding.pry
    p "error"
  end

  if "player" == entity
    csv = csv + json["resultSets"][0]["headers"].to_csv
    headers = json["resultSets"][0]["headers"]

    if "shootingsplits" == split_name
      for j in (0...json["resultSets"].size - 1)
        if json["resultSets"][j]["headers"] != headers
          binding.pry
          p "headers don't match"
        end
        json["resultSets"][j]["rowSet"].each{|row|
          csv = csv + row.to_csv
        }
      end

      File.open( season + "/" + season_type[1] + "/playerdailystats/#{player[:PLAYER_ID]}_#{day.year}#{day.month}#{day.day}_#{split_name}_#{measure_type}.csv", "w" ){|f|
        f.write csv
      }

      csv2 = json["resultSets"].last["headers"].to_csv
      json["resultSets"].last["rowSet"].each{|row|
        csv2 = csv2 + row.to_csv
      }

      File.open( season + "/" + season_type[1] + "/playerdailystats/#{player[:PLAYER_ID]}_#{day.year}#{day.month}#{day.day}_#{split_name}_#{measure_type}_#{json["resultSets"].last["name"]}.csv", "w" ){|f|
        f.write csv2
      }
    else
      json["resultSets"].each{|resultSet|
        if resultSet["headers"] != headers
          binding.pry
          p "headers don't match"
        end
        resultSet["rowSet"].each{|row|
          csv = csv + row.to_csv
        }
      }

      File.open( season + "/" + season_type[1] + "/playerdailystats/#{player[:PLAYER_ID]}_#{day.year}#{day.month}#{day.day}_#{split_name}_#{measure_type}.csv", "w" ){|f|
        f.write csv
      }
    end
  elsif "team" == entity
    begin
    headers = json["resultSets"][0]["headers"]

    if "shootingsplits" == split_name
      csv = csv + json["resultSets"][0]["headers"].to_csv

      for j in (0...json["resultSets"].size - 1)
        if json["resultSets"][j]["headers"] != headers
          binding.pry
          p "headers don't match"
        end
        json["resultSets"][j]["rowSet"].each{|row|
          csv = csv + row.to_csv
        }
      end

      File.open( season + "/" + season_type[1] + "/teamdailystats/#{team[:TEAM_ID]}_#{day.year}#{day.month}#{day.day}_#{split_name}_#{measure_type}.csv", "w" ){|f|

        f.write csv
      }

      csv2 = json["resultSets"].last["headers"].to_csv
      json["resultSets"].last["rowSet"].each{|row|
        csv2 = csv2 + row.to_csv
      }

      File.open( season + "/" + season_type[1] + "/teamdailystats/#{team[:TEAM_ID]}_#{day.year}#{day.month}#{day.day}_#{split_name}_#{measure_type}_#{json["resultSets"].last["name"]}.csv", "w" ){|f|
        f.write csv2
      }
    else
      #csv = csv + json["resultSets"][0]["headers"].to_csv
      headers_mod = headers[0..1] + headers[3..-1]
      rows = ""

      json["resultSets"].each{|resultSet|
        if resultSet["headers"] != headers
          new_headers = resultSet["headers"][0..1] + resultSet["headers"][3..-1]

          #if (resultSet["rowSet"][2] == resultSet["rowSet"][1]) and (new_headers == headers_mod)
          if new_headers == headers_mod
            headers = headers_mod
            #all good
          else
            binding.pry
            p "headers don't match"
          end
        end
        resultSet["rowSet"].each{|row|
          rows = rows + row.to_csv
        }
      }
      csv = headers.to_csv + rows

      File.open( season + "/" + season_type[1] + "/teamdailystats/#{team[:TEAM_ID]}_#{day.year}#{day.month}#{day.day}_#{split_name}_#{measure_type}.csv", "w" ){|f|
        f.write csv
      }
    end
    rescue StandardError => e
      binding.pry
      p "hi"
    end
  end
end

consumer_thread = Thread.new do
  loop do
    # Stop looping when the producer is finished producing work
    break if sysexit && work_queue.length == 0
    found_index = nil

    # The MonitorMixin requires us to obtain a lock on the threads array in case
    # a different thread may try to make changes to it.
    threads.synchronize do
      # First, wait on an available spot in the threads array.  This fires every
      # time a signal is sent to the "threads_available" variable
      threads_available.wait_while do
        threads.select { |thread| thread.nil? || thread.status == false  ||
                                  thread["finished"].nil? == false}.length == 0
      end
      # Once an available spot is found, get the index of that spot so we may
      # use it for the new thread
      found_index = threads.rindex { |thread| thread.nil? || thread.status == false ||
                                              thread["finished"].nil? == false }
    end

    # Get a new unit of work from the work queue
    task_parameters = work_queue.pop

    # Pass the currency variable to the new thread so it can use it as a parameter to go
    # get the exchange rates
    threads[found_index] = Thread.new(task_parameters) do
      if "daily" == stat_type
        downloadNbaData( task_parameters )
      elsif "tracking" == stat_type
        downloadTrackingData( task_parameters )
      end

      #Net::HTTP.get("download.finance.yahoo.com","/d/quotes.csv?e=.csv&amp;f=sl1d1t1&amp;s=CADUSD=X")
      # When this thread is finished, mark it as such so the consumer knows it is a
      # free spot in the array.
      Thread.current["finished"] = true

      # Tell the consumer to check the thread array
      threads.synchronize do
        threads_available.signal
      end
    end
  end
end

#seasons_a = seasons_h.to_a
if season_index
  seasons_a = [ seasons_a[ season_index ] ]
end

if s_range
  seasons_a = seasons_a[ s_range[0]...s_range[1] ]
end

producer_thread = Thread.new do
  seasons_a.each{|arr|
    season = arr[0]
    season_dates = arr[1]
    seasontype_url.each_with_index{|season_type,i|

      if "player" == entity
        players = database[ :"#{season_type[1]}_#{player_tables[5]}" ].where(:SEASON => season).distinct.select(:PLAYER_ID).entries

        if "no" == season_dates[ i + 1 ] 
          p "skip playoffs for #{season_dates[i]}"
          next
        end
        begin
        season_start = Date.parse( season_dates[ i ] )
        season_end = Date.parse( season_dates[ i + 1 ] )
        rescue StandardError => e
          p "error parsing #{season_dates[i]}"
          next
        end

        dir = FileUtils::mkdir_p season + "/" + season_type[1] + "/player#{stat_type}stats"

        players.each_with_index{|player,player_i|
          if PLAYER_SKIP and (player_i < PLAYER_SKIP)
            next
          end
          if player_id_skip and (player[:PLAYER_ID] != player_id_skip)
            next
          elsif player_id_skip and (player[:PLAYER_ID] == player_id_skip)
            p "commence, found player: #{player[:PLAYER_ID]} is #{player_id_skip}"
            player_id_skip = nil
          end
          day = season_start
          
          while day < season_end
            if day_skip and (day < day_skip)
              day = day + 1
              next
            end
            splits.each{|split|
              split_name = split[0]
              measure_types = split[1]

              measure_types.each{|measure_type|
                task_parameters = [ split_name, season_start, day, measure_type, player, season, season_type, "player" ]
                work_queue << task_parameters

                # Tell the consumer to check the thread array so it can attempt to schedule the
                # next job if a free spot exists.
                threads.synchronize do
                  threads_available.signal
                end
              }
            }

            p "player #{player_i}/#{players.size} day: #{day}"
            day = day + 1
          end
        }

      elsif "team" == entity
        teams = database[ :"#{season_type[1]}_#{team_tables[6]}" ].where(:SEASON => season).distinct.select(:TEAM_ID).entries

        if "no" == season_dates[ i + 1 ] 
          p "skip playoffs for #{season_dates[i]}"
          next
        end
        begin
        season_start = Date.parse( season_dates[ i ] )
        season_end = Date.parse( season_dates[ i + 1 ] )
        rescue StandardError => e
          p "error parsing #{season_dates[i]}"
          next
        end

        dir = FileUtils::mkdir_p season + "/" + season_type[1] + "/team#{stat_type}stats"

        teams.each_with_index{|team,team_i|
          if team_skip and (team_i < team_skip)
            next
          end
          if team_id_skip and (team[:TEAM_ID] != team_id_skip)
            next
          elsif team_id_skip and (team[:TEAM_ID] == team_id_skip)
            p "commence, found team: #{team[:TEAM_ID]} is #{team_id_skip}"
            team_id_skip = nil
          end
          day = season_start
          
          while day < season_end
            if day_skip and (day < day_skip)
              day = day + 1
              next
            end
            splits.each{|split|
              split_name = split[0]
              measure_types = split[1]

              measure_types.each{|measure_type|
                task_parameters = [ split_name, season_start, day, measure_type, team, season, season_type, "team" ]
                work_queue << task_parameters

                # Tell the consumer to check the thread array so it can attempt to schedule the
                # next job if a free spot exists.
                threads.synchronize do
                  threads_available.signal
                end
              }
            }

            p "team #{team_i}/#{teams.size} day: #{day}"
            day = day + 1
          end
        }
      end
    }
  }
  sysexit = true
end

# Join on both the producer and consumer threads so the main thread doesn't exit while
# they are doing work.
producer_thread.join
consumer_thread.join

# Join on the child processes to allow them to finish (if any are left)
threads.each do |thread|
    thread.join unless thread.nil?
end
=begin
#http://stats.nba.com/player/#!/202699/stats/?Season=2014-15&SeasonType=Regular%20Season&DateFrom=10%2F28%2F2014&DateTo=10%2F28%2F2014
http://stats.nba.com/stats/playerdashboardbygeneralsplits?DateFrom=10%2F28%2F2014&DateTo=10%2F28%2F2014&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerID=202699&PlusMinus=N&Rank=&Season=2014-15&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision=

#http://stats.nba.com/player/#!/202699/stats/?Season=2014-15&SeasonType=Regular%20Season&DateFrom=10%2F28%2F2014&DateTo=10%2F28%2F2014&Split=opp
http://stats.nba.com/stats/playerdashboardbyopponent?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerID=202699&PlusMinus=N&Rank=N&Season=2014-15&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision=

#http://stats.nba.com/player/#!/202699/stats/?Season=2014-15&SeasonType=Regular%20Season&DateFrom=10%2F28%2F2014&DateTo=10%2F28%2F2014&Split=lastn
http://stats.nba.com/stats/playerdashboardbylastngames?DateFrom=10%2F28%2F2014&DateTo=10%2F28%2F2014&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerID=202699&PlusMinus=N&Rank=N&Season=2014-15&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision=

#http://stats.nba.com/player/#!/202699/stats/?Season=2014-15&SeasonType=Regular%20Season&DateFrom=10%2F28%2F2014&DateTo=10%2F28%2F2014&Split=ingame
http://stats.nba.com/stats/playerdashboardbygamesplits?DateFrom=10%2F28%2F2014&DateTo=10%2F28%2F2014&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerID=202699&PlusMinus=N&Rank=N&Season=2014-15&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision=
-
#http://stats.nba.com/player/#!/202699/stats/advanced/?Season=2014-15&SeasonType=Regular%20Season
http://stats.nba.com/stats/playerdashboardbygeneralsplits?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerID=202699&PlusMinus=N&Rank=N&Season=2014-15&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision=
=end
#=begin
def deleteDupsFromPlayerLogs
  seasons = Dir.glob('*').select {|f| File.directory? f and f.match /\d+/}
  seasons.each{|season|
    p "deleting #{season}"
    database.run "delete from '#{season.gsub(/-/,"_")}_playoffs_player_gamelogs' where rowid not in ( select min(rowid) from '#{season.gsub(/-/,"_")}_playoffs_player_gamelogs' group by player_id, game_id )"
    database.run "delete from '#{season.gsub(/-/,"_")}_regularseason_player_gamelogs' where rowid not in ( select min(rowid) from '#{season.gsub(/-/,"_")}_regularseason_player_gamelogs' group by player_id, game_id )"
  }
end
#deleteDupsFromPlayerLogs
=begin
          otherdata = database[ :"playerbioinfo" ].distinct.where(:PERSON_ID => player[:PLAYER_ID]).select(:BIRTHDATE, :DISPLAY_FIRST_LAST, :HEIGHT, :WEIGHT, :JERSEY, :POSITION, :TEAM_NAME, :TEAM_ID, :TEAM_CITY, :TEAM_ABBREVIATION, :PLAYERCODE).entries
          birthdate = otherdata[0][:BIRTHDATE].to_date.strftime("%B %-d %Y")
          year = season.split("-")[0].to_i + 1 
          abbr = convertBBRTeamAbbr( player[:TEAM_ABBREVIATION], year )
          data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].distinct.where(:"Birth Date" => birthdate).where(:"Team" => abbr).entries

          if data.size == 1
            p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data[0][:Player]} #{data[0][:Team]}"
            data[0][:PLAYER_ID] = player[:PLAYER_ID]
            database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data[0][:Player], :Ht => data[0][:Ht], :Wt => data[0][:Wt], :Exp => data[0][:Exp], :College => data[0][:College], :"Birth Date" => data[0][:"Birth Date"]).update(data[0])
          elsif data.size > 1
            data.each_with_index{|d,ind|
              p "#{ind}: #{d[:Team]} #{d[:Player]} #{d[:'Birth Date']} #{d[:Ht]} #{d[:Wt]}"
            }
            p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]}"
            binding.pry
            matches.each{|match_index|
              data[match_index][:PLAYER_ID] = player[:PLAYER_ID]
              database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data[match_index][:Player], :Ht => data[match_index][:Ht], :Wt => data[match_index][:Wt], :Exp => data[match_index][:Exp], :College => data[match_index][:College], :"Birth Date" => data[match_index][:"Birth Date"]).update(data[match_index])
              p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data[match_index][:Player]} #{data[match_index][:Team]}"
            }
            p "more than 1"
          else
            data2 = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].distinct.where(Sequel.like(:"Player", "% #{name.split(' ')[1]}%")).entries

            if 1 == data2.size
              p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data2[0][:Player]} #{data2[0][:Team]}"
              data2[0][:PLAYER_ID] = player[:PLAYER_ID]
              database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data2[0][:Player], :Ht => data2[0][:Ht], :Wt => data2[0][:Wt], :Exp => data2[0][:Exp], :College => data2[0][:College], :"Birth Date" => data2[0][:"Birth Date"]).update(data2[0])
            elsif data2.size > 1
              if data2.size > 50
                data2 = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].distinct.where(Sequel.like(:"Player", "%#{name.split(' ')[0]}%")).entries
              end
              data2.each_with_index{|d,ind|
                p "#{ind}: #{d[:Team]} #{d[:Player]} #{d[:'Birth Date']} #{d[:Ht]} #{d[:Wt]}"
              }
              p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]}"
              binding.pry
              #change match_index in real-time, then change it back
              matches.each{|match_index|
                data2[match_index][:PLAYER_ID] = player[:PLAYER_ID]
                database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data2[match_index][:Player], :Ht => data2[match_index][:Ht], :Wt => data2[match_index][:Wt], :Exp => data2[match_index][:Exp], :College => data2[match_index][:College], :"Birth Date" => data2[match_index][:"Birth Date"]).update(data2[match_index])
                p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data2[match_index][:Player]} #{data2[match_index][:Team]}"
              }
              p "more than 1"
            else
              data2 = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].distinct.where(:Team => abbr).entries
              data2.each_with_index{|d,ind|
                p "#{ind}: #{d[:Team]} #{d[:Player]} #{d[:'Birth Date']} #{d[:Ht]} #{d[:Wt]}"
              }
              p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]}"
              binding.pry
              #change match_index in real-time, then change it back
              matches.each{|match_index|
                data2[match_index][:PLAYER_ID] = player[:PLAYER_ID]
                database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data2[match_index][:Player], :Ht => data2[match_index][:Ht], :Wt => data2[match_index][:Wt], :Exp => data2[match_index][:Exp], :College => data2[match_index][:College], :"Birth Date" => data2[match_index][:"Birth Date"]).update(data2[match_index])
                p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data2[match_index][:Player]} #{data2[match_index][:Team]}"
              }
              p "error"
            end
          end
    end
  }
}
=end
=begin
    seasons.each{|season|
      players = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{player_tables[5]}" ].distinct.select(:TEAM_ID, :TEAM_ABBREVIATION, :PLAYER_ID, :PLAYER_NAME).entries
      players.each_with_index{|player,p_index|
        matches = 0

        name = player[:PLAYER_NAME]
        data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => name).limit(1).entries

        if data and data.size > 0
          position = data[0][:Pos]

          hash = { :PLAYER_ID => player[:PLAYER_ID], :Player => name, :Position => position }
          data[0][:PLAYER_ID] = player[:PLAYER_ID]

          database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data[0][:Player], :Ht => data[0][:Ht], :Wt => data[0][:Wt], :Exp => data[0][:Exp], :College => data[0][:College], :"Birth Date" => data[0][:"Birth Date"]).update(data[0])
        else
          otherdata = database[ :"playerbioinfo" ].distinct.where(:PERSON_ID => player[:PLAYER_ID]).select(:BIRTHDATE, :DISPLAY_FIRST_LAST, :HEIGHT, :WEIGHT, :JERSEY, :POSITION, :TEAM_NAME, :TEAM_ID, :TEAM_CITY, :TEAM_ABBREVIATION, :PLAYERCODE).entries
          birthdate = otherdata[0][:BIRTHDATE].to_date.strftime("%B %-d %Y")
          year = season.split("-")[0].to_i + 1 
          abbr = convertBBRTeamAbbr( player[:TEAM_ABBREVIATION], year )
          data = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].distinct.where(:"Birth Date" => birthdate).where(:"Team" => abbr).entries

          if data.size == 1
            p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data[0][:Player]} #{data[0][:Team]}"
            data[0][:PLAYER_ID] = player[:PLAYER_ID]
            database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data[0][:Player], :Ht => data[0][:Ht], :Wt => data[0][:Wt], :Exp => data[0][:Exp], :College => data[0][:College], :"Birth Date" => data[0][:"Birth Date"]).update(data[0])
          elsif data.size > 1
            data.each_with_index{|d,ind|
              p "#{ind}: #{d[:Team]} #{d[:Player]} #{d[:'Birth Date']} #{d[:Ht]} #{d[:Wt]}"
            }
            p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]}"
            binding.pry
            matches.each{|match_index|
              data[match_index][:PLAYER_ID] = player[:PLAYER_ID]
              database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data[match_index][:Player], :Ht => data[match_index][:Ht], :Wt => data[match_index][:Wt], :Exp => data[match_index][:Exp], :College => data[match_index][:College], :"Birth Date" => data[match_index][:"Birth Date"]).update(data[match_index])
              p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data[match_index][:Player]} #{data[match_index][:Team]}"
            }
            p "more than 1"
          else
            data2 = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].distinct.where(Sequel.like(:"Player", "% #{name.split(' ')[1]}%")).entries

            if 1 == data2.size
              p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data2[0][:Player]} #{data2[0][:Team]}"
              data2[0][:PLAYER_ID] = player[:PLAYER_ID]
              database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data2[0][:Player], :Ht => data2[0][:Ht], :Wt => data2[0][:Wt], :Exp => data2[0][:Exp], :College => data2[0][:College], :"Birth Date" => data2[0][:"Birth Date"]).update(data2[0])
            elsif data2.size > 1
              if data2.size > 50
                data2 = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].distinct.where(Sequel.like(:"Player", "%#{name.split(' ')[0]}%")).entries
              end
              data2.each_with_index{|d,ind|
                p "#{ind}: #{d[:Team]} #{d[:Player]} #{d[:'Birth Date']} #{d[:Ht]} #{d[:Wt]}"
              }
              p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]}"
              binding.pry
              #change match_index in real-time, then change it back
              matches.each{|match_index|
                data2[match_index][:PLAYER_ID] = player[:PLAYER_ID]
                database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data2[match_index][:Player], :Ht => data2[match_index][:Ht], :Wt => data2[match_index][:Wt], :Exp => data2[match_index][:Exp], :College => data2[match_index][:College], :"Birth Date" => data2[match_index][:"Birth Date"]).update(data2[match_index])
                p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data2[match_index][:Player]} #{data2[match_index][:Team]}"
              }
              p "more than 1"
            else
              data2 = database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].distinct.where(:Team => abbr).entries
              data2.each_with_index{|d,ind|
                p "#{ind}: #{d[:Team]} #{d[:Player]} #{d[:'Birth Date']} #{d[:Ht]} #{d[:Wt]}"
              }
              p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]}"
              binding.pry
              #change match_index in real-time, then change it back
              matches.each{|match_index|
                data2[match_index][:PLAYER_ID] = player[:PLAYER_ID]
                database[ :"#{season.gsub(/-/,"_")}_bioinfo" ].where(:Player => data2[match_index][:Player], :Ht => data2[match_index][:Ht], :Wt => data2[match_index][:Wt], :Exp => data2[match_index][:Exp], :College => data2[match_index][:College], :"Birth Date" => data2[match_index][:"Birth Date"]).update(data2[match_index])
                p "#{player[:PLAYER_NAME]} #{player[:TEAM_ABBREVIATION]} matches with #{data2[match_index][:Player]} #{data2[match_index][:Team]}"
              }
              p "error"
            end
          end
        end
      }
      next
    }
=end
=begin
seasons_h.each{|season,season_dates|
  seasontype.each{|type|
    if "regularseason" == type
      date = Date.parse( season_dates[0] )
      end_date = Date.parse( season_dates[1] )
    else
      date = Date.parse( season_dates[1] )
      end_date = Date.parse( season_dates[2] )
    end

      teams = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{team_tables[6]}" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:TEAM_ABBREVIATION).entries
      players = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{player_tables[5]}" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:PLAYER_ID, :PLAYER_NAME, :TEAM_ABBREVIATION).entries
#=begin
      teams.each{|team|
        last_row = nil
        while date < end_date
          entries = database[:"#{season.gsub(/-/,"_")}_#{type}_daily_averages"].where(:average_type => average_type).where(:date => date.to_s).where(:team_abbreviation => team[:TEAM_ABBREVIATION]).where(:player_name => nil).entries
          if entries and entries.size > 0
            last_row = entries[0]
            if entries.size > 1
              binding.pry
              p "entries > 1, clean this up!!!!!!!!!!!!!!!!"
            end
          else
            p "#{date} #{team}"
          end

          if last_row and (last_row[:date] != date.to_s)
            last_row[:date_of_data] = last_row[:date]
            last_row[:date] = date.to_s
            binding.pry
          elsif last_row
            p "entry for: #{team[:TEAM_ABBREVIATION]} #{last_row[:date]} #{last_row[:date_of_data]} #{date.to_s}"
          end

          date = date + 1
        end
      }
#=end
      #if true == bTeam
      #  boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStats" ].where(:TEAM_ABBREVIATION => team[:TEAM_ABBREVIATION]).order(:DATE).entries
      #else
      #end
      players.each_with_index{|player,i|
        last_row = nil

        first_date_str = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_PlayerStats" ].where(:PLAYER_NAME => player[:PLAYER_NAME]).order(:DATE).min(:DATE)
        first_game_date = Date.parse( first_date_str )
        num_expected_boxscores = (end_date - first_game_date + 1).to_i

        #average_types = [ nil,"home","away","0 rest","1 rest","2 rest","3 rest","4 rest","5 rest","6 rest","34","46","prev2","prev5","opponent vs","starter","bench"]
        average_types = [ nil,"home","away","0 rest","1 rest","2 rest","3 rest","4 rest","5 rest","6 rest","34","46","prev2","prev5","opponent vs","starter","bench"]
        average_types.each{|average_type|
        entries_size = database[:"#{season.gsub(/-/,"_")}_#{type}_daily_averages"].where(:average_type => average_type).where(:player_name => player[:PLAYER_NAME]).where(:team_abbreviation => player[:TEAM_ABBREVIATION]).count
        if num_expected_boxscores != entries_size
          #binding.pry
          entries = database[:"#{season.gsub(/-/,"_")}_#{type}_daily_averages"].where(:average_type => average_type).where(:player_name => player[:PLAYER_NAME]).where(:team_abbreviation => player[:TEAM_ABBREVIATION]).order(:DATE).entries

          binding.pry
          cur_date = entries.first[:date]

          entries.each{|row|

            binding.pry
            while cur_date + 1 < row[:date]
              cur_date = cur_date + 1

              last_boxscore[:date] = cur_date
              #last_boxscore[:date_of_data] = last_boxscore_date

              database[:"#{season.gsub(/-/,"_")}_#{type}_daily_averages"].insert(last_boxscore.to_hash)
            end
            cur_date = row[:date]
            last_boxscore = row
          }

          #last_boxscore_date = last_boxscore[:date]
          while cur_date < season_end + 1
            cur_date = cur_date + 1

            last_boxscore[:date] = cur_date
            #last_boxscore[:date_of_data] = last_boxscore_date

            database[:"#{season.gsub(/-/,"_")}_#{type}_daily_averages"].insert(last_boxscore.to_hash)
          end

          p "#{player} entries: #{entries_size} num_expected_boxscores: #{num_expected_boxscores}"
        else
          p "#{player} #{i}/#{players.size} has #{entries_size} entries"
        end
      }
    }
  }
}
=end
=begin
def calculateScoringFormulaError( coefficients_array, season, type )
  #PTS_mean
  #opp_def_rtg
  #opp_def_rtg vs. _position, and mean opp_def_rtg
  #opp_pts vs. _position, and mean opp_pts
  #away_
  #home_
  #2nd_pts, FBPs, PITP, PTS OFF TO
  #team_PACE
  #opp_PACE
  #team_REST
  #opp_REST
  #prev2_PTS
  #prev5_PTS
  tablename = season + " " + type + " daily averages"
  tablename = tablename.gsub(/[^0-9a-zA-Z_]/,'_').to_sym

  player_tables = [ "advanced_PlayerStats", "fourfactors_sqlPlayersFourFactors", "misc_sqlPlayersMisc", "playertrack_PlayerTrack", "scoring_sqlPlayersScoring", "traditional_PlayerStats", "usage_sqlPlayersUsage" ]
  team_tables = [ "advanced_TeamStats", "fourfactors_sqlTeamsFourFactors", "misc_sqlTeamsMisc", "playertrack_PlayerTrackTeam", "scoring_sqlTeamsScoring", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlTeamsUsage" ]

  teams = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{team_tables[6]}" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:TEAM_ABBREVIATION).entries
  team_averages = Hash.new
  teams.each{|team|
    team_averages[ team[:TEAM_ABBREVIATION] ] = database[:"#{tablename}"].select(:PTS_mean, :FGA_mean, :FGM_mean, :FG3A_mean, :FG3M_mean, :FTM_mean, :FTA_mean, :FG_PCT, :FG3_PCT, :FT_PCT).where(:player_name => nil, :team_abbreviation => team[:TEAM_ABBREVIATION], :average_type => nil, :date => :date_of_data).entries
  }

  binding.pry
  players = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{player_tables[5]}" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:PLAYER_ID, :PLAYER_NAME).entries
  players.each{|player|
    splits = database[:"#{tablename}"].select(:PTS_mean, :FGA_mean, :FGM_mean, :FG3A_mean, :FG3M_mean, :FTM_mean, :FTA_mean, :FG_PCT, :FG3_PCT, :FT_PCT).where(:player_name => player_name, :date => :date_of_data).entries
    home_splits = database[:"#{tablename}"].select(:PTS_mean, :FGA_mean, :FGM_mean, :FG3A_mean, :FG3M_mean, :FTM_mean, :FTA_mean, :FG_PCT, :FG3_PCT, :FT_PCT).where(:player_name => player_name, :average_type => "home", :date => :date_of_data).entries
    away_splits = database[:"#{tablename}"].select(:PTS_mean, :FGA_mean, :FGM_mean, :FG3A_mean, :FG3M_mean, :FTM_mean, :FTA_mean, :FG_PCT, :FG3_PCT, :FT_PCT).where(:player_name => player_name, :average_type => "away", :date => :date_of_data).entries
  }
  database[:"#{tablename}"].where(:).select(:PTS_mean, :FGA_mean, :FGM_mean, :FG3A_mean, :FG3M_mean, :FTM_mean, :FTA_mean, :FG_PCT, :FG3_PCT, :FT_PCT
                                      column :player_name, :text
                                      column :date, :text
                                      column :date_of_data, :text
                                      column :game_id, :text
                                      column :team_abbreviation, :text
                                      column :average_type, :text
                                      column :opponent_against_abbr, :text
                                      column :games_played, :integer

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
        column :FGA_2PT_mean, :decimal
        column :PTS_2PT_mean, :decimal
        column :PTS_2PT_MR_mean, :decimal
        column :PTS_3PT_mean, :decimal
        column :PTS_FT_mean, :decimal

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
#opponents
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
        column :o_FGA_2PT_total, :decimal
        column :o_FGA_2PT_mean, :decimal
        column :o_FGA_2PT_median, :decimal
        column :o_PTS_2PT_total, :decimal
        column :o_PTS_2PT_mean, :decimal
        column :o_PTS_2PT_median, :decimal
        column :o_PTS_2PT_MR_total, :decimal
        column :o_PTS_2PT_MR_mean, :decimal
        column :o_PTS_2PT_MR_median, :decimal
        column :o_PTS_3PT_total, :decimal
        column :o_PTS_3PT_mean, :decimal
        column :o_PTS_3PT_median, :decimal
        column :o_PTS_FB_total, :decimal
        column :o_PTS_FB_mean, :decimal
        column :o_PTS_FB_median, :decimal
        #column :o_PTS_FT_total, :decimal
        #column :o_PTS_FT_mean, :decimal
        #column :o_PTS_FT_median, :decimal
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
    data_pos = database[:"fanduelinfo"].distinct.where(:PLAYER_ID => boxscore_traditional[:PLAYER_ID]).where(:Date => boxscore_traditional[:DATE].gsub(/-/,"").to_i).entries
=end
#end

=begin
def calculateReboundFormulaError( coefficients_array )
  OREB_mean
  DREB_mean
  avg_DREB_chances
  avg_OREB_chances
  opp avg_DREB_chances
  opp avg_OREB_chances
  DREB%
  OREB%
  DREB%, OREB% of other team
  DREB%, OREB% of opponent on other team

  opp_def_rtg
  opp_def_rtg vs. _position, and mean opp_def_rtg
  opp_pts vs. _position, and mean opp_pts
  team_PACE
  opp_PACE
  team_REST
  opp_REST
  prev2_PTS
  prev5_PTS
end
=end

def leaders( season_sym, category, limit, date )
  database[season_sym].select(category).select(:Player).where(:date => date).where(:Position => "PG").order(category).entries
end

