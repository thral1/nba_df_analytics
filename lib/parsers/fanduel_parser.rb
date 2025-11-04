#!/usr/bin/env ruby
require 'rubygems'
require 'csv'
require 'sequel'
require 'sqlite3'
require 'pry'
require 'date'
require 'nokogiri'
require 'open-uri'
require 'json'
require 'net/http'

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
        doc = Nokogiri::HTML( URI.open( "http://rotoguru1.com/cgi-bin/hyday.pl?game=fd&mon=#{day.month}&day=#{day.day}&year=#{day.year}" ) )

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
        doc = Nokogiri::HTML( URI.open( "http://rotoguru1.com/cgi-bin/hyday.pl?game=fd&mon=#{day.month}&day=#{day.day}&year=#{day.year}&scsv=1" ) )
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

def convertNBATeamAbbrToFanDuelAbbr( nbaAbbr )
  if "NOP" == nbaAbbr
    return "nor"
  elsif "PHX" == nbaAbbr
    return "pho"
  else
    return nbaAbbr.downcase
  end
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

