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

def divide( num, den )
  if !num or !den or 0 == den
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

def createDailyTeamAveragesTable( database, new_tablename )
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

def checkIfPlayerChangedTeams2( database, season, type, i, team_gamelogs, team_abbr )
  if i > 0
    return team_gamelogs[i][:TEAM_ABBREVIATION]
  else
    return team_abbr
  end
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
      #p "front_b2b"
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
def calculateDerivedStats( statSet, o_statSet, database, tablename, team_abbreviation, game_id, bTeam, team_daily_averages = nil ) 
  tLoopStart = Time.now
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
      
      if nil == team_daily_averages
        team_daily_averages = database[tablename].where(:team_abbreviation => team_abbreviation).where(:game_id => game_id).entries[0]
      end
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
    doc = Nokogiri::HTML( URI.open( "https://www.basketball-reference.com/teams/#{team_corrected}/#{year}.html" ) )
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
  doc = Nokogiri::HTML( URI.open( "https://www.basketball-reference.com/teams/#{team_corrected}/#{year}.html" ) )

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
      doc = Nokogiri::HTML( URI.open( "https://www.basketball-reference.com/teams/#{new_abbr}/#{year}.html" ) )
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

def populatePointsFeatures( r, pts_mean, def_rtg_delta, def_rtg_v_position_delta, o_pts_delta, b2b, opp_b2b, extra_rest,opp_extra_rest, location, location_pts_effect, rest_effect, pts_paint_effect, pts_off_tov_effect, fb_effect, pts_2ndchance_effect,usg_PCT, usg_PCT_minus_TOV, location_pts, rest_pts, opp_rest_pts, expected_PTS_pace,pts_pace_effect, expected_PTS_pace2, pts_pace2_effect, expected_PTS_pace3, pts_pace3_effect, expected_PTS_def_rtg, def_rtg_effect,expected_PTS_def_rtg_v_position, def_rtg_v_position_effect, expected_PTS_off_rtg, off_rtg_PTS_effect, expected_PTS_opp_PTS,expected_PTS_opp_PTS_effect, mean_starter_pts, mean_bench_pts, starterbench_pts_effect, starter, mean_starterbench_pts, prev_pts,prev_pts_delta, prev2_pts, prev2_pts_delta, prev5_pts, prev5_pts_delta, ft_effect, expected_FTM, vegas_ratio_pts, vegas_ratio_pts_effect,vegas_ratio_pts_pinnacle, vegas_ratio_pts_pinnacle_effect, vegas_ratio_pts_opp_pinnacle, vegas_ratio_pts_opp_pinnacle_effect,vegas_ratio_pts_ou_pinnacle, vegas_ratio_pts_ou_pinnacle_effect, adjusted_cfg_pts,adjusted_ufg_pts, adjusted_fg_pts, cfg_effect, actual_PTS )

  r[:PTS_mean] = pts_mean
  r[:def_rtg_delta] = def_rtg_delta 
  r[:def_rtg_v_position_delta] = def_rtg_v_position_delta 
  r[:o_pts_delta] = o_pts_delta 
  r[:b2b] = b2b 
  r[:opp_b2b] = opp_b2b 
  r[:extra_rest] = extra_rest 
                             
  r[:opp_extra_rest] = opp_extra_rest 
  r[:location] = location 
  r[:location_pts_effect] = location_pts_effect 
  r[:rest_effect] = rest_effect 
  r[:pts_paint_effect] = pts_paint_effect 
  r[:pts_off_tov_effect] = pts_off_tov_effect 
  r[:fb_effect] = fb_effect 
  r[:pts_2ndchance_effect] = pts_2ndchance_effect 
                             
  r[:USG_PCT] = usg_PCT 
  r[:USG_PCT_minus_TOV] = usg_PCT_minus_TOV 
  r[:location_pts] = location_pts 
  r[:rest_pts] = rest_pts 
  r[:opp_rest_pts] = opp_rest_pts 
  r[:expected_PTS_pace] = expected_PTS_pace 
                             
  r[:pts_pace_effect] = pts_pace_effect 
  r[:expected_PTS_pace2] = expected_PTS_pace2 
  r[:pts_pace2_effect] = pts_pace2_effect 
  r[:expected_PTS_pace3] = expected_PTS_pace3 
  r[:pts_pace3_effect] = pts_pace3_effect 
  r[:expected_PTS_def_rtg] = expected_PTS_def_rtg 
  r[:def_rtg_effect] = def_rtg_effect 
                             
  r[:expected_PTS_def_rtg_v_position] = expected_PTS_def_rtg_v_position 
  r[:def_rtg_v_position_effect] = def_rtg_v_position_effect 
  r[:expected_PTS_off_rtg] = expected_PTS_off_rtg 
  r[:off_rtg_PTS_effect] = off_rtg_PTS_effect 
  r[:expected_PTS_opp_PTS] = expected_PTS_opp_PTS 
                             
  r[:expected_PTS_opp_PTS_effect] = expected_PTS_opp_PTS_effect 
  r[:mean_starter_pts] = mean_starter_pts 
  r[:mean_bench_pts] = mean_bench_pts 
  r[:starterbench_pts_effect] = starterbench_pts_effect 
  r[:starter] = starter 
  r[:mean_starterbench_pts] = mean_starterbench_pts 
  r[:prev_pts] = prev_pts 
                             
  r[:prev_pts_delta] = prev_pts_delta 
  r[:prev2_pts] = prev2_pts 
  r[:prev2_pts_delta] = prev2_pts_delta 
  r[:prev5_pts] = prev5_pts 
  r[:prev5_pts_delta] = prev5_pts_delta 
  r[:ft_effect] = ft_effect 
  r[:expected_FTM] = expected_FTM 
  r[:vegas_ratio_pts] = vegas_ratio_pts 
  r[:vegas_ratio_pts_effect] = vegas_ratio_pts_effect 
                             
  r[:vegas_ratio_pts_pinnacle] = vegas_ratio_pts_pinnacle 
  r[:vegas_ratio_pts_pinnacle_effect] = vegas_ratio_pts_pinnacle_effect 
  r[:vegas_ratio_pts_opp_pinnacle] = vegas_ratio_pts_opp_pinnacle 
  r[:vegas_ratio_pts_opp_pinnacle_effect] = vegas_ratio_pts_opp_pinnacle_effect 
                             
  r[:vegas_ratio_pts_ou_pinnacle] = vegas_ratio_pts_ou_pinnacle 
  r[:vegas_ratio_pts_ou_pinnacle_effect] = vegas_ratio_pts_ou_pinnacle_effect 
  r[:adjusted_cfg_pts] = adjusted_cfg_pts 
                             
  r[:adjusted_ufg_pts] = adjusted_ufg_pts 
  r[:adjusted_fg_pts] = adjusted_fg_pts 
  r[:cfg_effect] = cfg_effect 
  r[:ft_effect] = ft_effect 
  r[:expected_FTM] = expected_FTM 

  r[:actual_PTS] = actual_PTS
end

def populatePointsPerMinFeatures( r, mean_pts_per_min, def_rtg_delta, def_rtg_v_position_delta, o_pts_delta_per_min,location_pts_effect_per_min, rest_effect_per_min, pts_pace_effect_per_min, pts_paint_effect_per_min,pts_off_tov_effect_per_min, fb_effect_per_min, pts_2ndchance_effect_per_min, usg_PCT, usg_PCT_minus_TOV,location_pts_per_min, rest_pts_per_min, opp_rest_pts_per_min, expected_PTS_pace_per_min, expected_PTS_pace2_per_min,pts_pace2_effect_per_min, expected_PTS_pace3_per_min, pts_pace3_effect_per_min, expected_PTS_def_rtg_per_min, def_rtg_effect_per_min,expected_PTS_def_rtg_v_position_per_min, def_rtg_v_position_effect_per_min, expected_PTS_off_rtg_per_min,off_rtg_PTS_effect_per_min, expected_PTS_opp_PTS_per_min, expected_PTS_opp_PTS_effect_per_min, mean_starter_pts_per_min, mean_bench_pts_per_min,starterbench_pts_effect_per_min, prev_pts_per_min, prev_pts_delta_per_min, prev2_pts_per_min, prev2_pts_delta_per_min, prev5_pts_per_min,prev5_pts_delta_per_min, ft_effect_per_min, expected_FTM_per_min, vegas_ratio_pts_per_min, vegas_ratio_pts_effect_per_min, vegas_ratio_pts_pinnacle_per_min,vegas_ratio_pts_pinnacle_effect_per_min, vegas_ratio_pts_opp_pinnacle_per_min, vegas_ratio_pts_opp_pinnacle_effect_per_min,vegas_ratio_pts_ou_pinnacle_per_min, vegas_ratio_pts_ou_pinnacle_effect_per_min, adjusted_cfg_pts_per_min,adjusted_ufg_pts_per_min, adjusted_fg_pts_per_min, cfg_effect_per_min,actual_mins, actual_pts_per_min )

  r[:mean_pts_per_min] = mean_pts_per_min
  r[:def_rtg_delta] = def_rtg_delta 
  r[:def_rtg_v_position_delta] = def_rtg_v_position_delta 
  r[:o_pts_delta_per_min] = o_pts_delta_per_min 
  r[:location_pts_effect_per_min] = location_pts_effect_per_min 
  r[:rest_effect_per_min] = rest_effect_per_min 
  r[:pts_pace_effect_per_min] = pts_pace_effect_per_min 
  r[:pts_paint_effect_per_min] = pts_paint_effect_per_min
  r[:pts_off_tov_effect_per_min] = pts_off_tov_effect_per_min 
  r[:fb_effect_per_min] = fb_effect_per_min 
  r[:pts_2ndchance_effect_per_min] = pts_2ndchance_effect_per_min 
  r[:USG_PCT] = usg_PCT
  r[:USG_PCT_minus_TOV] = usg_PCT_minus_TOV
  r[:location_pts_per_min] = location_pts_per_min 
  r[:rest_pts_per_min] = rest_pts_per_min 
  r[:opp_rest_pts_per_min] = opp_rest_pts_per_min 
  r[:expected_PTS_pace_per_min] = expected_PTS_pace_per_min 
  r[:expected_PTS_pace2_per_min] = expected_PTS_pace2_per_min 
  r[:pts_pace2_effect_per_min] = pts_pace2_effect_per_min 
  r[:expected_PTS_pace3_per_min] = expected_PTS_pace3_per_min 
  r[:pts_pace3_effect_per_min] = pts_pace3_effect_per_min 
  r[:expected_PTS_def_rtg_per_min] = expected_PTS_def_rtg_per_min 
  r[:def_rtg_effect_per_min] = def_rtg_effect_per_min 
  r[:expected_PTS_def_rtg_v_position_per_min] = expected_PTS_def_rtg_v_position_per_min 
  r[:def_rtg_v_position_effect_per_min] = def_rtg_v_position_effect_per_min 
  r[:expected_PTS_off_rtg_per_min] = expected_PTS_off_rtg_per_min 
  r[:off_rtg_PTS_effect_per_min] = off_rtg_PTS_effect_per_min 
  r[:expected_PTS_opp_PTS_per_min] = expected_PTS_opp_PTS_per_min 
  r[:expected_PTS_opp_PTS_effect_per_min] = expected_PTS_opp_PTS_effect_per_min 
  r[:mean_starter_pts_per_min] = mean_starter_pts_per_min 
  r[:mean_bench_pts_per_min] = mean_bench_pts_per_min 
  r[:starterbench_pts_effect_per_min] = starterbench_pts_effect_per_min 
  r[:prev_pts_per_min] = prev_pts_per_min 
  r[:prev_pts_delta_per_min] = prev_pts_delta_per_min 
  r[:prev2_pts_per_min] = prev2_pts_per_min 
  r[:prev2_pts_delta_per_min] = prev2_pts_delta_per_min 
  r[:prev5_pts_per_min] = prev5_pts_per_min 
  r[:prev5_pts_delta_per_min] = prev5_pts_delta_per_min 
  r[:ft_effect_per_min] = ft_effect_per_min 
  r[:expected_FTM_per_min] = expected_FTM_per_min 
  r[:vegas_ratio_pts_per_min] = vegas_ratio_pts_per_min 
  r[:vegas_ratio_pts_effect_per_min] = vegas_ratio_pts_effect_per_min 
  r[:vegas_ratio_pts_pinnacle_per_min] = vegas_ratio_pts_pinnacle_per_min 
  r[:vegas_ratio_pts_pinnacle_effect_per_min] = vegas_ratio_pts_pinnacle_effect_per_min 
  r[:vegas_ratio_pts_opp_pinnacle_per_min] = vegas_ratio_pts_opp_pinnacle_per_min 
  r[:vegas_ratio_pts_opp_pinnacle_effect_per_min] = vegas_ratio_pts_opp_pinnacle_effect_per_min 
  r[:vegas_ratio_pts_ou_pinnacle_per_min] = vegas_ratio_pts_ou_pinnacle_per_min 
  r[:vegas_ratio_pts_ou_pinnacle_effect_per_min] = vegas_ratio_pts_ou_pinnacle_effect_per_min 
  r[:adjusted_cfg_pts_per_min] = adjusted_cfg_pts_per_min 
  r[:adjusted_ufg_pts_per_min] = adjusted_ufg_pts_per_min 
  r[:adjusted_fg_pts_per_min] = adjusted_fg_pts_per_min 
  r[:cfg_effect_per_min] = cfg_effect_per_min 
  r[:actual_mins] = actual_mins 
  r[:actual_pts_per_min] = actual_pts_per_min
end

#team oreb_pct
#mean_starter_oreb,mean_bench_oreb,mean_starterbench_oreb,prev_oreb,prev2_oreb,prev5_oreb,rest_oreb,rest_oreb_effect,opp_rest_oreb,opp_rest_oreb_effect,location_oreb,location_oreb_effect,expected_OREB,expected_OREB_effect,scaled_oreb_pct,scaled_oreb_pct_effect,scaled_oreb,scaled_oreb_pct_effect,modded_oreb,modded_oreb_effect

def populateOrebFeatures( r, oreb_mean, oreb_PCT, opp_average_DREB_PCT, opp_average_DREB_mean,o_oreb_delta, location_oreb_effect,rest_oreb_effect,location_oreb,rest_oreb,opp_rest_oreb,mean_starter_oreb,mean_bench_oreb,starterbench_oreb_effect,mean_starterbench_oreb,prev_oreb,prev_oreb_delta,prev2_oreb, prev2_oreb_delta, prev5_oreb, prev5_oreb_delta, o_oreb_pct_delta,expected_OREB,expected_OREB_effect,scaled_oreb_pct,scaled_oreb_pct_effect, scaled_oreb, scaled_oreb_effect, modded_oreb, modded_oreb_effect,opp_average_v_position_OREB_mean,opp_average_v_position_OREB_PCT, opp_average_OREB_PCT,team_misses, team_3p_misses, team_2p_misses,team_ft_misses, opp_average_FT_PCT,opp_average_FG_PCT, opp_average_FG3_PCT, opp_average_FG2_PCT,expected_OREB_pace, oreb_pace_effect, expected_OREB_pace2, oreb_pace2_effect, expected_OREB_pace3, oreb_pace3_effect, expected_OREB_def_rtg,def_rtg_OREB_effect, expected_OREB_off_rtg, off_rtg_OREB_effect, expected_OREB_opp_OREB, expected_OREB_opp_OREB_effect, vegas_ratio_oreb,vegas_ratio_oreb_effect, vegas_ratio_oreb_pinnacle, vegas_ratio_oreb_pinnacle_effect, vegas_ratio_oreb_opp_pinnacle, vegas_ratio_oreb_opp_pinnacle_effect,vegas_ratio_oreb_ou_pinnacle, vegas_ratio_oreb_ou_pinnacle_effect, team_average_FT_PCT, team_average_FG_PCT, team_average_FG3_PCT,team_average_FG2_PCT, actual_OREB )

  r[:OREB_mean] = oreb_mean
  r[:OREB_PCT] = oreb_PCT
  r[:opp_average_DREB_PCT] = opp_average_DREB_PCT
  r[:opp_average_DREB_mean] = opp_average_DREB_mean
                           
  r[:o_oreb_delta] = o_oreb_delta 
  r[:location_oreb_effect] = location_oreb_effect
  r[:rest_oreb_effect] = rest_oreb_effect
  r[:location_oreb] = location_oreb
  r[:rest_oreb] = rest_oreb
                           
  r[:opp_rest_oreb] = opp_rest_oreb
  r[:mean_starter_oreb] = mean_starter_oreb
  r[:mean_bench_oreb] = mean_bench_oreb
  r[:starterbench_oreb_effect] = starterbench_oreb_effect
  r[:mean_starterbench_oreb] = mean_starterbench_oreb
  r[:prev_oreb] = prev_oreb
  r[:prev_oreb_delta] = prev_oreb_delta 
                           
  r[:prev2_oreb] = prev2_oreb 
  r[:prev2_oreb_delta] = prev2_oreb_delta 
  r[:prev5_oreb] = prev5_oreb 
  r[:prev5_oreb_delta] = prev5_oreb_delta 
  r[:o_oreb_pct_delta] = o_oreb_pct_delta
  r[:expected_OREB] = expected_OREB
  r[:expected_OREB_effect] = expected_OREB_effect
  r[:scaled_oreb_pct] = scaled_oreb_pct
                           
  r[:scaled_oreb_pct_effect] = scaled_oreb_pct_effect 
  r[:scaled_oreb] = scaled_oreb 
  r[:scaled_oreb_effect] = scaled_oreb_effect 
  r[:modded_oreb] = modded_oreb 
  r[:modded_oreb_effect] = modded_oreb_effect
  r[:opp_average_v_position_OREB_mean] = opp_average_v_position_OREB_mean
                           
  r[:opp_average_v_position_OREB_PCT] = opp_average_v_position_OREB_PCT
  r[:opp_average_OREB_PCT] = opp_average_OREB_PCT
  r[:team_misses] = team_misses 
  r[:team_3p_misses] = team_3p_misses 
  r[:team_2p_misses] = team_2p_misses 
                           
  r[:team_ft_misses] = team_ft_misses 
  r[:opp_average_FT_PCT] = opp_average_FT_PCT
  r[:opp_average_FG_PCT] = opp_average_FG_PCT
  r[:opp_average_FG3_PCT] = opp_average_FG3_PCT
  r[:opp_average_FG2_PCT] = opp_average_FG2_PCT
                           
  r[:expected_OREB_pace] = expected_OREB_pace 
  r[:oreb_pace_effect] = oreb_pace_effect 
  r[:expected_OREB_pace2] = expected_OREB_pace2 
  r[:oreb_pace2_effect] = oreb_pace2_effect 
  r[:expected_OREB_pace3] = expected_OREB_pace3 
  r[:oreb_pace3_effect] = oreb_pace3_effect 
  r[:expected_OREB_def_rtg] = expected_OREB_def_rtg 
                           
  r[:def_rtg_OREB_effect] = def_rtg_OREB_effect 
  r[:expected_OREB_off_rtg] = expected_OREB_off_rtg 
  r[:off_rtg_OREB_effect] = off_rtg_OREB_effect 
  r[:expected_OREB_opp_OREB] = expected_OREB_opp_OREB 
  r[:expected_OREB_opp_OREB_effect] = expected_OREB_opp_OREB_effect 
  r[:vegas_ratio_oreb] = vegas_ratio_oreb 
                           
  r[:vegas_ratio_oreb_effect] = vegas_ratio_oreb_effect 
  r[:vegas_ratio_oreb_pinnacle] = vegas_ratio_oreb_pinnacle 
  r[:vegas_ratio_oreb_pinnacle_effect] = vegas_ratio_oreb_pinnacle_effect 
  r[:vegas_ratio_oreb_opp_pinnacle] = vegas_ratio_oreb_opp_pinnacle 
  r[:vegas_ratio_oreb_opp_pinnacle_effect] = vegas_ratio_oreb_opp_pinnacle_effect 
                           
  r[:vegas_ratio_oreb_ou_pinnacle] = vegas_ratio_oreb_ou_pinnacle 
  r[:vegas_ratio_oreb_ou_pinnacle_effect] = vegas_ratio_oreb_ou_pinnacle_effect 
  r[:team_average_FT_PCT] = team_average_FT_PCT
  r[:team_average_FG_PCT] = team_average_FG_PCT
  r[:team_average_FG3_PCT] = team_average_FG3_PCT
  r[:team_average_FG2_PCT] = team_average_FG2_PCT

  r[:actual_OREB] = actual_OREB
end

def populateOrebsPerMinFeatures( r, mean_oreb_per_min, oreb_PCT, opp_average_DREB_PCT, opp_average_dreb_per_min,o_oreb_delta_per_min, location_oreb_effect_per_min,rest_oreb_effect_per_min,location_oreb_per_min,rest_oreb_per_min,opp_rest_oreb_per_min,mean_starter_oreb_per_min,mean_bench_oreb_per_min,starterbench_oreb_effect_per_min,mean_starterbench_oreb_per_min,prev_oreb_per_min,prev_oreb_delta_per_min, prev2_oreb_per_min,prev2_oreb_delta_per_min, prev5_oreb_per_min, prev5_oreb_delta_per_min, o_oreb_pct_delta,expected_OREB_per_min,expected_OREB_effect_per_min,scaled_oreb_pct,scaled_oreb_pct_effect, scaled_oreb_per_min, scaled_oreb_effect_per_min, modded_oreb_per_min, modded_oreb_effect_per_min,opp_average_oreb_per_min, opp_average_v_position_OREB_PCT, opp_average_OREB_PCT,team_misses_per_min, team_3p_misses_per_min,team_2p_misses_per_min, team_ft_misses_per_min, opp_average_FT_PCT, opp_average_FG_PCT, opp_average_FG3_PCT, opp_average_FG2_PCT,expected_OREB_pace_per_min, oreb_pace_effect_per_min, expected_OREB_pace2_per_min, oreb_pace2_effect_per_min, expected_OREB_pace3_per_min,oreb_pace3_effect_per_min, expected_OREB_def_rtg_per_min, def_rtg_OREB_effect_per_min, expected_OREB_off_rtg_per_min, off_rtg_OREB_effect_per_min,expected_OREB_opp_OREB_per_min, expected_OREB_opp_OREB_effect_per_min, vegas_ratio_oreb_per_min, vegas_ratio_oreb_effect_per_min, vegas_ratio_oreb_pinnacle_per_min,vegas_ratio_oreb_pinnacle_effect_per_min, vegas_ratio_oreb_opp_pinnacle_per_min, vegas_ratio_oreb_opp_pinnacle_effect_per_min, vegas_ratio_oreb_ou_pinnacle_per_min,vegas_ratio_oreb_ou_pinnacle_effect_per_min, actual_oreb_per_min )

  r[:mean_oreb_per_min] = mean_oreb_per_min
  r[:OREB_PCT] = oreb_PCT
  r[:opp_average_DREB_PCT] = opp_average_DREB_PCT
  r[:opp_average_dreb_per_min] = opp_average_dreb_per_min
                                      
  r[:o_oreb_delta_per_min] = o_oreb_delta_per_min 
  r[:location_oreb_effect_per_min] = location_oreb_effect_per_min
  r[:rest_oreb_effect_per_min] = rest_oreb_effect_per_min
                                      
  r[:location_oreb_per_min] = location_oreb_per_min
  r[:rest_oreb_per_min] = rest_oreb_per_min
  r[:opp_rest_oreb_per_min] = opp_rest_oreb_per_min
  r[:mean_starter_oreb_per_min] = mean_starter_oreb_per_min
  r[:mean_bench_oreb_per_min] = mean_bench_oreb_per_min
                                      
  r[:starterbench_oreb_effect_per_min] = starterbench_oreb_effect_per_min
  r[:mean_starterbench_oreb_per_min] = mean_starterbench_oreb_per_min
  r[:prev_oreb_per_min] = prev_oreb_per_min
  r[:prev_oreb_delta_per_min] = prev_oreb_delta_per_min 
  r[:prev2_oreb_per_min] = prev2_oreb_per_min 
                                      
  r[:prev2_oreb_delta_per_min] = prev2_oreb_delta_per_min 
  r[:prev5_oreb_per_min] = prev5_oreb_per_min 
  r[:prev5_oreb_delta_per_min] = prev5_oreb_delta_per_min 
  r[:o_oreb_pct_delta] = o_oreb_pct_delta
  r[:expected_OREB_per_min] = expected_OREB_per_min
  r[:expected_OREB_effect_per_min] = expected_OREB_effect_per_min
                                      
  r[:scaled_oreb_pct] = scaled_oreb_pct
  r[:scaled_oreb_pct_effect] = scaled_oreb_pct_effect 
  r[:scaled_oreb_per_min] = scaled_oreb_per_min 
  r[:scaled_oreb_effect_per_min] = scaled_oreb_effect_per_min 
  r[:modded_oreb_per_min] = modded_oreb_per_min 
  r[:modded_oreb_effect_per_min] = modded_oreb_effect_per_min
                                      
  r[:opp_average_oreb_per_min] = opp_average_oreb_per_min 
  r[:opp_average_v_position_OREB_PCT] = opp_average_v_position_OREB_PCT
  r[:opp_average_OREB_PCT] = opp_average_OREB_PCT
  r[:team_misses_per_min] = team_misses_per_min 
  r[:team_3p_misses_per_min] = team_3p_misses_per_min 
                                      
  r[:team_2p_misses_per_min] = team_2p_misses_per_min 
  r[:team_ft_misses_per_min] = team_ft_misses_per_min 
  r[:opp_average_FT_PCT] = opp_average_FT_PCT
  r[:opp_average_FG_PCT] = opp_average_FG_PCT
  r[:opp_average_FG3_PCT] = opp_average_FG3_PCT
  r[:opp_average_FG2_PCt] = opp_average_FG2_PCT
                                      
  r[:expected_OREB_pace_per_min] = expected_OREB_pace_per_min 
  r[:oreb_pace_effect_per_min] = oreb_pace_effect_per_min 
  r[:expected_OREB_pace2_per_min] = expected_OREB_pace2_per_min 
  r[:oreb_pace2_effect_per_min] = oreb_pace2_effect_per_min 
  r[:expected_OREB_pace3_per_min] = expected_OREB_pace3_per_min 
                                      
  r[:oreb_pace3_effect_per_min] = oreb_pace3_effect_per_min 
  r[:expected_OREB_def_rtg_per_min] = expected_OREB_def_rtg_per_min 
  r[:def_rtg_OREB_effect_per_min] = def_rtg_OREB_effect_per_min 
  r[:expected_OREB_off_rtg_per_min] = expected_OREB_off_rtg_per_min 
  r[:off_rtg_OREB_effect_per_min] = off_rtg_OREB_effect_per_min 
                                      
  r[:expected_OREB_opp_OREB_per_min] = expected_OREB_opp_OREB_per_min 
  r[:expected_OREB_opp_OREB_effect_per_min] = expected_OREB_opp_OREB_effect_per_min 
  r[:vegas_ratio_oreb_per_min] = vegas_ratio_oreb_per_min 
  r[:vegas_ratio_oreb_effect_per_min] = vegas_ratio_oreb_effect_per_min 
  r[:vegas_ratio_oreb_pinnacle_per_min] = vegas_ratio_oreb_pinnacle_per_min 
                                      
  r[:vegas_ratio_oreb_pinnacle_effect_per_min] = vegas_ratio_oreb_pinnacle_effect_per_min 
  r[:vegas_ratio_oreb_opp_pinnacle_per_min] = vegas_ratio_oreb_opp_pinnacle_per_min 
  r[:vegas_ratio_oreb_opp_pinnacle_effect_per_min] = vegas_ratio_oreb_opp_pinnacle_effect_per_min 
  r[:vegas_ratio_oreb_ou_pinnacle_per_min] = vegas_ratio_oreb_ou_pinnacle_per_min 
                                      
  r[:vegas_ratio_oreb_ou_pinnacle_effect_per_min] = vegas_ratio_oreb_ou_pinnacle_effect_per_min 
  r[:actual_oreb_per_min] = actual_oreb_per_min
end

def populateDrebsFeatures( r, dreb_mean, dreb_PCT,opp_average_OREB_PCT,opp_average_OREB_mean,o_dreb_delta, location_dreb_effect,rest_dreb_effect,location_dreb,rest_dreb,opp_rest_dreb,mean_starter_dreb,mean_bench_dreb,starterbench_dreb_effect,mean_starterbench_dreb,prev_dreb,prev_dreb_delta,prev2_dreb,prev2_dreb_delta,prev5_dreb,prev5_dreb_delta,o_dreb_pct_delta,expected_DREB,expected_DREB_effect,scaled_dreb_pct_effect, scaled_dreb_effect,modded_dreb, modded_dreb_effect,opp_average_v_position_DREB_mean,opp_average_v_position_DREB_PCT,opp_average_DREB_PCT,oa_misses,oa_3p_misses,oa_2p_misses,oa_ft_misses,opp_average_FT_PCT,opp_average_FG_PCT,opp_average_FG3_PCT,opp_average_FG2_PCT,expected_DREB_pace, dreb_pace_effect, expected_DREB_pace2, dreb_pace2_effect, expected_DREB_pace3, dreb_pace3_effect,expected_DREB_def_rtg, def_rtg_DREB_effect, expected_DREB_off_rtg, off_rtg_DREB_effect, expected_DREB_opp_DREB, expected_DREB_opp_DREB_effect,vegas_ratio_dreb, vegas_ratio_dreb_effect, vegas_ratio_dreb_pinnacle, vegas_ratio_dreb_pinnacle_effect, vegas_ratio_dreb_opp_pinnacle,vegas_ratio_dreb_opp_pinnacle_effect, vegas_ratio_dreb_ou_pinnacle, vegas_ratio_dreb_ou_pinnacle_effect, actual_DREB )
  r[:DREB_mean] = dreb_mean
  r[:DREB_PCT] = dreb_PCT
  r[:opp_average_OREB_PCT] = opp_average_OREB_PCT
  r[:opp_average_OREB_mean] = opp_average_OREB_mean
                              
  r[:o_dreb_delta] = o_dreb_delta 
  r[:location_dreb_effect] = location_dreb_effect
  r[:rest_dreb_effect] = rest_dreb_effect
  r[:location_dreb] = location_dreb
  r[:rest_dreb] = rest_dreb
  r[:opp_rest_dreb] = opp_rest_dreb
                              
  r[:mean_starter_dreb] = mean_starter_dreb
  r[:mean_bench_dreb] = mean_bench_dreb
  r[:starterbench_dreb_effect] = starterbench_dreb_effect
  r[:mean_starterbench_dreb] = mean_starterbench_dreb
  r[:prev_dreb] = prev_dreb
  r[:prev_dreb_delta] = prev_dreb_delta
  r[:prev2_dreb] = prev2_dreb
                              
  r[:prev2_dreb_delta] = prev2_dreb_delta
  r[:prev5_dreb] = prev5_dreb
  r[:prev5_dreb_delta] = prev5_dreb_delta
  r[:o_dreb_pct_delta] = o_dreb_pct_delta
  r[:expected_DREB] = expected_DREB
  r[:expected_DREB_effect] = expected_DREB_effect
  r[:scaled_dreb_pct_effect] = scaled_dreb_pct_effect 
  r[:scaled_dreb_effect] = scaled_dreb_effect
                              
  r[:modded_dreb] = modded_dreb 
  r[:modded_dreb_effect] = modded_dreb_effect
  r[:opp_average_v_position_DREB_mean] = opp_average_v_position_DREB_mean
  r[:opp_average_v_position_DREB_PCT] = opp_average_v_position_DREB_PCT
  r[:opp_average_DREB_PCT] = opp_average_DREB_PCT
                              
  r[:oa_misses] = oa_misses
  r[:oa_3p_misses] = oa_3p_misses
  r[:oa_2p_misses] = oa_2p_misses
  r[:oa_ft_misses] = oa_ft_misses
  r[:opp_average_FT_PCT] = opp_average_FT_PCT
  r[:opp_average_FG_PCT] = opp_average_FG_PCT
  r[:opp_average_FG3_PCT] = opp_average_FG3_PCT
                              
  r[:opp_average_FG2_PCT] = opp_average_FG2_PCT
  r[:expected_DREB_pace] = expected_DREB_pace 
  r[:dreb_pace_effect] = dreb_pace_effect 
  r[:expected_DREB_pace2] = expected_DREB_pace2 
  r[:dreb_pace2_effect] = dreb_pace2_effect 
  r[:expected_DREB_pace3] = expected_DREB_pace3 
  r[:dreb_pace3_effect] = dreb_pace3_effect 
                              
  r[:expected_DREB_def_rtg] = expected_DREB_def_rtg 
  r[:def_rtg_DREB_effect] = def_rtg_DREB_effect 
  r[:expected_DREB_off_rtg] = expected_DREB_off_rtg 
  r[:off_rtg_DREB_effect] = off_rtg_DREB_effect 
  r[:expected_DREB_opp_DREB] = expected_DREB_opp_DREB 
  r[:expected_DREB_opp_DREB_effect] = expected_DREB_opp_DREB_effect 
                              
  r[:vegas_ratio_dreb] = vegas_ratio_dreb 
  r[:vegas_ratio_dreb_effect] = vegas_ratio_dreb_effect 
  r[:vegas_ratio_dreb_pinnacle] = vegas_ratio_dreb_pinnacle 
  r[:vegas_ratio_dreb_pinnacle_effect] = vegas_ratio_dreb_pinnacle_effect 
  r[:vegas_ratio_dreb_opp_pinnacle] = vegas_ratio_dreb_opp_pinnacle 
                              
  r[:vegas_ratio_dreb_opp_pinnacle_effect] = vegas_ratio_dreb_opp_pinnacle_effect 
  r[:vegas_ratio_dreb_ou_pinnacle] = vegas_ratio_dreb_ou_pinnacle 
  r[:vegas_ratio_dreb_ou_pinnacle_effect] = vegas_ratio_dreb_ou_pinnacle_effect 
  r[:actual_DREB] = actual_DREB
end

def populateDrebsPerMinFeatures( r, mean_dreb_per_min, dreb_PCT, opp_average_OREB_PCT, opp_average_oreb_per_min,o_dreb_delta_per_min, location_dreb_effect_per_min,rest_dreb_effect_per_min,location_dreb_per_min,rest_dreb_per_min,opp_rest_dreb_per_min,mean_starter_dreb_per_min,mean_bench_dreb_per_min,starterbench_dreb_effect_per_min,mean_starterbench_dreb_per_min,prev_dreb_per_min,prev_dreb_delta_per_min, prev2_dreb_per_min, prev2_dreb_delta_per_min, prev5_dreb_per_min, prev5_dreb_delta_per_min,o_dreb_pct_delta,expected_dreb_per_min,expected_dreb_effect_per_min,scaled_dreb_pct,scaled_dreb_pct_effect, scaled_dreb_per_min, scaled_dreb_effect_per_min,modded_dreb_per_min, modded_dreb_effect_per_min,opp_average_dreb_per_min, opp_average_v_position_DREB_PCT, opp_average_DREB_PCT,team_misses_per_min, team_3p_misses_per_min, team_2p_misses_per_min, team_ft_misses_per_min, opp_average_FT_PCT,opp_average_FG_PCT, opp_average_FG3_PCT, opp_average_FG2_PCT, expected_DREB_pace_per_min, dreb_pace_effect_per_min,expected_DREB_pace2_per_min, dreb_pace2_effect_per_min, expected_DREB_pace3_per_min, dreb_pace3_effect_per_min, expected_DREB_def_rtg_per_min,def_rtg_DREB_effect_per_min, expected_DREB_off_rtg_per_min, off_rtg_DREB_effect_per_min, expected_DREB_opp_DREB_per_min, expected_DREB_opp_DREB_effect_per_min,vegas_ratio_dreb_per_min, vegas_ratio_dreb_effect_per_min, vegas_ratio_dreb_pinnacle_per_min, vegas_ratio_dreb_pinnacle_effect_per_min,vegas_ratio_dreb_opp_pinnacle_per_min, vegas_ratio_dreb_opp_pinnacle_effect_per_min, vegas_ratio_dreb_ou_pinnacle_per_min, vegas_ratio_dreb_ou_pinnacle_effect_per_min,actual_dreb_per_min )

  r[:mean_dreb_per_min] = mean_dreb_per_min
  r[:DREB_PCT] = dreb_PCT
  r[:opp_average_OREB_PCT] = opp_average_OREB_PCT
  r[:opp_average_oreb_per_min] = opp_average_oreb_per_min
                                      
  r[:o_dreb_delta_per_min] = o_dreb_delta_per_min 
  r[:location_dreb_effect_per_min] = location_dreb_effect_per_min
  r[:rest_dreb_effect_per_min] = rest_dreb_effect_per_min
                                      
  r[:location_dreb_per_min] = location_dreb_per_min
  r[:rest_dreb_per_min] = rest_dreb_per_min
  r[:opp_rest_dreb_per_min] = opp_rest_dreb_per_min
  r[:mean_starter_dreb_per_min] = mean_starter_dreb_per_min
  r[:mean_bench_dreb_per_min] = mean_bench_dreb_per_min
  r[:starterbench_dreb_effect_per_min] = starterbench_dreb_effect_per_min
                                      
  r[:mean_starterbench_dreb_per_min] = mean_starterbench_dreb_per_min
  r[:prev_dreb_per_min] = prev_dreb_per_min
  r[:prev_dreb_delta_per_min] = prev_dreb_delta_per_min 
  r[:prev2_dreb_per_min] = prev2_dreb_per_min 
  r[:prev2_dreb_delta_per_min] = prev2_dreb_delta_per_min 
  r[:prev5_dreb_per_min] = prev5_dreb_per_min 
  r[:prev5_dreb_delta_per_min] = prev5_dreb_delta_per_min 
                                      
  r[:o_dreb_pct_delta] = o_dreb_pct_delta
  r[:expected_dreb_per_min] = expected_dreb_per_min
  r[:expected_dreb_effect_per_min] = expected_dreb_effect_per_min
  r[:scaled_dreb_pct] = scaled_dreb_pct
  r[:scaled_dreb_pct_effect] = scaled_dreb_pct_effect 
  r[:scaled_dreb_per_min] = scaled_dreb_per_min 
  r[:scaled_dreb_effect_per_min] = scaled_dreb_effect_per_min 
                                      
  r[:modded_dreb_per_min] = modded_dreb_per_min 
  r[:modded_dreb_effect_per_min] = modded_dreb_effect_per_min
  r[:opp_average_dreb_per_min] = opp_average_dreb_per_min 
  r[:opp_average_v_position_DREB_PCT] = opp_average_v_position_DREB_PCT
  r[:opp_average_DREB_PCT] = opp_average_DREB_PCT
                                      
  r[:team_misses_per_min] = team_misses_per_min 
  r[:team_3p_misses_per_min] = team_3p_misses_per_min 
  r[:team_2p_misses_per_min] = team_2p_misses_per_min 
  r[:team_ft_misses_per_min] = team_ft_misses_per_min 
  r[:opp_average_FT_PCT] = opp_average_FT_PCT
                                      
  r[:opp_average_FG_PCT] = opp_average_FG_PCT
  r[:opp_average_FG3_PCT] = opp_average_FG3_PCT
  r[:opp_average_FG2_PCT] = opp_average_FG2_PCT
  r[:expected_DREB_pace_per_min] = expected_DREB_pace_per_min 
  r[:dreb_pace_effect_per_min] = dreb_pace_effect_per_min 
                                      
  r[:expected_DREB_pace2_per_min] = expected_DREB_pace2_per_min 
  r[:dreb_pace2_effect_per_min] = dreb_pace2_effect_per_min 
  r[:expected_DREB_pace3_per_min] = expected_DREB_pace3_per_min 
  r[:dreb_pace3_effect_per_min] = dreb_pace3_effect_per_min 
  r[:expected_DREB_def_rtg_per_min] = expected_DREB_def_rtg_per_min 
                                      
  r[:def_rtg_DREB_effect_per_min] = def_rtg_DREB_effect_per_min 
  r[:expected_DREB_off_rtg_per_min] = expected_DREB_off_rtg_per_min 
  r[:off_rtg_DREB_effect_per_min] = off_rtg_DREB_effect_per_min 
  r[:expected_DREB_opp_DREB_per_min] = expected_DREB_opp_DREB_per_min 
  r[:expected_DREB_opp_DREB_effect_per_min] = expected_DREB_opp_DREB_effect_per_min 
                                      
  r[:vegas_ratio_dreb_per_min] = vegas_ratio_dreb_per_min 
  r[:vegas_ratio_dreb_effect_per_min] = vegas_ratio_dreb_effect_per_min 
  r[:vegas_ratio_dreb_pinnacle_per_min] = vegas_ratio_dreb_pinnacle_per_min 
  r[:vegas_ratio_dreb_pinnacle_effect_per_min] = vegas_ratio_dreb_pinnacle_effect_per_min 
                                      
  r[:vegas_ratio_dreb_opp_pinnacle_per_min] = vegas_ratio_dreb_opp_pinnacle_per_min 
  r[:vegas_ratio_dreb_opp_pinnacle_effect_per_min] = vegas_ratio_dreb_opp_pinnacle_effect_per_min 
  r[:vegas_ratio_dreb_ou_pinnacle_per_min] = vegas_ratio_dreb_ou_pinnacle_per_min 
  r[:vegas_ratio_dreb_ou_pinnacle_effect_per_min] = vegas_ratio_dreb_ou_pinnacle_effect_per_min 
                                      
  r[:actual_dreb_per_min] = actual_dreb_per_min
end

def populateAstFeatures( r, ast_mean, ast_PCT,team_average_o_AST_PCT,o_ast_delta,location_ast,location_ast_effect,rest_ast,rest_ast_effect,opp_rest_ast,opp_rest_ast_effect,starter,mean_starter_ast,mean_bench_ast,starterbench_ast_effect,mean_starterbench_ast,prev_ast,prev_ast_delta,prev2_ast,prev2_ast_delta,prev5_ast,prev5_ast_delta,o_ast_pct_delta,expected_AST,expected_AST_effect,scaled_assist_pct_effect, scaled_assist,scaled_assist_effect,modded_assist,modded_assist_effect,expected_AST_pace, ast_pace_effect, expected_AST_pace2, ast_pace2_effect, expected_AST_pace3, ast_pace3_effect, expected_AST_def_rtg,def_rtg_AST_effect, expected_AST_off_rtg, off_rtg_AST_effect, expected_AST_opp_AST, expected_AST_opp_AST_effect, vegas_ratio_ast,vegas_ratio_ast_effect, vegas_ratio_ast_pinnacle, vegas_ratio_ast_pinnacle_effect, vegas_ratio_ast_opp_pinnacle, vegas_ratio_ast_opp_pinnacle_effect,vegas_ratio_ast_ou_pinnacle, vegas_ratio_ast_ou_pinnacle_effect, actual_AST )

  r[:AST_mean] = ast_mean
  r[:AST_PCT] = ast_PCT
  r[:team_average_o_AST_PCT] = team_average_o_AST_PCT
  r[:o_ast_delta] = o_ast_delta 
  r[:location_ast] = location_ast
  r[:location_ast_effect] = location_ast_effect
  r[:rest_ast] = rest_ast
  r[:rest_ast_effect] = rest_ast_effect
  r[:opp_rest_ast] = opp_rest_ast
  r[:opp_rest_ast_effect] = opp_rest_ast_effect
                                
  r[:mean_starter_ast] = mean_starter_ast
  r[:mean_bench_ast] = mean_bench_ast
  r[:starterbench_ast_effect] = starterbench_ast_effect
  r[:mean_starterbench_ast] = mean_starterbench_ast
  r[:prev_ast] = prev_ast
  r[:prev_ast_delta] = prev_ast_delta
  r[:prev2_ast] = prev2_ast
  r[:prev2_ast_delta] = prev2_ast_delta
                                
  r[:prev5_ast] = prev5_ast
  r[:prev5_ast_delta] = prev5_ast_delta
  r[:o_ast_pct_delta] = o_ast_pct_delta
  r[:expected_AST] = expected_AST
  r[:expected_AST_effect] = expected_AST_effect
  r[:scaled_assist_pct_effect] = scaled_assist_pct_effect 
  r[:scaled_assist] = scaled_assist
  r[:scaled_assist_effect] = scaled_assist_effect
  r[:modded_assist] = modded_assist
  r[:modded_assist_effect] = modded_assist_effect
                                
  r[:expected_AST_pace] = expected_AST_pace 
  r[:ast_pace_effect] = ast_pace_effect 
  r[:expected_AST_pace2] = expected_AST_pace2 
  r[:ast_pace2_effect] = ast_pace2_effect 
  r[:expected_AST_pace3] = expected_AST_pace3 
  r[:ast_pace3_effect] = ast_pace3_effect 
  r[:expected_AST_def_rtg] = expected_AST_def_rtg 
                                
  r[:def_rtg_AST_effect] = def_rtg_AST_effect 
  r[:expected_AST_off_rtg] = expected_AST_off_rtg 
  r[:off_rtg_AST_effect] = off_rtg_AST_effect 
  r[:expected_AST_opp_AST] = expected_AST_opp_AST 
  r[:expected_AST_opp_AST_effect] = expected_AST_opp_AST_effect 
  r[:vegas_ratio_ast] = vegas_ratio_ast 
                                
  r[:vegas_ratio_ast_effect] = vegas_ratio_ast_effect 
  r[:vegas_ratio_ast_pinnacle] = vegas_ratio_ast_pinnacle 
  r[:vegas_ratio_ast_pinnacle_effect] = vegas_ratio_ast_pinnacle_effect 
  r[:vegas_ratio_ast_opp_pinnacle] = vegas_ratio_ast_opp_pinnacle 
  r[:vegas_ratio_ast_opp_pinnacle_effect] = vegas_ratio_ast_opp_pinnacle_effect 
                                
  r[:vegas_ratio_ast_ou_pinnacle] = vegas_ratio_ast_ou_pinnacle 
  r[:vegas_ratio_ast_ou_pinnacle_effect] = vegas_ratio_ast_ou_pinnacle_effect 
  r[:actual_AST] = actual_AST
end

def populateAstPerMinFeatures( r, mean_ast_per_min, ast_PCT,team_average_o_AST_PCT,o_ast_delta_per_min,location_ast_per_min,location_ast_effect_per_min,rest_ast_per_min,rest_ast_effect_per_min,opp_rest_ast_per_min,opp_rest_ast_effect_per_min,mean_starter_ast_per_min,mean_bench_ast_per_min,starterbench_ast_effect_per_min,mean_starterbench_ast_per_min,prev_ast_per_min,prev_ast_delta_per_min,prev2_ast_per_min,prev2_ast_delta_per_min,prev5_ast_per_min,prev5_ast_delta_per_min,o_ast_pct_delta,expected_AST_per_min,expected_AST_effect_per_min,scaled_assist_pct_effect, scaled_assist_per_min,scaled_assist_effect_per_min,modded_assist_per_min,modded_assist_effect_per_min,expected_AST_pace_per_min, ast_pace_effect_per_min,expected_AST_pace2_per_min, ast_pace2_effect_per_min, expected_AST_pace3_per_min, ast_pace3_effect_per_min, expected_AST_def_rtg_per_min,def_rtg_AST_effect_per_min, expected_AST_off_rtg_per_min, off_rtg_AST_effect_per_min, expected_AST_opp_AST_per_min, expected_AST_opp_AST_effect_per_min,vegas_ratio_ast_per_min, vegas_ratio_ast_effect_per_min, vegas_ratio_ast_pinnacle_per_min, vegas_ratio_ast_pinnacle_effect_per_min,vegas_ratio_ast_opp_pinnacle_per_min, vegas_ratio_ast_opp_pinnacle_effect_per_min, vegas_ratio_ast_ou_pinnacle_per_min, vegas_ratio_ast_ou_pinnacle_effect_per_min,actual_ast_per_min )

  r[:mean_ast_per_min] = mean_ast_per_min
  r[:AST_PCT] = ast_PCT
  r[:team_average_o_AST_PCT] = team_average_o_AST_PCT
  r[:o_ast_delta_per_min] = o_ast_delta_per_min 
  r[:location_ast_per_min] = location_ast_per_min
  r[:location_ast_effect_per_min] = location_ast_effect_per_min
  r[:rest_ast_per_min] = rest_ast_per_min
  r[:rest_ast_effect_per_min] = rest_ast_effect_per_min
  r[:opp_rest_ast_per_min] = opp_rest_ast_per_min
  r[:opp_rest_ast_effect_per_min] = opp_rest_ast_effect_per_min
  r[:mean_starter_ast_per_min] = mean_starter_ast_per_min
  r[:mean_bench_ast_per_min] = mean_bench_ast_per_min
  r[:starterbench_ast_effect_per_min] = starterbench_ast_effect_per_min
  r[:mean_starterbench_ast_per_min] = mean_starterbench_ast_per_min
  r[:prev_ast_per_min] = prev_ast_per_min
  r[:prev_ast_delta_per_min] = prev_ast_delta_per_min
  r[:prev2_ast_per_min] = prev2_ast_per_min
  r[:prev2_ast_delta_per_min] = prev2_ast_delta_per_min
  r[:prev5_ast_per_min] = prev5_ast_per_min
  r[:prev5_ast_delta_per_min] = prev5_ast_delta_per_min
  r[:o_ast_pct_delta] = o_ast_pct_delta
  r[:expected_AST_per_min] = expected_AST_per_min
  r[:expected_AST_effect_per_min] = expected_AST_effect_per_min
  r[:scaled_assist_pct_effect] = scaled_assist_pct_effect 
  r[:scaled_assist_per_min] = scaled_assist_per_min
  r[:scaled_assist_effect_per_min] = scaled_assist_effect_per_min
  r[:modded_assist_per_min] = modded_assist_per_min
  r[:modded_assist_effect_per_min] = modded_assist_effect_per_min
  r[:expected_AST_pace_per_min] = expected_AST_pace_per_min 
  r[:ast_pace_effect_per_min] = ast_pace_effect_per_min 
  r[:expected_AST_pace2_per_min] = expected_AST_pace2_per_min 
  r[:ast_pace2_effect_per_min] = ast_pace2_effect_per_min 
  r[:expected_AST_pace3_per_min] = expected_AST_pace3_per_min 
  r[:ast_pace3_effect_per_min] = ast_pace3_effect_per_min 
  r[:expected_AST_def_rtg_per_min] = expected_AST_def_rtg_per_min 
  r[:def_rtg_AST_effect_per_min] = def_rtg_AST_effect_per_min 
  r[:expected_AST_off_rtg_per_min] = expected_AST_off_rtg_per_min 
  r[:off_rtg_AST_effect_per_min] = off_rtg_AST_effect_per_min 
  r[:expected_AST_opp_AST_per_min] = expected_AST_opp_AST_per_min 
  r[:expected_AST_opp_AST_effect_per_min] = expected_AST_opp_AST_effect_per_min 
  r[:vegas_ratio_ast_per_min] = vegas_ratio_ast_per_min 
  r[:vegas_ratio_ast_effect_per_min] = vegas_ratio_ast_effect_per_min 
  r[:vegas_ratio_ast_pinnacle_per_min] = vegas_ratio_ast_pinnacle_per_min 
  r[:vegas_ratio_ast_pinnacle_effect_per_min] = vegas_ratio_ast_pinnacle_effect_per_min 
  r[:vegas_ratio_ast_opp_pinnacle_per_min] = vegas_ratio_ast_opp_pinnacle_per_min 
  r[:vegas_ratio_ast_opp_pinnacle_effect_per_min] = vegas_ratio_ast_opp_pinnacle_effect_per_min 
  r[:vegas_ratio_ast_ou_pinnacle_per_min] = vegas_ratio_ast_ou_pinnacle_per_min 
  r[:vegas_ratio_ast_ou_pinnacle_effect_per_min] = vegas_ratio_ast_ou_pinnacle_effect_per_min 
  r[:actual_ast_per_min] = actual_ast_per_min
end

def populateTOVFeatures( r, tov_mean, to_PCT,team_average_o_TO_PCT,o_tov_delta,location_tov_effect,rest_tov_effect,location_tov,rest_tov,opp_rest_tov,opp_rest_tov_effect,mean_starter_tov,mean_bench_tov,starterbench_tov_effect,mean_starterbench_tov,prev_tov,prev_tov_delta,prev2_tov,prev2_tov_delta,prev5_tov,prev5_tov_delta,o_tov_pct_delta,expected_TOV,expected_TOV_effect,scaled_turnover,scaled_turnover_pct_effect, scaled_turnover_effect,modded_turnover,modded_turnover_effect,expected_TOV_pace, tov_pace_effect, expected_TOV_pace2, tov_pace2_effect, expected_TOV_pace3,tov_pace3_effect, expected_TOV_def_rtg, def_rtg_TOV_effect, expected_TOV_off_rtg, off_rtg_TOV_effect, expected_TOV_opp_TOV, expected_TOV_opp_TOV_effect,vegas_ratio_tov, vegas_ratio_tov_effect, vegas_ratio_tov_pinnacle, vegas_ratio_tov_pinnacle_effect, vegas_ratio_tov_opp_pinnacle,vegas_ratio_tov_opp_pinnacle_effect, vegas_ratio_tov_ou_pinnacle, vegas_ratio_tov_ou_pinnacle_effect, actual_TOV )
  r[:TOV_mean] = tov_mean
  r[:TO_PCT] = to_PCT
  r[:team_average_o_TO_PCT] = team_average_o_TO_PCT
  r[:o_tov_delta] = o_tov_delta 
  r[:location_tov_effect] = location_tov_effect
  r[:rest_tov_effect] = rest_tov_effect
  r[:location_tov] = location_tov
  r[:rest_tov] = rest_tov
  r[:opp_rest_tov] = opp_rest_tov
  r[:opp_rest_tov_effect] = opp_rest_tov_effect
  r[:mean_starter_tov] = mean_starter_tov
  r[:mean_bench_tov] = mean_bench_tov
  r[:starterbench_tov_effect] = starterbench_tov_effect
  r[:mean_starterbench_tov] = mean_starterbench_tov
  r[:prev_tov] = prev_tov
  r[:prev_tov_delta] = prev_tov_delta
  r[:prev2_tov] = prev2_tov
  r[:prev2_tov_delta] = prev2_tov_delta
  r[:prev5_tov] = prev5_tov
  r[:prev5_tov_delta] = prev5_tov_delta
  r[:o_tov_pct_delta] = o_tov_pct_delta
  r[:expected_TOV] = expected_TOV
  r[:expected_TOV_effect] = expected_TOV_effect
  r[:scaled_turnover] = scaled_turnover
  r[:scaled_turnover_pct_effect] = scaled_turnover_pct_effect 
  r[:scaled_turnover_effect] = scaled_turnover_effect
  r[:modded_turnover] = modded_turnover
  r[:modded_turnover_effect] = modded_turnover_effect
  r[:expected_TOV_pace] = expected_TOV_pace 
  r[:tov_pace_effect] = tov_pace_effect 
  r[:expected_TOV_pace2] = expected_TOV_pace2 
  r[:tov_pace2_effect] = tov_pace2_effect 
  r[:expected_TOV_pace3] = expected_TOV_pace3 
  r[:tov_pace3_effect] = tov_pace3_effect 
  r[:expected_TOV_def_rtg] = expected_TOV_def_rtg 
  r[:def_rtg_TOV_effect] = def_rtg_TOV_effect 
  r[:expected_TOV_off_rtg] = expected_TOV_off_rtg 
  r[:off_rtg_TOV_effect] = off_rtg_TOV_effect 
  r[:expected_TOV_opp_TOV] = expected_TOV_opp_TOV 
  r[:expected_TOV_opp_TOV_effect] = expected_TOV_opp_TOV_effect 
  r[:vegas_ratio_tov] = vegas_ratio_tov 
  r[:vegas_ratio_tov_effect] = vegas_ratio_tov_effect 
  r[:vegas_ratio_tov_pinnacle] = vegas_ratio_tov_pinnacle 
  r[:vegas_ratio_tov_pinnacle_effect] = vegas_ratio_tov_pinnacle_effect 
  r[:vegas_ratio_tov_opp_pinnacle] = vegas_ratio_tov_opp_pinnacle 
  r[:vegas_ratio_tov_opp_pinnacle_effect] = vegas_ratio_tov_opp_pinnacle_effect 
  r[:vegas_ratio_tov_ou_pinnacle] = vegas_ratio_tov_ou_pinnacle 
  r[:vegas_ratio_tov_ou_pinnacle_effect] = vegas_ratio_tov_ou_pinnacle_effect 
  r[:actual_TOV] = actual_TOV
end

def populateTOVPerMinFeatures( r, mean_tov_per_min, to_PCT,team_average_o_TO_PCT,o_tov_delta_per_min,location_tov_effect_per_min,rest_tov_effect_per_min,location_tov_per_min,rest_tov_per_min,opp_rest_tov_per_min,opp_rest_tov_effect_per_min,mean_starter_tov_per_min,mean_bench_tov_per_min,starterbench_tov_effect_per_min,mean_starterbench_tov_per_min,prev_tov_per_min,prev_tov_delta_per_min,prev2_tov_per_min,prev2_tov_delta_per_min,prev5_tov_per_min,prev5_tov_delta_per_min,o_tov_pct_delta,expected_TOV_per_min,expected_TOV_effect_per_min,scaled_turnover_per_min,scaled_turnover_pct_effect, scaled_turnover_effect_per_min,modded_turnover_per_min,modded_turnover_effect_per_min,expected_TOV_pace_per_min, tov_pace_effect_per_min, expected_TOV_pace2_per_min,tov_pace2_effect_per_min, expected_TOV_pace3_per_min, tov_pace3_effect_per_min, expected_TOV_def_rtg_per_min, def_rtg_TOV_effect_per_min,expected_TOV_off_rtg_per_min, off_rtg_TOV_effect_per_min, expected_TOV_opp_TOV_per_min, expected_TOV_opp_TOV_effect_per_min, vegas_ratio_tov_per_min,vegas_ratio_tov_effect_per_min, vegas_ratio_tov_pinnacle_per_min, vegas_ratio_tov_pinnacle_effect_per_min, vegas_ratio_tov_opp_pinnacle_per_min,vegas_ratio_tov_opp_pinnacle_effect_per_min, vegas_ratio_tov_ou_pinnacle_per_min, vegas_ratio_tov_ou_pinnacle_effect_per_min, actual_tov_per_min )
  r[:mean_tov_per_min] = mean_tov_per_min
  r[:TO_PCT] = to_PCT
  r[:team_average_o_TO_PCT] = team_average_o_TO_PCT
  r[:o_tov_delta_per_min] = o_tov_delta_per_min 
  r[:location_tov_effect_per_min] = location_tov_effect_per_min
  r[:rest_tov_effect_per_min] = rest_tov_effect_per_min
  r[:location_tov_per_min] = location_tov_per_min
  r[:rest_tov_per_min] = rest_tov_per_min
  r[:opp_rest_tov_per_min] = opp_rest_tov_per_min
  r[:opp_rest_tov_effect_per_min] = opp_rest_tov_effect_per_min
  r[:mean_starter_tov_per_min] = mean_starter_tov_per_min
  r[:mean_bench_tov_per_min] = mean_bench_tov_per_min
  r[:starterbench_tov_effect_per_min] = starterbench_tov_effect_per_min
  r[:mean_starterbench_tov_per_min] = mean_starterbench_tov_per_min
  r[:prev_tov_per_min] = prev_tov_per_min
  r[:prev_tov_delta_per_min] = prev_tov_delta_per_min
  r[:prev2_tov_per_min] = prev2_tov_per_min
  r[:prev2_tov_delta_per_min] = prev2_tov_delta_per_min
  r[:prev5_tov_per_min] = prev5_tov_per_min
  r[:prev5_tov_delta_per_min] = prev5_tov_delta_per_min
  r[:o_tov_pct_delta] = o_tov_pct_delta
  r[:expected_TOV_per_min] = expected_TOV_per_min
  r[:expected_TOV_effect_per_min] = expected_TOV_effect_per_min
  r[:scaled_turnover_per_min] = scaled_turnover_per_min
  r[:scaled_turnover_pct_effect] = scaled_turnover_pct_effect 
  r[:scaled_turnover_effect_per_min] = scaled_turnover_effect_per_min
  r[:modded_turnover_per_min] = modded_turnover_per_min
  r[:modded_turnover_effect_per_min] = modded_turnover_effect_per_min
  r[:expected_TOV_pace_per_min] = expected_TOV_pace_per_min 
  r[:tov_pace_effect_per_min] = tov_pace_effect_per_min 
  r[:expected_TOV_pace2_per_min] = expected_TOV_pace2_per_min 
  r[:tov_pace2_effect_per_min] = tov_pace2_effect_per_min 
  r[:expected_TOV_pace3_per_min] = expected_TOV_pace3_per_min 
  r[:tov_pace3_effect_per_min] = tov_pace3_effect_per_min 
  r[:expected_TOV_def_rtg_per_min] = expected_TOV_def_rtg_per_min 
  r[:def_rtg_TOV_effect_per_min] = def_rtg_TOV_effect_per_min 
  r[:expected_TOV_off_rtg_per_min] = expected_TOV_off_rtg_per_min 
  r[:off_rtg_TOV_effect_per_min] = off_rtg_TOV_effect_per_min 
  r[:expected_TOV_opp_TOV_per_min] = expected_TOV_opp_TOV_per_min 
  r[:expected_TOV_opp_TOV_effect_per_min] = expected_TOV_opp_TOV_effect_per_min 
  r[:vegas_ratio_tov_per_min] = vegas_ratio_tov_per_min 
  r[:vegas_ratio_tov_effect_per_min] = vegas_ratio_tov_effect_per_min 
  r[:vegas_ratio_tov_pinnacle_per_min] = vegas_ratio_tov_pinnacle_per_min 
  r[:vegas_ratio_tov_pinnacle_effect_per_min] = vegas_ratio_tov_pinnacle_effect_per_min 
  r[:vegas_ratio_tov_opp_pinnacle_per_min] = vegas_ratio_tov_opp_pinnacle_per_min 
  r[:vegas_ratio_tov_opp_pinnacle_effect_per_min] = vegas_ratio_tov_opp_pinnacle_effect_per_min 
  r[:vegas_ratio_tov_ou_pinnacle_per_min] = vegas_ratio_tov_ou_pinnacle_per_min 
  r[:vegas_ratio_tov_ou_pinnacle_effect_per_min] = vegas_ratio_tov_ou_pinnacle_effect_per_min 
  r[:actual_tov_per_min] = actual_tov_per_min
end

def populateBlocksFeatures( r, blk_mean, pct_BLK,team_average_o_PCT_BLK,o_blk_delta,location_blk_effect,rest_blk_effect,location_blk,rest_blk,opp_rest_blk,opp_rest_blk_effect,mean_starter_blk,mean_bench_blk,starterbench_blk_effect,mean_starterbench_blk,prev_blk,prev_blk_delta,prev2_blk,prev2_blk_delta,prev5_blk,prev5_blk_delta,o_blk_pct_delta,expected_BLK,expected_BLK_effect,scaled_block_pct_effect, scaled_block,scaled_block_effect,modded_block,modded_block_effect,expected_BLK_pace, blk_pace_effect, expected_BLK_pace2, blk_pace2_effect, expected_BLK_pace3,blk_pace3_effect, expected_BLK_def_rtg, def_rtg_BLK_effect, expected_BLK_off_rtg, off_rtg_BLK_effect, expected_BLK_opp_BLK,expected_BLK_opp_BLK_effect, vegas_ratio_blk, vegas_ratio_blk_effect, vegas_ratio_blk_pinnacle, vegas_ratio_blk_pinnacle_effect,vegas_ratio_blk_opp_pinnacle, vegas_ratio_blk_opp_pinnacle_effect, vegas_ratio_blk_ou_pinnacle, vegas_ratio_blk_ou_pinnacle_effect,actual_BLK )

  r[:BLK_mean] = blk_mean
  r[:PCT_BLK] = pct_BLK
  r[:team_average_o_PCT_BLK] = team_average_o_PCT_BLK
  r[:o_blk_delta] = o_blk_delta 
  r[:location_blk_effect] = location_blk_effect
  r[:rest_blk_effect] = rest_blk_effect
  r[:location_blk] = location_blk
  r[:rest_blk] = rest_blk
  r[:opp_rest_blk] = opp_rest_blk
  r[:opp_rest_blk_effect] = opp_rest_blk_effect
  r[:mean_starter_blk] = mean_starter_blk
  r[:mean_bench_blk] = mean_bench_blk
  r[:starterbench_blk_effect] = starterbench_blk_effect
  r[:mean_starterbench_blk] = mean_starterbench_blk
  r[:prev_blk] = prev_blk
  r[:prev_blk_delta] = prev_blk_delta
  r[:prev2_blk] = prev2_blk
  r[:prev2_blk_delta] = prev2_blk_delta
  r[:prev5_blk] = prev5_blk
  r[:prev5_blk_delta] = prev5_blk_delta
  r[:o_blk_pct_delta] = o_blk_pct_delta
  r[:expected_BLK] = expected_BLK
  r[:expected_BLK_effect] = expected_BLK_effect
  r[:scaled_block_pct_effect] = scaled_block_pct_effect 
  r[:scaled_block] = scaled_block
  r[:scaled_block_effect] = scaled_block_effect
  r[:modded_block] = modded_block
  r[:modded_block_effect] = modded_block_effect
  r[:expected_BLK_pace] = expected_BLK_pace 
  r[:blk_pace_effect] = blk_pace_effect 
  r[:expected_BLK_pace2] = expected_BLK_pace2 
  r[:blk_pace2_effect] = blk_pace2_effect 
  r[:expected_BLK_pace3] = expected_BLK_pace3 
  r[:blk_pace3_effect] = blk_pace3_effect 
  r[:expected_BLK_def_rtg] = expected_BLK_def_rtg 
  r[:def_rtg_BLK_effect] = def_rtg_BLK_effect 
  r[:expected_BLK_off_rtg] = expected_BLK_off_rtg 
  r[:off_rtg_BLK_effect] = off_rtg_BLK_effect 
  r[:expected_BLK_opp_BLK] = expected_BLK_opp_BLK 
  r[:expected_BLK_opp_BLK_effect] = expected_BLK_opp_BLK_effect 
  r[:vegas_ratio_blk] = vegas_ratio_blk 
  r[:vegas_ratio_blk_effect] = vegas_ratio_blk_effect 
  r[:vegas_ratio_blk_pinnacle] = vegas_ratio_blk_pinnacle 
  r[:vegas_ratio_blk_pinnacle_effect] = vegas_ratio_blk_pinnacle_effect 
  r[:vegas_ratio_blk_opp_pinnacle] = vegas_ratio_blk_opp_pinnacle 
  r[:vegas_ratio_blk_opp_pinnacle_effect] = vegas_ratio_blk_opp_pinnacle_effect 
  r[:vegas_ratio_blk_ou_pinnacle] = vegas_ratio_blk_ou_pinnacle 
  r[:vegas_ratio_blk_ou_pinnacle_effect] = vegas_ratio_blk_ou_pinnacle_effect 
  r[:actual_BLK] = actual_BLK
end

def populateBlocksPerMinFeatures( r, mean_blk_per_min, pct_BLK,team_average_o_PCT_BLK,o_blk_delta_per_min,location_blk_effect_per_min,rest_blk_effect_per_min,location_blk_per_min,rest_blk_per_min,opp_rest_blk_per_min,opp_rest_blk_effect_per_min,mean_starter_blk_per_min,mean_bench_blk_per_min,starterbench_blk_effect_per_min,mean_starterbench_blk_per_min,prev_blk_per_min,prev_blk_delta_per_min,prev2_blk_per_min,prev2_blk_delta_per_min,prev5_blk_per_min,prev5_blk_delta_per_min,o_blk_pct_delta,expected_BLK_per_min,expected_BLK_effect_per_min,scaled_block_pct_effect_per_min,scaled_block_per_min,scaled_block_effect_per_min,modded_block_per_min,modded_block_effect_per_min,expected_BLK_pace_per_min, blk_pace_effect_per_min,expected_BLK_pace2_per_min, blk_pace2_effect_per_min, expected_BLK_pace3_per_min, blk_pace3_effect_per_min, expected_BLK_def_rtg_per_min,def_rtg_BLK_effect_per_min, expected_BLK_off_rtg_per_min, off_rtg_BLK_effect_per_min, expected_BLK_opp_BLK_per_min, expected_BLK_opp_BLK_effect_per_min,vegas_ratio_blk_per_min, vegas_ratio_blk_effect_per_min, vegas_ratio_blk_pinnacle_per_min, vegas_ratio_blk_pinnacle_effect_per_min,vegas_ratio_blk_opp_pinnacle_per_min, vegas_ratio_blk_opp_pinnacle_effect_per_min, vegas_ratio_blk_ou_pinnacle_per_min, vegas_ratio_blk_ou_pinnacle_effect_per_min,actual_blk_per_min )

  r[:mean_blk_per_min] = mean_blk_per_min
  r[:PCT_BLK] = pct_BLK
  r[:team_average_o_PCT_BLK] = team_average_o_PCT_BLK
  r[:o_blk_delta_per_min] = o_blk_delta_per_min 
  r[:location_blk_effect_per_min] = location_blk_effect_per_min
  r[:rest_blk_effect_per_min] = rest_blk_effect_per_min
  r[:location_blk_per_min] = location_blk_per_min
  r[:rest_blk_per_min] = rest_blk_per_min
  r[:opp_rest_blk_per_min] = opp_rest_blk_per_min
  r[:opp_rest_blk_effect_per_min] = opp_rest_blk_effect_per_min
  r[:mean_starter_blk_per_min] = mean_starter_blk_per_min
  r[:mean_bench_blk_per_min] = mean_bench_blk_per_min
  r[:starterbench_blk_effect_per_min] = starterbench_blk_effect_per_min
  r[:mean_starterbench_blk_per_min] = mean_starterbench_blk_per_min
  r[:prev_blk_per_min] = prev_blk_per_min
  r[:prev_blk_delta_per_min] = prev_blk_delta_per_min
  r[:prev2_blk_per_min] = prev2_blk_per_min
  r[:prev2_blk_delta_per_min] = prev2_blk_delta_per_min
  r[:prev5_blk_per_min] = prev5_blk_per_min
  r[:prev5_blk_delta_per_min] = prev5_blk_delta_per_min
  r[:o_blk_pct_delta] = o_blk_pct_delta
  r[:expected_BLK_per_min] = expected_BLK_per_min
  r[:expected_BLK_effect_per_min] = expected_BLK_effect_per_min
  r[:scaled_block_pct_effect_per_min] = scaled_block_pct_effect_per_min 
  r[:scaled_block_per_min] = scaled_block_per_min
  r[:scaled_block_effect_per_min] = scaled_block_effect_per_min
  r[:modded_block_per_min] = modded_block_per_min
  r[:modded_block_effect_per_min] = modded_block_effect_per_min
  r[:expected_BLK_pace_per_min] = expected_BLK_pace_per_min 
  r[:blk_pace_effect_per_min] = blk_pace_effect_per_min 
  r[:expected_BLK_pace2_per_min] = expected_BLK_pace2_per_min 
  r[:blk_pace2_effect_per_min] = blk_pace2_effect_per_min 
  r[:expected_BLK_pace3_per_min] = expected_BLK_pace3_per_min 
  r[:blk_pace3_effect_per_min] = blk_pace3_effect_per_min 
  r[:expected_BLK_def_rtg_per_min] = expected_BLK_def_rtg_per_min 
  r[:def_rtg_BLK_effect_per_min] = def_rtg_BLK_effect_per_min 
  r[:expected_BLK_off_rtg_per_min] = expected_BLK_off_rtg_per_min 
  r[:off_rtg_BLK_effect_per_min] = off_rtg_BLK_effect_per_min 
  r[:expected_BLK_opp_BLK_per_min] = expected_BLK_opp_BLK_per_min 
  r[:expected_BLK_opp_BLK_effect_per_min] = expected_BLK_opp_BLK_effect_per_min 
  r[:vegas_ratio_blk_per_min] = vegas_ratio_blk_per_min 
  r[:vegas_ratio_blk_effect_per_min] = vegas_ratio_blk_effect_per_min 
  r[:vegas_ratio_blk_pinnacle_per_min] = vegas_ratio_blk_pinnacle_per_min 
  r[:vegas_ratio_blk_pinnacle_effect_per_min] = vegas_ratio_blk_pinnacle_effect_per_min 
  r[:vegas_ratio_blk_opp_pinnacle_per_min] = vegas_ratio_blk_opp_pinnacle_per_min 
  r[:vegas_ratio_blk_opp_pinnacle_effect_per_min] = vegas_ratio_blk_opp_pinnacle_effect_per_min 
  r[:vegas_ratio_blk_ou_pinnacle_per_min] = vegas_ratio_blk_ou_pinnacle_per_min 
  r[:vegas_ratio_blk_ou_pinnacle_effect_per_min] = vegas_ratio_blk_ou_pinnacle_effect_per_min 
  r[:actual_blk_per_min] = actual_blk_per_min
end

def populateStealsFeatures( r, stl_mean, pct_STL,team_average_o_PCT_STL,o_stl_delta,location_stl_effect,rest_stl_effect,location_stl,rest_stl,opp_rest_stl,opp_rest_stl_effect,mean_starter_stl,mean_bench_stl,starterbench_stl_effect,mean_starterbench_stl,prev_stl,prev_stl_delta,prev2_stl,prev2_stl_delta,prev5_stl,prev5_stl_delta,o_stl_pct_delta,expected_STL,expected_STL_effect,scaled_steal,scaled_steal_effect,scaled_pct_stl_effect,modded_steal,modded_steal_effect,expected_STL_pace, stl_pace_effect, expected_STL_pace2, stl_pace2_effect,expected_STL_pace3, stl_pace3_effect, expected_STL_def_rtg, def_rtg_STL_effect, expected_STL_off_rtg, off_rtg_STL_effect,expected_STL_opp_STL, expected_STL_opp_STL_effect, vegas_ratio_stl, vegas_ratio_stl_effect, vegas_ratio_stl_pinnacle, vegas_ratio_stl_pinnacle_effect,vegas_ratio_stl_opp_pinnacle, vegas_ratio_stl_opp_pinnacle_effect, vegas_ratio_stl_ou_pinnacle, vegas_ratio_stl_ou_pinnacle_effect, actual_STL )

  r[:STL_mean] = stl_mean
  r[:PCT_STL] = pct_STL
  r[:team_average_o_PCT_STL] = team_average_o_PCT_STL
  r[:o_stl_delta] = o_stl_delta 
  r[:location_stl_effect] = location_stl_effect
  r[:rest_stl_effect] = rest_stl_effect
  r[:location_stl] = location_stl
  r[:rest_stl] = rest_stl
  r[:opp_rest_stl] = opp_rest_stl
  r[:opp_rest_stl_effect] = opp_rest_stl_effect
  r[:mean_starter_stl] = mean_starter_stl
  r[:mean_bench_stl] = mean_bench_stl
  r[:starterbench_stl_effect] = starterbench_stl_effect
  r[:mean_starterbench_stl] = mean_starterbench_stl
  r[:prev_stl] = prev_stl
  r[:prev_stl_delta] = prev_stl_delta
  r[:prev2_stl] = prev2_stl
  r[:prev2_stl_delta] = prev2_stl_delta
  r[:prev5_stl] = prev5_stl
  r[:prev5_stl_delta] = prev5_stl_delta
  r[:o_stl_pct_delta] = o_stl_pct_delta
  r[:expected_STL] = expected_STL
  r[:expected_STL_effect] = expected_STL_effect
  r[:scaled_steal] = scaled_steal
  r[:scaled_steal_effect] = scaled_steal_effect
  r[:scaled_pct_stl_effect] = scaled_pct_stl_effect
  r[:modded_steal] = modded_steal
  r[:modded_steal_effect] = modded_steal_effect
  r[:expected_STL_pace] = expected_STL_pace 
  r[:stl_pace_effect] = stl_pace_effect 
  r[:expected_STL_pace2] = expected_STL_pace2 
  r[:stl_pace2_effect] = stl_pace2_effect 
  r[:expected_STL_pace3] = expected_STL_pace3 
  r[:stl_pace3_effect] = stl_pace3_effect 
  r[:expected_STL_def_rtg] = expected_STL_def_rtg 
  r[:def_rtg_STL_effect] = def_rtg_STL_effect 
  r[:expected_STL_off_rtg] = expected_STL_off_rtg 
  r[:off_rtg_STL_effect] = off_rtg_STL_effect 
  r[:expected_STL_opp_STL] = expected_STL_opp_STL 
  r[:expected_STL_opp_STL_effect] = expected_STL_opp_STL_effect 
  r[:vegas_ratio_stl] = vegas_ratio_stl 
  r[:vegas_ratio_stl_effect] = vegas_ratio_stl_effect 
  r[:vegas_ratio_stl_pinnacle] = vegas_ratio_stl_pinnacle 
  r[:vegas_ratio_stl_pinnacle_effect] = vegas_ratio_stl_pinnacle_effect 
  r[:vegas_ratio_stl_opp_pinnacle] = vegas_ratio_stl_opp_pinnacle 
  r[:vegas_ratio_stl_opp_pinnacle_effect] = vegas_ratio_stl_opp_pinnacle_effect 
  r[:vegas_ratio_stl_ou_pinnacle] = vegas_ratio_stl_ou_pinnacle 
  r[:vegas_ratio_stl_ou_pinnacle_effect] = vegas_ratio_stl_ou_pinnacle_effect 
  r[:actual_STL] = actual_STL
end

def populateStealsPerMinFeatures( r, mean_stl_per_min, pct_STL,team_average_o_PCT_STL,o_stl_delta_per_min,location_stl_effect_per_min,rest_stl_effect_per_min,location_stl_per_min,rest_stl_per_min,opp_rest_stl_per_min,opp_rest_stl_effect_per_min,mean_starter_stl_per_min,mean_bench_stl_per_min,starterbench_stl_effect_per_min,mean_starterbench_stl_per_min,prev_stl_per_min,prev_stl_delta_per_min,prev2_stl_per_min,prev2_stl_delta_per_min,prev5_stl_per_min,prev5_stl_delta_per_min,o_stl_pct_delta,expected_STL_per_min,expected_STL_effect_per_min,scaled_steal_per_min,scaled_steal_effect_per_min,scaled_pct_stl_effect_per_min,modded_steal_per_min,modded_steal_effect_per_min,expected_STL_pace_per_min, stl_pace_effect_per_min,expected_STL_pace2_per_min, stl_pace2_effect_per_min, expected_STL_pace3_per_min, stl_pace3_effect_per_min, expected_STL_def_rtg_per_min,def_rtg_STL_effect_per_min, expected_STL_off_rtg_per_min, off_rtg_STL_effect_per_min, expected_STL_opp_STL_per_min, expected_STL_opp_STL_effect_per_min,vegas_ratio_stl_per_min, vegas_ratio_stl_effect_per_min, vegas_ratio_stl_pinnacle_per_min, vegas_ratio_stl_pinnacle_effect_per_min,vegas_ratio_stl_opp_pinnacle_per_min, vegas_ratio_stl_opp_pinnacle_effect_per_min, vegas_ratio_stl_ou_pinnacle_per_min, vegas_ratio_stl_ou_pinnacle_effect_per_min,actual_stl_per_min )

  r[:mean_stl_per_min] = mean_stl_per_min
  r[:PCT_STL] = pct_STL
  r[:team_average_o_PCT_STL] = team_average_o_PCT_STL
  r[:o_stl_delta_per_min] = o_stl_delta_per_min 
  r[:location_stl_effect_per_min] = location_stl_effect_per_min
  r[:rest_stl_effect_per_min] = rest_stl_effect_per_min
  r[:location_stl_per_min] = location_stl_per_min
  r[:rest_stl_per_min] = rest_stl_per_min
  r[:opp_rest_stl_per_min] = opp_rest_stl_per_min
  r[:opp_rest_stl_effect_per_min] = opp_rest_stl_effect_per_min
  r[:mean_starter_stl_per_min] = mean_starter_stl_per_min
  r[:mean_bench_stl_per_min] = mean_bench_stl_per_min
  r[:starterbench_stl_effect_per_min] = starterbench_stl_effect_per_min
  r[:mean_starterbench_stl_per_min] = mean_starterbench_stl_per_min
  r[:prev_stl_per_min] = prev_stl_per_min
  r[:prev_stl_delta_per_min] = prev_stl_delta_per_min
  r[:prev2_stl_per_min] = prev2_stl_per_min
  r[:prev2_stl_delta_per_min] = prev2_stl_delta_per_min
  r[:prev5_stl_per_min] = prev5_stl_per_min
  r[:prev5_stl_delta_per_min] = prev5_stl_delta_per_min
  r[:o_stl_pct_delta] = o_stl_pct_delta
  r[:expected_STL_per_min] = expected_STL_per_min
  r[:expected_STL_effect_per_min] = expected_STL_effect_per_min
  r[:scaled_steal_per_min] = scaled_steal_per_min
  r[:scaled_steal_effect_per_min] = scaled_steal_effect_per_min
  r[:scaled_pct_stl_effect_per_min] = scaled_pct_stl_effect_per_min
  r[:modded_steal_per_min] = modded_steal_per_min
  r[:modded_steal_effect_per_min] = modded_steal_effect_per_min
  r[:expected_STL_pace_per_min] = expected_STL_pace_per_min 
  r[:stl_pace_effect_per_min] = stl_pace_effect_per_min 
  r[:expected_STL_pace2_per_min] = expected_STL_pace2_per_min 
  r[:stl_pace2_effect_per_min] = stl_pace2_effect_per_min 
  r[:expected_STL_pace3_per_min] = expected_STL_pace3_per_min 
  r[:stl_pace3_effect_per_min] = stl_pace3_effect_per_min 
  r[:expected_STL_def_rtg_per_min] = expected_STL_def_rtg_per_min 
  r[:def_rtg_STL_effect_per_min] = def_rtg_STL_effect_per_min 
  r[:expected_STL_off_rtg_per_min] = expected_STL_off_rtg_per_min 
  r[:off_rtg_STL_effect_per_min] = off_rtg_STL_effect_per_min 
  r[:expected_STL_opp_STL_per_min] = expected_STL_opp_STL_per_min 
  r[:expected_STL_opp_STL_effect_per_min] = expected_STL_opp_STL_effect_per_min 
  r[:vegas_ratio_stl_per_min] = vegas_ratio_stl_per_min 
  r[:vegas_ratio_stl_effect_per_min] = vegas_ratio_stl_effect_per_min 
  r[:vegas_ratio_stl_pinnacle_per_min] = vegas_ratio_stl_pinnacle_per_min 
  r[:vegas_ratio_stl_pinnacle_effect_per_min] = vegas_ratio_stl_pinnacle_effect_per_min 
  r[:vegas_ratio_stl_opp_pinnacle_per_min] = vegas_ratio_stl_opp_pinnacle_per_min 
  r[:vegas_ratio_stl_opp_pinnacle_effect_per_min] = vegas_ratio_stl_opp_pinnacle_effect_per_min 
  r[:vegas_ratio_stl_ou_pinnacle_per_min] = vegas_ratio_stl_ou_pinnacle_per_min 
  r[:vegas_ratio_stl_ou_pinnacle_effect_per_min] = vegas_ratio_stl_ou_pinnacle_effect_per_min 
  r[:actual_stl_per_min] = actual_stl_per_min
end

#expected_OREB, previous_average[:o_DREB_PCT].to_f, b2b.to_f, mean_b2b_OREB.to_f, mean_b2b_OREB_PCT.to_f, mean_extra_rest_OREB.to_f, mean_extra_rest_OREB_PCT.to_f, r[:mean_opp_b2b

def populateSecondsFeatures( r, average_seconds, rest_effect_seconds, location_pts_effect_seconds, prev_seconds, prev2_seconds,prev5_seconds, mean_starter_seconds, mean_bench_seconds, mean_starterbench_seconds, avg_mins_10_or_less, avg_mins_20_or_less,avg_mins_30_or_less, avg_mins_over_30, vegas_average_over_under_Pinnacle, over_under_ratio_pinnacle, point_spread_abs_3_or_less,point_spread_abs_6_or_less, point_spread_abs_9_or_less, point_spread_abs_12_or_less, point_spread_abs_over_9, point_spread_abs_over_12,point_spread_3_or_less, point_spread_6_or_less, point_spread_9_or_less, point_spread_12_or_less, point_spread_over_9, point_spread_over_12,point_spread_neg_3_or_less, point_spread_neg_6_or_less, point_spread_neg_9_or_less, point_spread_neg_12_or_less, point_spread_neg_over_9,point_spread_neg_over_12, actual_SECONDS )

  r[:average_seconds] = average_seconds
  r[:rest_effect_seconds] = rest_effect_seconds 
  r[:location_pts_effect_seconds] = location_pts_effect_seconds 
  r[:prev_seconds] = prev_seconds 
  r[:prev2_seconds] = prev2_seconds 
  r[:prev5_seconds] = prev5_seconds 
  r[:mean_starter_seconds] = mean_starter_seconds 
  r[:mean_bench_seconds] = mean_bench_seconds 
  r[:mean_starterbench_seconds] = mean_starterbench_seconds 
  r[:avg_mins_10_or_less] = avg_mins_10_or_less 
  r[:avg_mins_20_or_less] = avg_mins_20_or_less 
  r[:avg_mins_30_or_less] = avg_mins_30_or_less 
  r[:avg_mins_over_30] = avg_mins_over_30 
  r[:vegas_average_over_under_Pinnacle] = vegas_average_over_under_Pinnacle
  r[:over_under_ratio_pinnacle] = over_under_ratio_pinnacle 
  r[:point_spread_abs_3_or_less] = point_spread_abs_3_or_less 
  r[:point_spread_abs_6_or_less] = point_spread_abs_6_or_less 
  r[:point_spread_abs_9_or_less] = point_spread_abs_9_or_less 
  r[:point_spread_abs_12_or_less] = point_spread_abs_12_or_less 
  r[:point_spread_abs_over_9] = point_spread_abs_over_9 
  r[:point_spread_abs_over_12] = point_spread_abs_over_12 
  r[:point_spread_3_or_less] = point_spread_3_or_less 
  r[:point_spread_6_or_less] = point_spread_6_or_less 
  r[:point_spread_9_or_less] = point_spread_9_or_less 
  r[:point_spread_12_or_less] = point_spread_12_or_less 
  r[:point_spread_over_9] = point_spread_over_9 
  r[:point_spread_over_12] = point_spread_over_12 
  r[:point_spread_neg_3_or_less] = point_spread_neg_3_or_less 
  r[:point_spread_neg_6_or_less] = point_spread_neg_6_or_less 
  r[:point_spread_neg_9_or_less] = point_spread_neg_9_or_less 
  r[:point_spread_neg_12_or_less] = point_spread_neg_12_or_less 
  r[:point_spread_neg_over_9] = point_spread_neg_over_9 
  r[:point_spread_neg_over_12] = point_spread_neg_over_12 
  r[:actual_SECONDS] = actual_SECONDS
end

def createXYtable( database, type, tablename )
  puts "Dropping and re-creating table #{tablename}"
  database.drop_table? tablename
  database.create_table tablename do
    column :player_name, :text
    column :date, :text

    column :b2b, :integer
    column :opp_b2b, :integer
    column :extra_rest, :integer
    column :opp_extra_rest, :integer
    column :location, :text
    column :starter, :integer

=begin
    column :point_spread_abs_9_or_less, :decimal
    column :point_spread_abs_12_or_less, :decimal
    column :point_spread_abs_over_9, :decimal
    column :point_spread_abs_over_12, :decimal
    column :point_spread_3_or_less, :decimal
    column :point_spread_6_or_less, :decimal
    column :point_spread_9_or_less, :decimal
    column :point_spread_12_or_less, :decimal
    column :point_spread_over_9, :decimal
    column :point_spread_over_12, :decimal
    column :point_spread_neg_3_or_less, :decimal
    column :point_spread_neg_6_or_less, :decimal
    column :point_spread_neg_9_or_less, :decimal
    column :point_spread_neg_12_or_less, :decimal
    column :point_spread_neg_over_9, :decimal
    column :point_spread_neg_over_12, :decimal
=end

    column :PTS_mean, :decimal
    column :def_rtg_delta, :decimal
    column :def_rtg_v_position_delta, :decimal
    column :o_pts_delta, :decimal
    column :location_pts_effect, :decimal
    column :rest_effect, :decimal
    column :pts_paint_effect, :decimal
    column :pts_off_tov_effect, :decimal
    column :fb_effect, :decimal
    column :pts_2ndchance_effect, :decimal
    column :usg_pct, :decimal
    column :usg_pct_minus_tov, :decimal
    column :location_pts, :decimal
    column :rest_pts, :decimal
    column :opp_rest_pts, :decimal
    column :expected_PTS_pace, :decimal
    column :pts_pace_effect, :decimal
    column :expected_PTS_pace2, :decimal
    column :pts_pace2_effect, :decimal
    column :expected_PTS_pace3, :decimal
    column :pts_pace3_effect, :decimal
    column :expected_PTS_def_rtg, :decimal
    column :def_rtg_effect, :decimal
    column :expected_PTS_def_rtg_v_position, :decimal
    column :def_rtg_v_position_effect, :decimal
    column :expected_PTS_off_rtg, :decimal
    column :off_rtg_PTS_effect, :decimal
    column :expected_PTS_opp_PTS, :decimal
    column :expected_PTS_opp_PTS_effect, :decimal
    column :mean_starter_pts, :decimal
    column :mean_bench_pts, :decimal
    column :starterbench_pts_effect, :decimal
    column :mean_starterbench_pts, :decimal
    column :prev_pts, :decimal
    column :prev_pts_delta, :decimal
    column :prev2_pts, :decimal
    column :prev2_pts_delta, :decimal
    column :prev5_pts, :decimal
    column :prev5_pts_delta, :decimal
    column :ft_effect, :decimal
    column :expected_FTM, :decimal
    column :vegas_ratio_pts, :decimal
    column :vegas_ratio_pts_effect, :decimal
    column :vegas_ratio_pts_pinnacle, :decimal
    column :vegas_ratio_pts_pinnacle_effect, :decimal
    column :vegas_ratio_pts_opp_pinnacle, :decimal
    column :vegas_ratio_pts_opp_pinnacle_effect, :decimal
    column :vegas_ratio_pts_ou_pinnacle, :decimal
    column :vegas_ratio_pts_ou_pinnacle_effect, :decimal
    column :adjusted_cfg_pts, :decimal
    column :adjusted_ufg_pts, :decimal
    column :adjusted_fg_pts, :decimal
    column :cfg_effect, :decimal

    column :average_seconds_per_min, :decimal
    column :mean_starter_seconds_per_min, :decimal
    column :mean_bench_seconds_per_min, :decimal
    column :mean_starterbench_seconds_per_min, :decimal
    column :prev_seconds_per_min, :decimal
    column :prev2_seconds_per_min, :decimal
    column :prev5_seconds_per_min, :decimal
    column :mean_starter_seconds, :decimal
    column :mean_bench_seconds, :decimal
    column :mean_starterbench_seconds, :decimal
    column :actual_mins, :decimal
    column :actual_PTS, :integer

    ##
    #features_points_per_min = [ 
    column :mean_pts_per_min, :decimal
    #column :def_rtg_delta, :decimal
    #column :def_rtg_v_position_delta, :decimal
    column :o_pts_delta_per_min, :decimal
    column :location_pts_effect_per_min, :decimal
    column :rest_effect_per_min, :decimal
    column :pts_pace_effect_per_min, :decimal
    column :pts_paint_effect_per_min,:decimal
    column :pts_off_tov_effect_per_min, :decimal
    column :fb_effect_per_min, :decimal
    column :pts_2ndchance_effect_per_min, :decimal
    #column :USG_PCT, :decimal
    #column :USG_PCT_minus_TOV, :decimal
    column :location_pts_per_min, :decimal
    column :rest_pts_per_min, :decimal
    column :opp_rest_pts_per_min, :decimal
    column :expected_PTS_pace_per_min, :decimal
    column :expected_PTS_pace2_per_min, :decimal
    column :pts_pace2_effect_per_min, :decimal
    column :expected_PTS_pace3_per_min, :decimal
    column :pts_pace3_effect_per_min, :decimal
    column :expected_PTS_def_rtg_per_min, :decimal
    column :def_rtg_effect_per_min, :decimal
    column :expected_PTS_def_rtg_v_position_per_min, :decimal
    column :def_rtg_v_position_effect_per_min, :decimal
    column :expected_PTS_off_rtg_per_min, :decimal
    column :off_rtg_PTS_effect_per_min, :decimal
    column :expected_PTS_opp_PTS_per_min, :decimal
    column :expected_PTS_opp_PTS_effect_per_min, :decimal
    column :mean_starter_pts_per_min, :decimal
    column :mean_bench_pts_per_min, :decimal
    column :starterbench_pts_effect_per_min, :decimal
    column :prev_pts_per_min, :decimal
    column :prev_pts_delta_per_min, :decimal
    column :prev2_pts_per_min, :decimal
    column :prev2_pts_delta_per_min, :decimal
    column :prev5_pts_per_min, :decimal
    column :prev5_pts_delta_per_min, :decimal
    column :ft_effect_per_min, :decimal
    column :expected_FTM_per_min, :decimal
    column :vegas_ratio_pts_per_min, :decimal
    column :vegas_ratio_pts_effect_per_min, :decimal
    column :vegas_ratio_pts_pinnacle_per_min, :decimal
    column :vegas_ratio_pts_pinnacle_effect_per_min, :decimal
    column :vegas_ratio_pts_opp_pinnacle_per_min, :decimal
    column :vegas_ratio_pts_opp_pinnacle_effect_per_min, :decimal
    column :vegas_ratio_pts_ou_pinnacle_per_min, :decimal
    column :vegas_ratio_pts_ou_pinnacle_effect_per_min, :decimal
    column :adjusted_cfg_pts_per_min, :decimal
    column :adjusted_ufg_pts_per_min, :decimal
    column :adjusted_fg_pts_per_min, :decimal
    column :cfg_effect_per_min, :decimal
    column :average_seconds, :decimal
    column :rest_effect_seconds, :decimal
    column :location_pts_effect_seconds, :decimal
    column :actual_pts_per_min, :decimal


    #features_orebs = [ 
      column :OREB_mean, :decimal
        column :OREB_PCT, :decimal
        column :opp_average_DREB_PCT, :decimal
        column :opp_average_DREB_mean,:decimal
        column :o_oreb_delta, :decimal
        column :location_oreb_effect,:decimal
        column :rest_oreb_effect,:decimal
        column :location_oreb,:decimal
        column :rest_oreb,:decimal
        column :opp_rest_oreb,:decimal
        column :mean_starter_oreb,:decimal
        column :mean_bench_oreb,:decimal
        column :starterbench_oreb_effect,:decimal
        column :mean_starterbench_oreb,:decimal
        column :prev_oreb,:decimal
        column :prev_oreb_delta, :decimal
        column :prev2_oreb, :decimal
        column :prev2_oreb_delta, :decimal
        column :prev5_oreb, :decimal
        column :prev5_oreb_delta, :decimal
        column :o_oreb_pct_delta,:decimal
        column :expected_OREB,:decimal
        column :expected_OREB_effect,:decimal
        column :scaled_oreb_pct,:decimal
        column :scaled_oreb_pct_effect, :decimal
        column :scaled_oreb, :decimal
        column :scaled_oreb_effect, :decimal
        column :modded_oreb, :decimal
        column :modded_oreb_effect,:decimal
        column :opp_average_v_position_OREB_mean, :decimal
        column :opp_average_v_position_OREB_PCT, :decimal
        column :opp_average_OREB_PCT,:decimal
=begin
        column :average_seconds/60, :decimal
        column :mean_starter_seconds/60, :decimal
        column :mean_bench_seconds/60, :decimal
        column :mean_starterbench_seconds/60, :decimal
        column :prev_seconds/60, :decimal
        column :prev2_seconds/60, :decimal
        column :prev5_seconds/60, :decimal
=end
        column :team_misses, :decimal
        column :team_3p_misses, :decimal
        column :team_2p_misses, :decimal
        column :team_ft_misses, :decimal
        column :opp_average_FT_PCT,:decimal
        column :opp_average_FG_PCT, :decimal
        column :opp_average_FG3_PCT, :decimal
        column :opp_average_FG2_PCT, :decimal
        column :expected_OREB_pace, :decimal
        column :oreb_pace_effect, :decimal
        column :expected_OREB_pace2, :decimal
        column :oreb_pace2_effect, :decimal
        column :expected_OREB_pace3, :decimal
        column :oreb_pace3_effect, :decimal
        column :expected_OREB_def_rtg, :decimal
        column :def_rtg_OREB_effect, :decimal
        column :expected_OREB_off_rtg, :decimal
        column :off_rtg_OREB_effect, :decimal
        column :expected_OREB_opp_OREB, :decimal
        column :expected_OREB_opp_OREB_effect, :decimal
        column :vegas_ratio_oreb, :decimal
        column :vegas_ratio_oreb_effect, :decimal
        column :vegas_ratio_oreb_pinnacle, :decimal
        column :vegas_ratio_oreb_pinnacle_effect, :decimal
        column :vegas_ratio_oreb_opp_pinnacle, :decimal
        column :vegas_ratio_oreb_opp_pinnacle_effect, :decimal
        column :vegas_ratio_oreb_ou_pinnacle, :decimal
        column :vegas_ratio_oreb_ou_pinnacle_effect, :decimal
        column :team_average_FT_PCT, :decimal
        column :team_average_FG_PCT, :decimal
        column :team_average_FG3_PCT, :decimal
        column :team_average_FG2_PCT, :decimal
        column :actual_OREB,:decimal

    #features_orebs_per_min = [ 
      column :mean_oreb_per_min, :decimal
        column :previous_average_OREB_PCT, :decimal
        #column :opp_average_DREB_PCT, :decimal
        column :opp_average_dreb_per_min,:decimal
        column :o_oreb_delta_per_min, :decimal
        column :location_oreb_effect_per_min,:decimal
        column :rest_oreb_effect_per_min,:decimal
        column :location_oreb_per_min,:decimal
        column :rest_oreb_per_min,:decimal
        column :opp_rest_oreb_per_min,:decimal
        column :mean_starter_oreb_per_min,:decimal
        column :mean_bench_oreb_per_min,:decimal
        column :starterbench_oreb_effect_per_min,:decimal
        column :mean_starterbench_oreb_per_min,:decimal
        column :prev_oreb_per_min,:decimal
        column :prev_oreb_delta_per_min, :decimal
        column :prev2_oreb_per_min, :decimal
        column :prev2_oreb_delta_per_min, :decimal
        column :prev5_oreb_per_min, :decimal
        column :prev5_oreb_delta_per_min, :decimal
        #column :o_oreb_pct_delta,:decimal
        column :expected_OREB_per_min,:decimal
        column :expected_OREB_effect_per_min,:decimal
        #column :scaled_oreb_pct,:decimal
        #column :scaled_oreb_pct_effect, :decimal
        column :scaled_oreb_per_min, :decimal
        column :scaled_oreb_effect_per_min, :decimal
        column :modded_oreb_per_min, :decimal
        column :modded_oreb_effect_per_min,:decimal
        column :opp_average_oreb_per_min, :decimal
        #column :opp_average_v_position_OREB_PCT, :decimal
        #column :opp_average_OREB_PCT,:decimal
=begin
        column :average_seconds/60, :decimal
        column :mean_starter_seconds/60, :decimal
        column :mean_bench_seconds/60, :decimal
        column :mean_starterbench_seconds/60, :decimal
        column :prev_seconds/60, :decimal
        column :prev2_seconds/60, :decimal
        column :prev5_seconds/60, :decimal
=end
        column :team_misses_per_min, :decimal
        column :team_3p_misses_per_min, :decimal
        column :team_2p_misses_per_min, :decimal
        column :team_ft_misses_per_min, :decimal
=begin
        column :opp_average_FT_PCT,:decimal
        column :opp_average_FG_PCT, :decimal
        column :opp_average_FG3_PCT, :decimal
        column :opp_average_FG2_PCT, :decimal
=end
        column :expected_OREB_pace_per_min, :decimal
        column :oreb_pace_effect_per_min, :decimal
        column :expected_OREB_pace2_per_min, :decimal
        column :oreb_pace2_effect_per_min, :decimal
        column :expected_OREB_pace3_per_min, :decimal
        column :oreb_pace3_effect_per_min, :decimal
        column :expected_OREB_def_rtg_per_min, :decimal
        column :def_rtg_OREB_effect_per_min, :decimal
        column :expected_OREB_off_rtg_per_min, :decimal
        column :off_rtg_OREB_effect_per_min, :decimal
        column :expected_OREB_opp_OREB_per_min, :decimal
        column :expected_OREB_opp_OREB_effect_per_min, :decimal
        column :vegas_ratio_oreb_per_min, :decimal
        column :vegas_ratio_oreb_effect_per_min, :decimal
        column :vegas_ratio_oreb_pinnacle_per_min, :decimal
        column :vegas_ratio_oreb_pinnacle_effect_per_min, :decimal
        column :vegas_ratio_oreb_opp_pinnacle_per_min, :decimal
        column :vegas_ratio_oreb_opp_pinnacle_effect_per_min, :decimal
        column :vegas_ratio_oreb_ou_pinnacle_per_min, :decimal
        column :vegas_ratio_oreb_ou_pinnacle_effect_per_min, :decimal
        column :actual_oreb_per_min, :decimal

    #features_drebs = [ 
      column :DREB_mean, :decimal
        column :DREB_PCT,:decimal
        #column :opp_average_OREB_PCT,:decimal
        column :opp_average_OREB_mean,:decimal
        column :o_dreb_delta, :decimal
        column :location_dreb_effect,:decimal
        column :rest_dreb_effect,:decimal
        column :location_dreb,:decimal
        column :rest_dreb,:decimal
        column :opp_rest_dreb,:decimal
        column :mean_starter_dreb,:decimal
        column :mean_bench_dreb,:decimal
        column :starterbench_dreb_effect,:decimal
        column :mean_starterbench_dreb,:decimal
        column :prev_dreb,:decimal
        column :prev_dreb_delta,:decimal
        column :prev2_dreb,:decimal
        column :prev2_dreb_delta,:decimal
        column :prev5_dreb,:decimal
        column :prev5_dreb_delta,:decimal
        column :o_dreb_pct_delta,:decimal
        column :expected_DREB,:decimal
        column :expected_DREB_effect,:decimal
        column :scaled_dreb_pct_effect, :decimal
        column :scaled_dreb_effect,:decimal
        column :modded_dreb, :decimal
        column :modded_dreb_effect,:decimal
        column :opp_average_v_position_DREB_mean,:decimal
        column :opp_average_v_position_DREB_PCT,:decimal
        #column :opp_average_DREB_PCT,:decimal
=begin
        column :average_seconds/60, :decimal
        column :mean_starter_seconds/60, :decimal
        column :mean_bench_seconds/60, :decimal
        column :mean_starterbench_seconds/60, :decimal
        column :prev_seconds/60, :decimal
        column :prev2_seconds/60, :decimal
        column :prev5_seconds/60, :decimal
=end
        column :oa_misses,:decimal
        column :oa_3p_misses,:decimal
        column :oa_2p_misses,:decimal
        column :oa_ft_misses,:decimal
=begin
        column :opp_average_FT_PCT,:decimal
        column :opp_average_FG_PCT,:decimal
        column :opp_average_FG3_PCT,:decimal
        column :opp_average_FG2_PCT,:decimal
=end
        column :expected_DREB_pace, :decimal
        column :dreb_pace_effect, :decimal
        column :expected_DREB_pace2, :decimal
        column :dreb_pace2_effect, :decimal
        column :expected_DREB_pace3, :decimal
        column :dreb_pace3_effect, :decimal
        column :expected_DREB_def_rtg, :decimal
        column :def_rtg_DREB_effect, :decimal
        column :expected_DREB_off_rtg, :decimal
        column :off_rtg_DREB_effect, :decimal
        column :expected_DREB_opp_DREB, :decimal
        column :expected_DREB_opp_DREB_effect, :decimal
        column :vegas_ratio_dreb, :decimal
        column :vegas_ratio_dreb_effect, :decimal
        column :vegas_ratio_dreb_pinnacle, :decimal
        column :vegas_ratio_dreb_pinnacle_effect, :decimal
        column :vegas_ratio_dreb_opp_pinnacle, :decimal
        column :vegas_ratio_dreb_opp_pinnacle_effect, :decimal
        column :vegas_ratio_dreb_ou_pinnacle, :decimal
        column :vegas_ratio_dreb_ou_pinnacle_effect, :decimal
        column :actual_DREB, :decimal

    #features_drebs_per_min = [ 
    column :mean_dreb_per_min, :decimal
      #column :DREB_PCT, :decimal
      #column :opp_average_OREB_PCT, :decimal
      #column :opp_average_oreb_per_min,:decimal
      column :o_dreb_delta_per_min, :decimal
      column :location_dreb_effect_per_min,:decimal
      column :rest_dreb_effect_per_min,:decimal
      column :location_dreb_per_min,:decimal
      column :rest_dreb_per_min,:decimal
      column :opp_rest_dreb_per_min,:decimal
      column :mean_starter_dreb_per_min,:decimal
      column :mean_bench_dreb_per_min,:decimal
      column :starterbench_dreb_effect_per_min,:decimal
      column :mean_starterbench_dreb_per_min,:decimal
      column :prev_dreb_per_min,:decimal
      column :prev_dreb_delta_per_min, :decimal
      column :prev2_dreb_per_min, :decimal
      column :prev2_dreb_delta_per_min, :decimal
      column :prev5_dreb_per_min, :decimal
      column :prev5_dreb_delta_per_min, :decimal
      #column :o_dreb_pct_delta,:decimal
      column :expected_dreb_per_min,:decimal
      column :expected_dreb_effect_per_min,:decimal
      column :scaled_dreb_pct,:decimal
      #column :scaled_dreb_pct_effect, :decimal
      column :scaled_dreb_per_min, :decimal
      column :scaled_dreb_effect_per_min, :decimal
      column :modded_dreb_per_min, :decimal
      column :modded_dreb_effect_per_min,:decimal
      #column :opp_average_dreb_per_min, :decimal
      #column :opp_average_v_position_DREB_PCT, :decimal
      #column :opp_average_DREB_PCT,:decimal
=begin
      column :average_seconds/60, :decimal
      column :mean_starter_seconds/60, :decimal
      column :mean_bench_seconds/60, :decimal
      column :mean_starterbench_seconds/60, :decimal
      column :prev_seconds/60, :decimal
      column :prev2_seconds/60, :decimal
      column :prev5_seconds/60, :decimal
=end
=begin
      column :team_misses_per_min, :decimal
      column :team_3p_misses_per_min, :decimal
      column :team_2p_misses_per_min, :decimal
      column :team_ft_misses_per_min, :decimal
      column :opp_average_FT_PCT,:decimal
      column :opp_average_FG_PCT, :decimal
      column :opp_average_FG3_PCT, :decimal
      column :opp_average_FG2_PCT, :decimal
=end
      column :expected_DREB_pace_per_min, :decimal
      column :dreb_pace_effect_per_min, :decimal
      column :expected_DREB_pace2_per_min, :decimal
      column :dreb_pace2_effect_per_min, :decimal
      column :expected_DREB_pace3_per_min, :decimal
      column :dreb_pace3_effect_per_min, :decimal
      column :expected_DREB_def_rtg_per_min, :decimal
      column :def_rtg_DREB_effect_per_min, :decimal
      column :expected_DREB_off_rtg_per_min, :decimal
      column :off_rtg_DREB_effect_per_min, :decimal
      column :expected_DREB_opp_DREB_per_min, :decimal
      column :expected_DREB_opp_DREB_effect_per_min, :decimal
      column :vegas_ratio_dreb_per_min, :decimal
      column :vegas_ratio_dreb_effect_per_min, :decimal
      column :vegas_ratio_dreb_pinnacle_per_min, :decimal
      column :vegas_ratio_dreb_pinnacle_effect_per_min, :decimal
      column :vegas_ratio_dreb_opp_pinnacle_per_min, :decimal
      column :vegas_ratio_dreb_opp_pinnacle_effect_per_min, :decimal
      column :vegas_ratio_dreb_ou_pinnacle_per_min, :decimal
      column :vegas_ratio_dreb_ou_pinnacle_effect_per_min, :decimal
      column :actual_dreb_per_min,:decimal


    #features_assists = [ 
      column :AST_mean, :decimal
        column :AST_PCT,:decimal
        column :team_average_o_AST_PCT,:decimal
        column :o_ast_delta, :decimal
        column :location_ast,:decimal
        column :location_ast_effect,:decimal
        column :rest_ast,:decimal
        column :rest_ast_effect,:decimal
        column :opp_rest_ast,:decimal
        column :opp_rest_ast_effect,:decimal
        column :mean_starter_ast,:decimal
        column :mean_bench_ast,:decimal
        column :starterbench_ast_effect,:decimal
        column :mean_starterbench_ast,:decimal
        column :prev_ast,:decimal
        column :prev_ast_delta,:decimal
        column :prev2_ast,:decimal
        column :prev2_ast_delta,:decimal
        column :prev5_ast,:decimal
        column :prev5_ast_delta,:decimal
        column :o_ast_pct_delta,:decimal
        column :expected_AST,:decimal
        column :expected_AST_effect,:decimal
        column :scaled_assist_pct_effect, :decimal
        column :scaled_assist,:decimal
        column :scaled_assist_effect,:decimal
        column :modded_assist,:decimal
        column :modded_assist_effect,:decimal
        column :expected_AST_pace, :decimal
        column :ast_pace_effect, :decimal
        column :expected_AST_pace2, :decimal
        column :ast_pace2_effect, :decimal
        column :expected_AST_pace3, :decimal
        column :ast_pace3_effect, :decimal
        column :expected_AST_def_rtg, :decimal
        column :def_rtg_AST_effect, :decimal
        column :expected_AST_off_rtg, :decimal
        column :off_rtg_AST_effect, :decimal
        column :expected_AST_opp_AST, :decimal
        column :expected_AST_opp_AST_effect, :decimal
        column :vegas_ratio_ast, :decimal
        column :vegas_ratio_ast_effect, :decimal
        column :vegas_ratio_ast_pinnacle, :decimal
        column :vegas_ratio_ast_pinnacle_effect, :decimal
        column :vegas_ratio_ast_opp_pinnacle, :decimal
        column :vegas_ratio_ast_opp_pinnacle_effect, :decimal
        column :vegas_ratio_ast_ou_pinnacle, :decimal
        column :vegas_ratio_ast_ou_pinnacle_effect, :decimal
        column :actual_AST, :decimal

    #features_assists_per_min = [ 
    column :mean_ast_per_min, :decimal
      #column :AST_PCT,:decimal
      #column :team_average_o_AST_PCT,:decimal
      column :o_ast_delta_per_min, :decimal
      column :location_ast_per_min,:decimal
      column :location_ast_effect_per_min,:decimal
      column :rest_ast_per_min,:decimal
      column :rest_ast_effect_per_min,:decimal
      column :opp_rest_ast_per_min,:decimal
      column :opp_rest_ast_effect_per_min,:decimal
      column :mean_starter_ast_per_min,:decimal
      column :mean_bench_ast_per_min,:decimal
      column :starterbench_ast_effect_per_min,:decimal
      column :mean_starterbench_ast_per_min,:decimal
      column :prev_ast_per_min,:decimal
      column :prev_ast_delta_per_min,:decimal
      column :prev2_ast_per_min,:decimal
      column :prev2_ast_delta_per_min,:decimal
      column :prev5_ast_per_min,:decimal
      column :prev5_ast_delta_per_min,:decimal
      #column :o_ast_pct_delta,:decimal
      column :expected_AST_per_min,:decimal
      column :expected_AST_effect_per_min,:decimal
      #column :scaled_assist_pct_effect, :decimal
      column :scaled_assist_per_min,:decimal
      column :scaled_assist_effect_per_min,:decimal
      column :modded_assist_per_min,:decimal
      column :modded_assist_effect_per_min,:decimal
      column :expected_AST_pace_per_min, :decimal
      column :ast_pace_effect_per_min, :decimal
      column :expected_AST_pace2_per_min, :decimal
      column :ast_pace2_effect_per_min, :decimal
      column :expected_AST_pace3_per_min, :decimal
      column :ast_pace3_effect_per_min, :decimal
      column :expected_AST_def_rtg_per_min, :decimal
      column :def_rtg_AST_effect_per_min, :decimal
      column :expected_AST_off_rtg_per_min, :decimal
      column :off_rtg_AST_effect_per_min, :decimal
      column :expected_AST_opp_AST_per_min, :decimal
      column :expected_AST_opp_AST_effect_per_min, :decimal
      column :vegas_ratio_ast_per_min, :decimal
      column :vegas_ratio_ast_effect_per_min, :decimal
      column :vegas_ratio_ast_pinnacle_per_min, :decimal
      column :vegas_ratio_ast_pinnacle_effect_per_min, :decimal
      column :vegas_ratio_ast_opp_pinnacle_per_min, :decimal
      column :vegas_ratio_ast_opp_pinnacle_effect_per_min, :decimal
      column :vegas_ratio_ast_ou_pinnacle_per_min, :decimal
      column :vegas_ratio_ast_ou_pinnacle_effect_per_min, :decimal
      column :actual_ast_per_min, :decimal

    #features_turnovers = [ 
      column :TOV_mean, :decimal
      column :TO_PCT,:decimal
      column :team_average_o_TO_PCT,:decimal
      column :o_tov_delta, :decimal
      column :location_tov_effect,:decimal
      column :rest_tov_effect,:decimal
      column :location_tov,:decimal
      column :rest_tov,:decimal
      column :opp_rest_tov,:decimal
      column :opp_rest_tov_effect,:decimal
      column :mean_starter_tov,:decimal
      column :mean_bench_tov,:decimal
      column :starterbench_tov_effect,:decimal
      column :mean_starterbench_tov,:decimal
      column :prev_tov,:decimal
      column :prev_tov_delta,:decimal
      column :prev2_tov,:decimal
      column :prev2_tov_delta,:decimal
      column :prev5_tov,:decimal
      column :prev5_tov_delta,:decimal
      column :o_tov_pct_delta,:decimal
      column :expected_TOV,:decimal
      column :expected_TOV_effect,:decimal
      column :scaled_turnover,:decimal
      column :scaled_turnover_pct_effect, :decimal
      column :scaled_turnover_effect,:decimal
      column :modded_turnover,:decimal
      column :modded_turnover_effect,:decimal
      column :expected_TOV_pace, :decimal
      column :tov_pace_effect, :decimal
      column :expected_TOV_pace2, :decimal
      column :tov_pace2_effect, :decimal
      column :expected_TOV_pace3, :decimal
      column :tov_pace3_effect, :decimal
      column :expected_TOV_def_rtg, :decimal
      column :def_rtg_TOV_effect, :decimal
      column :expected_TOV_off_rtg, :decimal
      column :off_rtg_TOV_effect, :decimal
      column :expected_TOV_opp_TOV, :decimal
      column :expected_TOV_opp_TOV_effect, :decimal
      column :vegas_ratio_tov, :decimal
      column :vegas_ratio_tov_effect, :decimal
      column :vegas_ratio_tov_pinnacle, :decimal
      column :vegas_ratio_tov_pinnacle_effect, :decimal
      column :vegas_ratio_tov_opp_pinnacle, :decimal
      column :vegas_ratio_tov_opp_pinnacle_effect, :decimal
      column :vegas_ratio_tov_ou_pinnacle, :decimal
      column :vegas_ratio_tov_ou_pinnacle_effect, :decimal
      column :actual_TOV, :decimal

    #features_turnovers_per_min = [ 
    column :mean_tov_per_min, :decimal
      #column :TO_PCT,:decimal
      #column :team_average_o_TO_PCT,:decimal
      column :o_tov_delta_per_min, :decimal
      column :location_tov_effect_per_min,:decimal
      column :rest_tov_effect_per_min,:decimal
      column :location_tov_per_min,:decimal
      column :rest_tov_per_min,:decimal
      column :opp_rest_tov_per_min,:decimal
      column :opp_rest_tov_effect_per_min,:decimal
      column :mean_starter_tov_per_min,:decimal
      column :mean_bench_tov_per_min,:decimal
      column :starterbench_tov_effect_per_min,:decimal
      column :mean_starterbench_tov_per_min,:decimal
      column :prev_tov_per_min,:decimal
      column :prev_tov_delta_per_min,:decimal
      column :prev2_tov_per_min,:decimal
      column :prev2_tov_delta_per_min,:decimal
      column :prev5_tov_per_min,:decimal
      column :prev5_tov_delta_per_min,:decimal
      #column :o_tov_pct_delta,:decimal
      column :expected_TOV_per_min,:decimal
      column :expected_TOV_effect_per_min,:decimal
      column :scaled_turnover_per_min,:decimal
      #column :scaled_turnover_pct_effect, :decimal
      column :scaled_turnover_effect_per_min,:decimal
      column :modded_turnover_per_min,:decimal
      column :modded_turnover_effect_per_min,:decimal
      column :expected_TOV_pace_per_min, :decimal
      column :tov_pace_effect_per_min, :decimal
      column :expected_TOV_pace2_per_min, :decimal
      column :tov_pace2_effect_per_min, :decimal
      column :expected_TOV_pace3_per_min, :decimal
      column :tov_pace3_effect_per_min, :decimal
      column :expected_TOV_def_rtg_per_min, :decimal
      column :def_rtg_TOV_effect_per_min, :decimal
      column :expected_TOV_off_rtg_per_min, :decimal
      column :off_rtg_TOV_effect_per_min, :decimal
      column :expected_TOV_opp_TOV_per_min, :decimal
      column :expected_TOV_opp_TOV_effect_per_min, :decimal
      column :vegas_ratio_tov_per_min, :decimal
      column :vegas_ratio_tov_effect_per_min, :decimal
      column :vegas_ratio_tov_pinnacle_per_min, :decimal
      column :vegas_ratio_tov_pinnacle_effect_per_min, :decimal
      column :vegas_ratio_tov_opp_pinnacle_per_min, :decimal
      column :vegas_ratio_tov_opp_pinnacle_effect_per_min, :decimal
      column :vegas_ratio_tov_ou_pinnacle_per_min, :decimal
      column :vegas_ratio_tov_ou_pinnacle_effect_per_min, :decimal
      column :actual_tov_per_min,:decimal

    #features_blocks = [ 
      column :BLK_mean, :decimal
      column :PCT_BLK,:decimal
      column :team_average_o_PCT_BLK,:decimal
      column :o_blk_delta, :decimal
      column :location_blk_effect,:decimal
      column :rest_blk_effect,:decimal
      column :location_blk,:decimal
      column :rest_blk,:decimal
      column :opp_rest_blk,:decimal
      column :opp_rest_blk_effect,:decimal
      column :mean_starter_blk,:decimal
      column :mean_bench_blk,:decimal
      column :starterbench_blk_effect,:decimal
      column :mean_starterbench_blk,:decimal
      column :prev_blk,:decimal
      column :prev_blk_delta,:decimal
      column :prev2_blk,:decimal
      column :prev2_blk_delta,:decimal
      column :prev5_blk,:decimal
      column :prev5_blk_delta,:decimal
      column :o_blk_pct_delta,:decimal
      column :expected_BLK,:decimal
      column :expected_BLK_effect,:decimal
      column :scaled_block_pct_effect, :decimal
      column :scaled_block,:decimal
      column :scaled_block_effect,:decimal
      column :modded_block,:decimal
      column :modded_block_effect,:decimal
      column :expected_BLK_pace, :decimal
      column :blk_pace_effect, :decimal
      column :expected_BLK_pace2, :decimal
      column :blk_pace2_effect, :decimal
      column :expected_BLK_pace3, :decimal
      column :blk_pace3_effect, :decimal
      column :expected_BLK_def_rtg, :decimal
      column :def_rtg_BLK_effect, :decimal
      column :expected_BLK_off_rtg, :decimal
      column :off_rtg_BLK_effect, :decimal
      column :expected_BLK_opp_BLK, :decimal
      column :expected_BLK_opp_BLK_effect, :decimal
      column :vegas_ratio_blk, :decimal
      column :vegas_ratio_blk_effect, :decimal
      column :vegas_ratio_blk_pinnacle, :decimal
      column :vegas_ratio_blk_pinnacle_effect, :decimal
      column :vegas_ratio_blk_opp_pinnacle, :decimal
      column :vegas_ratio_blk_opp_pinnacle_effect, :decimal
      column :vegas_ratio_blk_ou_pinnacle, :decimal
      column :vegas_ratio_blk_ou_pinnacle_effect, :decimal
      column :actual_BLK, :decimal

    #features_blocks_per_min = [ 
      column :mean_blk_per_min, :decimal
        #column :PCT_BLK,:decimal
        #column :team_average_o_PCT_BLK,:decimal
        column :o_blk_delta_per_min, :decimal
        column :location_blk_effect_per_min,:decimal
        column :rest_blk_effect_per_min,:decimal
        column :location_blk_per_min,:decimal
        column :rest_blk_per_min,:decimal
        column :opp_rest_blk_per_min,:decimal
        column :opp_rest_blk_effect_per_min,:decimal
        column :mean_starter_blk_per_min,:decimal
        column :mean_bench_blk_per_min,:decimal
        column :starterbench_blk_effect_per_min,:decimal
        column :mean_starterbench_blk_per_min,:decimal
        column :prev_blk_per_min,:decimal
        column :prev_blk_delta_per_min,:decimal
        column :prev2_blk_per_min,:decimal
        column :prev2_blk_delta_per_min,:decimal
        column :prev5_blk_per_min,:decimal
        column :prev5_blk_delta_per_min,:decimal
        #column :o_blk_pct_delta,:decimal
        column :expected_BLK_per_min,:decimal
        column :expected_BLK_effect_per_min,:decimal
        column :scaled_block_pct_effect_per_min, :decimal
        column :scaled_block_per_min,:decimal
        column :scaled_block_effect_per_min,:decimal
        column :modded_block_per_min,:decimal
        column :modded_block_effect_per_min,:decimal
        column :expected_BLK_pace_per_min, :decimal
        column :blk_pace_effect_per_min, :decimal
        column :expected_BLK_pace2_per_min, :decimal
        column :blk_pace2_effect_per_min, :decimal
        column :expected_BLK_pace3_per_min, :decimal
        column :blk_pace3_effect_per_min, :decimal
        column :expected_BLK_def_rtg_per_min, :decimal
        column :def_rtg_BLK_effect_per_min, :decimal
        column :expected_BLK_off_rtg_per_min, :decimal
        column :off_rtg_BLK_effect_per_min, :decimal
        column :expected_BLK_opp_BLK_per_min, :decimal
        column :expected_BLK_opp_BLK_effect_per_min, :decimal
        column :vegas_ratio_blk_per_min, :decimal
        column :vegas_ratio_blk_effect_per_min, :decimal
        column :vegas_ratio_blk_pinnacle_per_min, :decimal
        column :vegas_ratio_blk_pinnacle_effect_per_min, :decimal
        column :vegas_ratio_blk_opp_pinnacle_per_min, :decimal
        column :vegas_ratio_blk_opp_pinnacle_effect_per_min, :decimal
        column :vegas_ratio_blk_ou_pinnacle_per_min, :decimal
        column :vegas_ratio_blk_ou_pinnacle_effect_per_min, :decimal
        column :actual_blk_per_min,:decimal

    #features_steals = [ 
      column :STL_mean, :decimal
        column :PCT_STL,:decimal
        column :team_average_o_PCT_STL,:decimal
        column :o_stl_delta, :decimal
        column :location_stl_effect,:decimal
        column :rest_stl_effect,:decimal
        column :location_stl,:decimal
        column :rest_stl,:decimal
        column :opp_rest_stl,:decimal
        column :opp_rest_stl_effect,:decimal
        column :mean_starter_stl,:decimal
        column :mean_bench_stl,:decimal
        column :starterbench_stl_effect,:decimal
        column :mean_starterbench_stl,:decimal
        column :prev_stl,:decimal
        column :prev_stl_delta,:decimal
        column :prev2_stl,:decimal
        column :prev2_stl_delta,:decimal
        column :prev5_stl,:decimal
        column :prev5_stl_delta,:decimal
        column :o_stl_pct_delta,:decimal
        column :expected_STL,:decimal
        column :expected_STL_effect,:decimal
        column :scaled_steal,:decimal
        column :scaled_steal_effect,:decimal
        column :scaled_pct_stl_effect,:decimal
        column :modded_steal,:decimal
        column :modded_steal_effect,:decimal
        column :expected_STL_pace, :decimal
        column :stl_pace_effect, :decimal
        column :expected_STL_pace2, :decimal
        column :stl_pace2_effect, :decimal
        column :expected_STL_pace3, :decimal
        column :stl_pace3_effect, :decimal
        column :expected_STL_def_rtg, :decimal
        column :def_rtg_STL_effect, :decimal
        column :expected_STL_off_rtg, :decimal
        column :off_rtg_STL_effect, :decimal
        column :expected_STL_opp_STL, :decimal
        column :expected_STL_opp_STL_effect, :decimal
        column :vegas_ratio_stl, :decimal
        column :vegas_ratio_stl_effect, :decimal
        column :vegas_ratio_stl_pinnacle, :decimal
        column :vegas_ratio_stl_pinnacle_effect, :decimal
        column :vegas_ratio_stl_opp_pinnacle, :decimal
        column :vegas_ratio_stl_opp_pinnacle_effect, :decimal
        column :vegas_ratio_stl_ou_pinnacle, :decimal
        column :vegas_ratio_stl_ou_pinnacle_effect, :decimal
        column :actual_STL, :decimal

    #features_steals_per_min = [ 
      column :mean_stl_per_min, :decimal
        #column :PCT_STL,:decimal
        #column :team_average_o_PCT_STL,:decimal
        column :o_stl_delta_per_min, :decimal
        column :location_stl_effect_per_min,:decimal
        column :rest_stl_effect_per_min,:decimal
        column :location_stl_per_min,:decimal
        column :rest_stl_per_min,:decimal
        column :opp_rest_stl_per_min,:decimal
        column :opp_rest_stl_effect_per_min,:decimal
        column :mean_starter_stl_per_min,:decimal
        column :mean_bench_stl_per_min,:decimal
        column :starterbench_stl_effect_per_min,:decimal
        column :mean_starterbench_stl_per_min,:decimal
        column :prev_stl_per_min,:decimal
        column :prev_stl_delta_per_min,:decimal
        column :prev2_stl_per_min,:decimal
        column :prev2_stl_delta_per_min,:decimal
        column :prev5_stl_per_min,:decimal
        column :prev5_stl_delta_per_min,:decimal
        #column :o_stl_pct_delta,:decimal
        column :expected_STL_per_min,:decimal
        column :expected_STL_effect_per_min,:decimal
        column :scaled_steal_per_min,:decimal
        column :scaled_steal_effect_per_min,:decimal
        column :scaled_pct_stl_effect_per_min,:decimal
        column :modded_steal_per_min,:decimal
        column :modded_steal_effect_per_min,:decimal
        column :expected_STL_pace_per_min, :decimal
        column :stl_pace_effect_per_min, :decimal
        column :expected_STL_pace2_per_min, :decimal
        column :stl_pace2_effect_per_min, :decimal
        column :expected_STL_pace3_per_min, :decimal
        column :stl_pace3_effect_per_min, :decimal
        column :expected_STL_def_rtg_per_min, :decimal
        column :def_rtg_STL_effect_per_min, :decimal
        column :expected_STL_off_rtg_per_min, :decimal
        column :off_rtg_STL_effect_per_min, :decimal
        column :expected_STL_opp_STL_per_min, :decimal
        column :expected_STL_opp_STL_effect_per_min, :decimal
        column :vegas_ratio_stl_per_min, :decimal
        column :vegas_ratio_stl_effect_per_min, :decimal
        column :vegas_ratio_stl_pinnacle_per_min, :decimal
        column :vegas_ratio_stl_pinnacle_effect_per_min, :decimal
        column :vegas_ratio_stl_opp_pinnacle_per_min, :decimal
        column :vegas_ratio_stl_opp_pinnacle_effect_per_min, :decimal
        column :vegas_ratio_stl_ou_pinnacle_per_min, :decimal
        column :vegas_ratio_stl_ou_pinnacle_effect_per_min, :decimal
        column :actual_stl_per_min, :decimal

    #expected_OREB, previous_average[:o_DREB_PCT].to_f, b2b.to_f, mean_b2b_OREB.to_f, mean_b2b_OREB_PCT.to_f, mean_extra_rest_OREB.to_f, mean_extra_rest_OREB_PCT.to_f, r[:mean_opp_b2b

    #features_seconds = [ 
      #column :average_seconds, :decimal
      #column :rest_effect_seconds, :decimal
      #column :location_pts_effect_seconds, :decimal
      column :prev_seconds, :decimal
      column :prev2_seconds, :decimal
      column :prev5_seconds, :decimal
      #column :mean_starter_seconds, :decimal
      #column :mean_bench_seconds, :decimal
      #column :mean_starterbench_seconds, :decimal
      column :avg_mins_10_or_less, :decimal
      column :avg_mins_20_or_less, :decimal
      column :avg_mins_30_or_less, :decimal
      column :avg_mins_over_30, :decimal
      column :vegas_average_over_under_Pinnacle, :decimal
      column :over_under_ratio_pinnacle, :decimal
      column :point_spread_abs_3_or_less, :decimal
      column :point_spread_abs_6_or_less, :decimal
      column :point_spread_abs_9_or_less, :decimal
      column :point_spread_abs_12_or_less, :decimal
      column :point_spread_abs_over_9, :decimal
      column :point_spread_abs_over_12, :decimal
      column :point_spread_3_or_less, :decimal
      column :point_spread_6_or_less, :decimal
      column :point_spread_9_or_less, :decimal
      column :point_spread_12_or_less, :decimal
      column :point_spread_over_9, :decimal
      column :point_spread_over_12, :decimal
      column :point_spread_neg_3_or_less, :decimal
      column :point_spread_neg_6_or_less, :decimal
      column :point_spread_neg_9_or_less, :decimal
      column :point_spread_neg_12_or_less, :decimal
      column :point_spread_neg_over_9, :decimal
      column :point_spread_neg_over_12, :decimal
      column :actual_SECONDS,:decimal
  end
end

def create_daily_averages_table( database, type, tablename )
  puts "Dropping and re-creating table #{tablename}"
  database.drop_table? tablename
  database.create_table tablename do
    # see http://sequel.rubyforge.org/rdoc/files/doc/schema_modification_rdoc.html
    # primary_key :id
    # Float :price
    column :player_name, :text
    column :player_id, :text
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

def fixVegasTable( seasons_h, season, type, database, bCalcPlayers )
  team_tables = [ "advanced_TeamStats", "fourfactors_sqlTeamsFourFactors", "misc_sqlTeamsMisc", "playertrack_PlayerTrackTeam", "scoring_sqlTeamsScoring", "traditional_TeamStarterBenchStats", "traditional_TeamStats", "usage_sqlTeamsUsage" ]
  teams = database[ :"#{season.gsub(/-/,"_")}_#{type}_#{team_tables[6]}" ].exclude(:TEAM_ABBREVIATION => "EST").exclude(:TEAM_ABBREVIATION => "WST").distinct.select(:TEAM_ABBREVIATION).entries

  entities = teams
  teams.each_with_index{|entity,entity_index|
    boxscores = database[ :"#{season.gsub(/-/,"_")}_#{type}_traditional_TeamStats" ].where(:TEAM_ABBREVIATION => entity[:TEAM_ABBREVIATION]).order(:DATE).entries
    betting_lines = database[ :"regularseason_bettinglines" ].where(:home => entity[:TEAM_ABBREVIATION], :final => "1").or(:away => entity[:TEAM_ABBREVIATION], :final => "1").entries

    num_boxscores = boxscores.size
    boxscores.each_with_index{|boxscore_traditional,i|
      game_id = boxscore_traditional[:GAME_ID]
      if nil == game_id
        game_id = boxscore_traditional[:Game_ID]
        if nil == game_id
          binding.pry
          p "err"
        end
      end

      lines = betting_lines.select{|line|
        line[:nbaGameID] == game_id
      }
      calculateAverageOverUnder( database, season, type, lines, entity[:TEAM_ABBREVIATION] )
    }
    p "entity #{entity} done w #{num_boxscores} games in #{season} season. #{entity_index} / #{entities.size} done"
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

def getDatabase(filename)
  puts "Connecting to sqlite://#{filename}"
  database = Sequel.sqlite(filename)
  #database = Sequel.connect("jdbc:sqlite:#{filename}")
  # database.test_connection # saves blank file
  return database
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


