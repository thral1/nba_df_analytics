require 'open-uri'
require 'nokogiri'
require 'pry'
require 'fileutils'
require 'json'
require 'csv'

seasons_h = {
    #"season", "day 1", "reg season end + 1", "playoffs end + 1"
  "2014-15" => [ "2014-10-28", "2015-4-16", "2015-6-17" ],
  "2013-14" => [ "2013-10-29", "2014-4-17", "2014-6-16" ],
  "2012-13" => [ "2012-10-30", "2013-4-18", "2013-6-21" ],
  "2011-12" => [ "2011-12-25", "2012-4-27", "2012-6-22" ],
  "2010-11" => [ "2010-10-29", "2011-4-17", "2011-6-13" ],
  "2009-10" => [ "2009-10-27", "2010-4-15", "2010-6-18" ],
  "2008-09" => [ "2008-10-28", "2009-4-17", "2009-6-15" ],
  "2008-09" => [ "2008-11-25", "2009-4-17", "2009-6-15" ],
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
=begin
http://stats.nba.com/stats/scoreboardV2?DayOffset=0&LeagueID=00&gameDate=12%2F13%2F2014
#http://stats.nba.com/stats/boxscoresummaryv2?GameID=0021400340
http://stats.nba.com/stats/boxscoretraditionalv2?EndPeriod=10&EndRange=28800&GameID=0021400340&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0
http://stats.nba.com/stats/boxscoreadvancedv2?EndPeriod=10&EndRange=28800&GameID=0021400340&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0
http://stats.nba.com/stats/boxscoremiscv2?EndPeriod=10&EndRange=28800&GameID=0021400340&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0
http://stats.nba.com/stats/boxscorescoringv2?EndPeriod=10&EndRange=28800&GameID=0021400340&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0
http://stats.nba.com/stats/boxscoreusagev2?EndPeriod=10&EndRange=28800&GameID=0021400340&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0
http://stats.nba.com/stats/boxscorefourfactorsv2?EndPeriod=10&EndRange=28800&GameID=0021400340&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0
http://stats.nba.com/stats/boxscoreplayertrackv2?EndPeriod=10&EndRange=55800&GameID=0021400340&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0
http://stats.nba.com/stats/playbyplayv2?EndPeriod=10&EndRange=55800&GameID=0021400340&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0

[52] pry(main)> json["resultSets"][0]["headers"]
["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_CITY","PLAYER_ID","PLAYER_NAME","START_POSITION","COMMENT","MIN","FGM","FGA","FG_PCT","FG3M","FG3A","FG3_PCT","FTM","FTA","FT_PCT","OREB","DREB","REB","AST","STL","BLK","TO","PF","PTS","PLUS_MINUS"]
["GAME_ID","TEAM_ID","TEAM_NAME","TEAM_ABBREVIATION","TEAM_CITY","MIN","FGM","FGA","FG_PCT","FG3M","FG3A","FG3_PCT","FTM","FTA","FT_PCT","OREB","DREB","REB","AST","STL","BLK","TO","PF","PTS","PLUS_MINUS"]

["GAME_ID","TEAM_ID","TEAM_NAME","TEAM_ABBREVIATION","TEAM_CITY","MIN","FGM","FGA","FG_PCT","FG3M","FG3A","FG3_PCT","FTM","FTA","FT_PCT","OREB","DREB","REB","AST","STL","BLK","TO","PF","PTS","PLUS_MINUS"]
json["resultSets"][0]["name"]
"PlayerStats"
pry(main)> json["resultSets"][1]["name"]
=> "TeamStats"
[55] pry(main)> json["resultSets"][2]["name"]
=> "TeamStarterBenchStats"
[56] pry(main)> json["resultSets"][1]["headers"]
[57] pry(main)> json["resultSets"][2]["headers"]
["GAME_ID",
"TEAM_ID",
"TEAM_NAME",
"TEAM_ABBREVIATION",
"TEAM_CITY",
"STARTERS_BENCH",
"MIN",
"FGM",
"FGA",
"FG_PCT",
"FG3M",
"FG3A",
"FG3_PCT",
"FTM",
"FTA",
"FT_PCT",
"OREB",
"DREB",
"REB",
"AST",
"STL",
"BLK",
"TO",
"PF",
"PTS"]
=end

def addDatesToBoxscores( season, dir, day, enddate )
  while day < enddate

    doc = Nokogiri::HTML( open( "http://stats.nba.com/stats/scoreboardV2?DayOffset=0&LeagueID=00&gameDate=#{day.month}%2F#{day.day}%2F#{day.year}" ) )
    games_json = JSON.parse( doc.text )["resultSets"][0]
    games_headers = games_json["headers"]
    games_json["rowSet"].each{|game|
      game_id = game[2]

      Dir.glob(dir[0] + "/" + game_id + "*").each{|filename|
        arr_of_arrs = CSV.parse( File.open( filename, "r" ) )

        if arr_of_arrs[0][1] == "SEASON"
          p "skipping #{filename}"
          next
        end

        csv = ""
        arr_of_arrs.each_with_index{|row,i|
          if 0 == i
            row = row.insert(1,"SEASON").insert(2,"DATE")
          else
            row = row.insert(1,season).insert(2,day.to_s)
          end
          csv = csv + row.to_csv
        }

        File.open( filename, "w" ){|f|
          f.write csv
        }
        p "Re-parsing #{filename}"
      }
    }
    p "parsed #{day.month} #{day.day}, #{day.year}"

    day = day + 1
  end
end
def parseSeason( season, dir, day, enddate )
categories = ["traditional","advanced", "misc", "scoring", "usage", "fourfactors", "playertrack"]#, "playbyplay"]
  while day < enddate

    doc = Nokogiri::HTML( open( "http://stats.nba.com/stats/scoreboardV2?DayOffset=0&LeagueID=00&gameDate=#{day.month}%2F#{day.day}%2F#{day.year}" ) )
    games_json = JSON.parse( doc.text )["resultSets"][0]
    games_headers = games_json["headers"]
    games_json["rowSet"].each{|game|
      game_id = game[2]
      categories.each{|category|
        d = Nokogiri::HTML( open( "http://stats.nba.com/stats/boxscore#{category}v2?EndPeriod=10&EndRange=28800&GameID=#{game_id}&RangeType=2&Season=#{season}&SeasonType=Regular+Season&StartPeriod=1&StartRange=0" ) )
        boxscore_json = JSON.parse( d.text )
        boxscore_json["resultSets"].each{|resultSet|
          csv = ""
          csv = csv + resultSet["headers"].to_csv
          resultSet["rowSet"].each{|row|
            csv = csv + row.to_csv
          }

          #team_dir = FileUtils::mkdir_p dir[0] + "/" + team_abbr
          File.open( dir[0] + "/" + game[2] + "_" + category + "_" + resultSet["name"] + ".csv", "w" ){|f|
            f.write csv
          }
        }
      }
    }
    p "parsed #{day.month} #{day.day}, #{day.year}"

    day = day + 1
  end
end

seasons_h.each{|season,dates|
  day = Date.parse dates[0]
  regseason_end = Date.parse dates[1]
  dir = FileUtils::mkdir_p season + "/regularseason"
  #parseSeason( season, dir, day, regseason_end )
  addDatesToBoxscores( season, dir, day, regseason_end )


  playoffs_end = Date.parse dates[2]
  dir = FileUtils::mkdir_p season + "/playoffs"
  #parseSeason( season, dir, regseason_end, playoffs_end )
  addDatesToBoxscores( season, dir, regseason_end, playoffs_end )
}


