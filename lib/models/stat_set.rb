#!/usr/bin/env ruby
require 'ruby-duration'
require 'rubygems'

class StatSet
  attr_accessor :name, :total, :game_total, :hash, :mean, :median

  def initialize( name = "" )
    @name = name
    @total = 0
    @game_total = 0
    @hash = Hash.new
    @mean = 0
    @median = 0
  end

  def +( y )
    @game_total = @game_total + y.game_total 
    @hash = @hash.merge y.hash

    @total = 0
    @hash.each{|k,v|
      @total = @total + v
    }

    if @hash.size > 0
      @mean = @total / @hash.size
    else
      @mean = 0
    end

    tmp_arr = @hash.values.sort
    if tmp_arr.size % 2
      @median = tmp_arr[ @hash.size / 2 ]
    else
      @median = (tmp_arr[ @hash.size / 2 ] + tmp_arr[ (@hash.size / 2) - 1 ]) / 2
    end
  end

  def calcAvg( y, numGames )
    #jlk - sort hash by gameid, and grab last "numGames" games
    #then recalculate mean and median
    #@game_total = @game_total + y.game_total 
    begin
      arr = y.hash.sort[ (y.hash.size - numGames )...(y.hash.size) ]
    rescue StandardError => e
      binding.pry
      p "hi"
    end
    @total = 0

    if arr
      arr.each{|a|
        id = a[0]
        v = a[1]
        @total = @total + v

        @hash[ id ] = v
      }
    end

    if @hash.size > 0
      @mean = @total / @hash.size
    else
      @mean = 0
    end

    tmp_arr = @hash.values.sort
    if tmp_arr.size % 2
      @median = tmp_arr[ @hash.size / 2 ]
    else
      @median = (tmp_arr[ @hash.size / 2 ] + tmp_arr[ (@hash.size / 2) - 1 ]) / 2
    end
  end

  def to_h
    h = {}
    h[ (@name.to_s + "_total").to_sym ] = @total
    h[ (@name.to_s + "_mean").to_sym ] = @mean
    h[ (@name.to_s + "_median").to_sym ] = @median
    return h
  end

  def to_opponent_h
    h = {}
    h[ ("o_" + @name.to_s + "_total").to_sym ] = @total
    h[ ("o_" + @name.to_s + "_mean").to_sym ] = @mean
    h[ ("o_" + @name.to_s + "_median").to_sym ] = @median
    return h
  end
end

class DerivedStats
  attr_accessor :FG_PCT, :FG3_PCT, :FG2_PCT, :FT_PCT, :TS_PCT, :EFG_PCT, :PCT_FGA_3PT, :FTA_RATE, :FG2A, :PCT_FGA_2PT, :PCT_FGA_3PT, :PCT_PTS_2PT, :PCT_PTS_3PT, :PCT_PTS_FB, :PCT_PTS_FT, :PCT_PTS_OFF_TOV, :PCT_PTS_PAINT, :PCT_PTS_2PT_MR
  attr_accessor :possessions_total, :OREB_PCT, :DREB_PCT, :REB_PCT, :AST_PCT, :PCT_STL, :PCT_BLK, :TO_PCT, :USG_PCT, :USG_PCT_minus_TOV, :OFF_RATING, :DEF_RATING
  attr_accessor :NET_RATING, :AST_TOV, :AST_RATIO, :TO_RATIO, :PACE, :PIE, :team_offensive_points_total, :team_defensive_points_total
  attr_accessor :CFG_PCT, :UFG_PCT, :DFG_PCT
  attr_accessor :PCT_AST_2PM, :PCT_UAST_2PM, :PCT_AST_3PM, :PCT_UAST_3PM, :PCT_AST_FGM, :PCT_UAST_FGM
  attr_accessor :PCT_FGM, :PCT_FGA, :PCT_FG3M, :PCT_FG3A, :PCT_FTM, :PCT_FTA, :PCT_OREB, :PCT_DREB, :PCT_REB, :PCT_AST, :PCT_TOV, :PCT_STL, :PCT_BLK, :PCT_BLKA, :PCT_PF, :PCT_PFD, :PCT_PTS

  def initialize
    @FG_PCT = 0
    @FG3_PCT = 0
    @FG2_PCT = 0
    @FT_PCT = 0

    @TS_PCT = 0
    @EFG_PCT = 0
    @PCT_FGA_3PT = 0
    @FTA_RATE = 0
    @FG2A = 0
    @PCT_FGA_2PT = 0
    @PCT_FGA_3PT = 0
    @PCT_PTS_2PT = 0
    @PCT_PTS_3PT = 0
    @PCT_PTS_FB = 0
    @PCT_PTS_FT = 0
    @PCT_PTS_OFF_TOV = 0
    @PCT_PTS_PAINT = 0
    @PCT_PTS_2PT_MR = 0
    @possessions_total = 0
    @OREB_PCT = 0
    @DREB_PCT = 0
    @REB_PCT = 0
    @AST_PCT = 0
    @PCT_STL = 0
    @PCT_BLK = 0
    @TO_PCT = 0
    @USG_PCT = 0
    @USG_PCT_minus_TOV = 0
    @OFF_RATING = 0
    @DEF_RATING = 0
    @NET_RATING = 0
    @AST_TOV = 0
    @AST_RATIO = 0
    @TO_RATIO = 0
    @PACE = 0
    @PIE = 0
    @CFG_PCT = 0
    @UFG_PCT = 0
    @DFG_PCT = 0

    @PCT_FGA_2PT = 0
    @PCT_FGA_3PT = 0
    @PCT_PTS_2PT = 0
    @PCT_PTS_2PT_MR = 0
    @PCT_PTS_3PT = 0
    @PCT_PTS_FB = 0
    @PCT_PTS_FT = 0
    @PCT_PTS_OFF_TOV = 0
    @PCT_PTS_PAINT = 0

    @PCT_AST_2PM = 0
    @PCT_UAST_2PM = 0
    @PCT_AST_3PM = 0
    @PCT_UAST_3PM = 0
    @PCT_AST_FGM = 0
    @PCT_UAST_FGM = 0

    @PCT_FGM = 0
    @PCT_FGA = 0
    @PCT_FG3M = 0
    @PCT_FG3A = 0
    @PCT_FTM = 0
    @PCT_FTA = 0
    @PCT_OREB = 0
    @PCT_DREB = 0
    @PCT_REB = 0
    @PCT_AST = 0
    @PCT_TOV = 0
    @PCT_STL = 0
    @PCT_BLK = 0
    @PCT_BLKA = 0
    @PCT_PF = 0
    @PCT_PFD = 0
    @PCT_PTS = 0
  end

  def +( y )
  end

  def calcAvg( y, numGames )
  end

  def to_h
    h = {}
    h[ :FG_PCT ] = @FG_PCT 
    h[ :FG3_PCT ] = @FG3_PCT 
    h[ :FG2_PCT ] = @FG2_PCT 
    h[ :FT_PCT ] = @FT_PCT 

    h[ :TS_PCT ] = @TS_PCT 
    h[ :EFG_PCT ] = @EFG_PCT 
    h[ :PCT_FGA_3PT ] = @PCT_FGA_3PT 
    h[ :FTA_RATE ] = @FTA_RATE 
    h[ :possessions_total ] = @possessions_total 
    h[ :OREB_PCT ] = @OREB_PCT 
    h[ :DREB_PCT ] = @DREB_PCT 
    h[ :REB_PCT ] = @REB_PCT 
    h[ :AST_PCT ] = @AST_PCT 
    h[ :PCT_STL ] = @PCT_STL 
    h[ :PCT_BLK ] = @PCT_BLK 
    h[ :TO_PCT ] = @TO_PCT 
    h[ :USG_PCT ] = @USG_PCT 
    h[ :USG_PCT_minus_TOV ] = @USG_PCT_minus_TOV
    h[ :OFF_RATING ] = @OFF_RATING 
    h[ :DEF_RATING ] = @DEF_RATING 
    h[ :NET_RATING ] = @NET_RATING 
    h[ :AST_TOV ] = @AST_TOV 
    h[ :AST_RATIO ] = @AST_RATIO 
    h[ :TO_RATIO ] = @TO_RATIO 
    h[ :PACE ] = @PACE 
    h[ :PIE ] = @PIE 
    h[ :CFG_PCT ] = @CFG_PCT 
    h[ :UFG_PCT ] = @UFG_PCT 
    h[ :DFG_PCT ] = @DFG_PCT 

    h[ :PCT_FGA_2PT ] = @PCT_FGA_2PT 
    h[ :PCT_FGA_3PT ] = @PCT_FGA_3PT 
    h[ :PCT_PTS_2PT ] = @PCT_PTS_2PT 
    h[ :PCT_PTS_2PT_MR ] = @PCT_PTS_2PT_MR 
    h[ :PCT_PTS_3PT ] = @PCT_PTS_3PT 
    h[ :PCT_PTS_FB ] = @PCT_PTS_FB 
    h[ :PCT_PTS_FT ] = @PCT_PTS_FT 
    h[ :PCT_PTS_OFF_TOV ] = @PCT_PTS_OFF_TOV 
    h[ :PCT_PTS_PAINT ] = @PCT_PTS_PAINT 

    h[ :PCT_AST_2PM ] = @PCT_AST_2PM
    h[ :PCT_UAST_2PM ] = @PCT_UAST_2PM
    h[ :PCT_AST_3PM ] = @PCT_AST_3PM
    h[ :PCT_UAST_3PM ] = @PCT_UAST_3PM
    h[ :PCT_AST_FGM ] = @PCT_AST_FGM
    h[ :PCT_UAST_FGM ] = @PCT_UAST_FGM

    h[ :PCT_FGM ] = @PCT_FGM
    h[ :PCT_FGA ] = @PCT_FGA
    h[ :PCT_FG3M ] = @PCT_FG3M
    h[ :PCT_FG3A ] = @PCT_FG3A
    h[ :PCT_FTM ] = @PCT_FTM
    h[ :PCT_FTA ] = @PCT_FTA
    h[ :PCT_OREB ] = @PCT_OREB
    h[ :PCT_DREB ] = @PCT_DREB
    h[ :PCT_REB ] = @PCT_REB
    h[ :PCT_AST ] = @PCT_AST
    h[ :PCT_TOV ] = @PCT_TOV
    h[ :PCT_STL ] = @PCT_STL
    h[ :PCT_BLK ] = @PCT_BLK
    h[ :PCT_BLKA ] = @PCT_BLKA
    h[ :PCT_PF ] = @PCT_PF
    h[ :PCT_PFD ] = @PCT_PFD
    h[ :PCT_PTS ] = @PCT_PTS
    return h
  end

  def to_opponent_h
    h = {}
    h[ :o_FG_PCT ] = @FG_PCT 
    h[ :o_FG3_PCT ] = @FG3_PCT 
    h[ :o_FG2_PCT ] = @FG2_PCT 
    h[ :o_FT_PCT ] = @FT_PCT 

    h[ :o_TS_PCT ] = @TS_PCT 
    h[ :o_EFG_PCT ] = @EFG_PCT 
    h[ :o_PCT_FGA_3PT ] = @PCT_FGA_3PT 
    h[ :o_FTA_RATE ] = @FTA_RATE 
    h[ :o_possessions_total ] = @possessions_total 
    h[ :o_OREB_PCT ] = @OREB_PCT 
    h[ :o_DREB_PCT ] = @DREB_PCT 
    h[ :o_REB_PCT ] = @REB_PCT 
    h[ :o_AST_PCT ] = @AST_PCT 
    h[ :o_PCT_STL ] = @PCT_STL 
    h[ :o_PCT_BLK ] = @PCT_BLK 
    h[ :o_TO_PCT ] = @TO_PCT 
    h[ :o_USG_PCT ] = @USG_PCT 
    h[ :o_OFF_RATING ] = @OFF_RATING 
    h[ :o_DEF_RATING ] = @DEF_RATING 
    h[ :o_NET_RATING ] = @NET_RATING 
    h[ :o_AST_TOV ] = @AST_TOV 
    h[ :o_AST_RATIO ] = @AST_RATIO 
    h[ :o_TO_RATIO ] = @TO_RATIO 
    h[ :o_PACE ] = @PACE 
    h[ :o_PIE ] = @PIE 
    h[ :o_CFG_PCT ] = @CFG_PCT 
    h[ :o_UFG_PCT ] = @UFG_PCT 
    h[ :o_DFG_PCT ] = @DFG_PCT 

    h[ :o_PCT_FGA_2PT ] = @PCT_FGA_2PT 
    h[ :o_PCT_FGA_3PT ] = @PCT_FGA_3PT 
    h[ :o_PCT_PTS_2PT ] = @PCT_PTS_2PT 
    h[ :o_PCT_PTS_2PT_MR ] = @PCT_PTS_2PT_MR 
    h[ :o_PCT_PTS_3PT ] = @PCT_PTS_3PT 
    h[ :o_PCT_PTS_FB ] = @PCT_PTS_FB 
    h[ :o_PCT_PTS_FT ] = @PCT_PTS_FT 
    h[ :o_PCT_PTS_OFF_TOV ] = @PCT_PTS_OFF_TOV 
    h[ :o_PCT_PTS_PAINT ] = @PCT_PTS_PAINT 

    h[ :o_PCT_AST_2PM ] = @PCT_AST_2PM
    h[ :o_PCT_UAST_2PM ] = @PCT_UAST_2PM
    h[ :o_PCT_AST_3PM ] = @PCT_AST_3PM
    h[ :o_PCT_UAST_3PM ] = @PCT_UAST_3PM
    h[ :o_PCT_AST_FGM ] = @PCT_AST_FGM
    h[ :o_PCT_UAST_FGM ] = @PCT_UAST_FGM

    h[ :o_PCT_FGM ] = @PCT_FGM
    h[ :o_PCT_FGA ] = @PCT_FGA
    h[ :o_PCT_FG3M ] = @PCT_FG3M
    h[ :o_PCT_FG3A ] = @PCT_FG3A
    h[ :o_PCT_FTM ] = @PCT_FTM
    h[ :o_PCT_FTA ] = @PCT_FTA
    h[ :o_PCT_OREB ] = @PCT_OREB
    h[ :o_PCT_DREB ] = @PCT_DREB
    h[ :o_PCT_REB ] = @PCT_REB
    h[ :o_PCT_AST ] = @PCT_AST
    h[ :o_PCT_TOV ] = @PCT_TOV
    h[ :o_PCT_STL ] = @PCT_STL
    h[ :o_PCT_BLK ] = @PCT_BLK
    h[ :o_PCT_BLKA ] = @PCT_BLKA
    h[ :o_PCT_PF ] = @PCT_PF
    h[ :o_PCT_PFD ] = @PCT_PFD
    h[ :o_PCT_PTS ] = @PCT_PTS
    return h
  end
end

class PerMinStats
  attr_accessor :FGM_per_min, :FGA_per_min, :FG2M_per_min, :FG2A_per_min, :FG3M_per_min, :FG3A_per_min, :FTM_per_min, :FTA_per_min, :PTS_per_min, :AST_2PM_per_min, :UAST_2PM_per_min, :AST_3PM_per_min, :UAST_3PM_per_min, :AST_FGM_per_min, :UAST_FGM_per_min, :PTS_OFF_TOV_per_min, :PTS_2ND_CHANCE_per_min, :PTS_FB_per_min, :PTS_PAINT_per_min, :OREB_per_min, :DREB_per_min, :REB_per_min, :AST_per_min, :STL_per_min, :BLK_per_min, :TOV_per_min, :PF_per_min, :PLUS_MINUS_per_min, :BLKA_per_min, :PFD_per_min, :DIST_per_min, :ORBC_per_min, :DRBC_per_min, :RBC_per_min, :TCHS_per_min, :SAST_per_min, :FTAST_per_min, :PASS_per_min, :CFGM_per_min, :CFGA_per_min, :UFGM_per_min, :UFGA_per_min, :DFGM_per_min, :DFGA_per_min

  def initialize
    @FGM_per_min = 0
    @FGA_per_min = 0
    @FG2M_per_min = 0
    @FG2A_per_min = 0
    @FG3M_per_min = 0
    @FG3A_per_min = 0
    @FTM_per_min = 0
    @FTA_per_min = 0
    @PTS_per_min = 0
    @AST_2PM_per_min = 0
    @UAST_2PM_per_min = 0
    @AST_3PM_per_min = 0
    @UAST_3PM_per_min = 0
    @AST_FGM_per_min = 0
    @UAST_FGM_per_min = 0
    @PTS_2PT_MR_per_min = 0
    @PTS_FT_per_min = 0
    @PTS_OFF_TOV_per_min = 0
    @PTS_2ND_CHANCE_per_min = 0
    @PTS_FB_per_min = 0
    @PTS_PAINT_per_min = 0
    @OREB_per_min = 0
    @DREB_per_min = 0
    @REB_per_min = 0
    @AST_per_min = 0
    @STL_per_min = 0
    @BLK_per_min = 0
    @TOV_per_min = 0
    @PF_per_min = 0
    @PLUS_MINUS_per_min = 0
    @BLKA_per_min = 0
    @PFD_per_min = 0
    @DIST_per_min = 0
    @ORBC_per_min = 0
    @DRBC_per_min = 0
    @RBC_per_min = 0
    @TCHS_per_min = 0
    @SAST_per_min = 0
    @FTAST_per_min = 0
    @PASS_per_min = 0
    @CFGM_per_min = 0
    @CFGA_per_min = 0
    @UFGM_per_min = 0
    @UFGA_per_min = 0
    @DFGM_per_min = 0
    @DFGA_per_min = 0
  end

  def +( y )
  end

  def calcAvg( y, numGames )
  end

  def to_h
    h = {}
    h[ :FGM_per_min ] = @FGM_per_min 
    h[ :FGA_per_min ] = @FGA_per_min 
    h[ :FG2M_per_min ] = @FG2M_per_min 
    h[ :FG2A_per_min ] = @FG2A_per_min 
    h[ :FG3M_per_min ] = @FG3M_per_min 
    h[ :FG3A_per_min ] = @FG3A_per_min 
    h[ :FTM_per_min ] = @FTM_per_min 
    h[ :FTA_per_min ] = @FTA_per_min 
    h[ :PTS_per_min ] = @PTS_per_min 
    h[ :AST_2PM_per_min ] = @AST_2PM_per_min 
    h[ :UAST_2PM_per_min ] = @UAST_2PM_per_min 
    h[ :AST_3PM_per_min ] = @AST_3PM_per_min 
    h[ :UAST_3PM_per_min ] = @UAST_3PM_per_min 
    h[ :AST_FGM_per_min ] = @AST_FGM_per_min 
    h[ :UAST_FGM_per_min ] = @UAST_FGM_per_min 
    h[ :PTS_OFF_TOV_per_min ] = @PTS_OFF_TOV_per_min 
    h[ :PTS_2ND_CHANCE_per_min ] = @PTS_2ND_CHANCE_per_min 
    h[ :PTS_FB_per_min ] = @PTS_FB_per_min 
    h[ :PTS_PAINT_per_min ] = @PTS_PAINT_per_min 
    h[ :OREB_per_min ] = @OREB_per_min 
    h[ :DREB_per_min ] = @DREB_per_min 
    h[ :REB_per_min ] = @REB_per_min 
    h[ :AST_per_min ] = @AST_per_min 
    h[ :STL_per_min ] = @STL_per_min 
    h[ :BLK_per_min ] = @BLK_per_min 
    h[ :TOV_per_min ] = @TOV_per_min 
    h[ :PF_per_min ] = @PF_per_min 
    h[ :PLUS_MINUS_per_min ] = @PLUS_MINUS_per_min 
    h[ :BLKA_per_min ] = @BLKA_per_min 
    h[ :PFD_per_min ] = @PFD_per_min 
    h[ :DIST_per_min ] = @DIST_per_min 
    h[ :ORBC_per_min ] = @ORBC_per_min 
    h[ :DRBC_per_min ] = @DRBC_per_min 
    h[ :RBC_per_min ] = @RBC_per_min 
    h[ :TCHS_per_min ] = @TCHS_per_min 
    h[ :SAST_per_min ] = @SAST_per_min 
    h[ :FTAST_per_min ] = @FTAST_per_min 
    h[ :PASS_per_min ] = @PASS_per_min 
    h[ :CFGM_per_min ] = @CFGM_per_min 
    h[ :CFGA_per_min ] = @CFGA_per_min 
    h[ :UFGM_per_min ] = @UFGM_per_min 
    h[ :UFGA_per_min ] = @UFGA_per_min 
    h[ :DFGM_per_min ] = @DFGM_per_min 
    h[ :DFGA_per_min ] = @DFGA_per_min 
    return h
  end

  def to_opponent_h
    h = {}
    h[ :o_FGM_per_min ] = @FGM_per_min 
    h[ :o_FGA_per_min ] = @FGA_per_min 
    h[ :o_FG2M_per_min ] = @FG2M_per_min 
    h[ :o_FG2A_per_min ] = @FG2A_per_min 
    h[ :o_FG3M_per_min ] = @FG3M_per_min 
    h[ :o_FG3A_per_min ] = @FG3A_per_min 
    h[ :o_FTM_per_min ] = @FTM_per_min 
    h[ :o_FTA_per_min ] = @FTA_per_min 
    h[ :o_PTS_per_min ] = @PTS_per_min 
    h[ :o_AST_2PM_per_min ] = @AST_2PM_per_min 
    h[ :o_UAST_2PM_per_min ] = @UAST_2PM_per_min 
    h[ :o_AST_3PM_per_min ] = @AST_3PM_per_min 
    h[ :o_UAST_3PM_per_min ] = @UAST_3PM_per_min 
    h[ :o_AST_FGM_per_min ] = @AST_FGM_per_min 
    h[ :o_UAST_FGM_per_min ] = @UAST_FGM_per_min 
    h[ :o_PTS_OFF_TOV_per_min ] = @PTS_OFF_TOV_per_min 
    h[ :o_PTS_2ND_CHANCE_per_min ] = @PTS_2ND_CHANCE_per_min 
    h[ :o_PTS_FB_per_min ] = @PTS_FB_per_min 
    h[ :o_PTS_PAINT_per_min ] = @PTS_PAINT_per_min 
    h[ :o_OREB_per_min ] = @OREB_per_min 
    h[ :o_DREB_per_min ] = @DREB_per_min 
    h[ :o_REB_per_min ] = @REB_per_min 
    h[ :o_AST_per_min ] = @AST_per_min 
    h[ :o_STL_per_min ] = @STL_per_min 
    h[ :o_BLK_per_min ] = @BLK_per_min 
    h[ :o_TOV_per_min ] = @TOV_per_min 
    h[ :o_PF_per_min ] = @PF_per_min 
    h[ :o_PLUS_MINUS_per_min ] = @PLUS_MINUS_per_min 
    h[ :o_BLKA_per_min ] = @BLKA_per_min 
    h[ :o_PFD_per_min ] = @PFD_per_min 
    h[ :o_DIST_per_min ] = @DIST_per_min 
    h[ :o_ORBC_per_min ] = @ORBC_per_min 
    h[ :o_DRBC_per_min ] = @DRBC_per_min 
    h[ :o_RBC_per_min ] = @RBC_per_min 
    h[ :o_TCHS_per_min ] = @TCHS_per_min 
    h[ :o_SAST_per_min ] = @SAST_per_min 
    h[ :o_FTAST_per_min ] = @FTAST_per_min 
    h[ :o_PASS_per_min ] = @PASS_per_min 
    h[ :o_CFGM_per_min ] = @CFGM_per_min 
    h[ :o_CFGA_per_min ] = @CFGA_per_min 
    h[ :o_UFGM_per_min ] = @UFGM_per_min 
    h[ :o_UFGA_per_min ] = @UFGA_per_min 
    h[ :o_DFGM_per_min ] = @DFGM_per_min 
    h[ :o_DFGA_per_min ] = @DFGA_per_min 
    return h
  end
end

class TimeStats
  attr_accessor :games_played, :wins, :losses, :ties, :win_pct, :seconds_played #:total_time_played, 

  def initialize
    @games_played = 0
    @wins = 0
    @losses = 0
    @ties = 0
    @win_pct = 0
    @seconds_played = StatSet.new( :seconds_played )
  end

  def +( y )
    @seconds_played + y.seconds_played 
    @games_played = @games_played + y.games_played
    @wins = @wins + y.wins
    @losses = @losses + y.losses
    @ties = @ties + y.ties

    if 0 == @games_played
      @win_pct = 0.1
    else
      @win_pct = (@wins / @games_played).to_f
    end
  end

  def calcAvg( y, numGames )
    @seconds_played.calcAvg( y.seconds_played , numGames )
    #jlk - these should be converted i guess to statsets
    #@wins.calcAvg( y.wins, numGames )
    #@losses.calcAvg( y.losses, numGames )
    #@ties.calcAvg( y.ties, numGames )

    @games_played = numGames

    if 0 == @games_played
      @win_pct = 0.1
    else
      @win_pct = (@wins / @games_played).to_f
    end
  end

  def to_h
    h = {}
    h [ :seconds_played_total ] = @seconds_played.total #@total_time_played.total_minutes
    h [ :games_played ] = @games_played
    h [ :wins ] = @wins
    h [ :losses ] = @losses
    h [ :ties ] = @ties
    h [ :win_pct ] = @win_pct
    h = h.merge @seconds_played.to_h

    return h
  end

  def to_opponent_h
    h = {}
    h [ :o_seconds_played_total ] = @seconds_played.total #@total_time_played.total_minutes
    h [ :o_games_played ] = @games_played
    h [ :o_wins ] = @wins
    h [ :o_losses ] = @losses
    h [ :o_ties ] = @ties
    h [ :o_win_pct ] = @win_pct
    h = h.merge @seconds_played.to_opponent_h

    return h
  end
end

class XYStats
  attr_accessor :game_id, :FGM, :FGA, :FG_PCT, :FG3M, :FG3A, :FG3_PCT, :FG2M, :FG2A, :FG2_PCT, :FTM, :FTA, :FT_PCT, :OREB, :DREB, :REB, :AST, :STL, :BLK, :TOV, :PF, :PTS, :PLUS_MINUS

  def initialize
    total_b2b_PTS = 0; 
    total_b2b_OREB = 0; 
    total_b2b_OREB_PCT = 0; 
    total_b2b_DREB = 0; 
    total_b2b_DREB_PCT = 0; 
    total_b2b_STL = 0; 
    total_b2b_AST = 0; 
    total_b2b_BLK = 0; 
    total_b2b_TOV = 0; 
    total_b2b_SECONDS = 0; 
    num_b2b_games = 0; 
    total_non_b2b_PTS = 0; 
    total_non_b2b_OREB = 0; 
    total_non_b2b_OREB_PCT = 0; 
    total_non_b2b_DREB = 0; 
    total_non_b2b_DREB_PCT = 0; 
    total_non_b2b_STL = 0; 
    total_non_b2b_AST = 0; 
    total_non_b2b_BLK = 0; 
    total_non_b2b_TOV = 0; 
    total_non_b2b_SECONDS = 0; 
    num_non_b2b_games = 0; 
    num_b2b_opp_games = 0; 
    num_non_b2b_opp_games = 0; 
    num_threeg4d_games = 0;
    num_extra_rest_games = 0;
    num_opp_threeg4d_games = 0;
    num_opp_extra_rest_games = 0;
    
    total_threeg4d_PTS = 0; 
    total_threeg4d_OREB = 0; 
    total_threeg4d_OREB_PCT = 0; 
    total_threeg4d_DREB = 0; 
    total_threeg4d_DREB_PCT = 0; 
    total_threeg4d_STL = 0; 
    total_threeg4d_AST = 0; 
    total_threeg4d_BLK = 0; 
    total_threeg4d_TOV = 0; 
    total_threeg4d_SECONDS = 0; 
    num_threeg4d_games = 0; 
    total_extra_rest_PTS = 0; 
    total_extra_rest_OREB = 0; 
    total_extra_rest_OREB_PCT = 0; 
    total_extra_rest_DREB = 0; 
    total_extra_rest_DREB_PCT = 0; 
    total_extra_rest_STL = 0; 
    total_extra_rest_AST = 0; 
    total_extra_rest_BLK = 0; 
    total_extra_rest_TOV = 0; 
    total_extra_rest_SECONDS = 0; 
    num_extra_rest_games = 0
    total_opp_threeg4d_PTS = 0; 
    total_opp_threeg4d_OREB = 0; 
    total_opp_threeg4d_OREB_PCT = 0; 
    total_opp_threeg4d_DREB = 0; 
    total_opp_threeg4d_DREB_PCT = 0; 
    total_opp_threeg4d_STL = 0; 
    total_opp_threeg4d_AST = 0; 
    total_opp_threeg4d_BLK = 0; 
    total_opp_threeg4d_TOV = 0; 
    total_opp_threeg4d_SECONDS = 0; 
    num_opp_threeg4d_games = 0; 
    total_opp_extra_rest_PTS = 0; 
    total_opp_extra_rest_OREB = 0; 
    total_opp_extra_rest_OREB_PCT = 0; 
    total_opp_extra_rest_DREB = 0; 
    total_opp_extra_rest_DREB_PCT = 0; 
    total_opp_extra_rest_STL = 0; 
    total_opp_extra_rest_AST = 0; 
    total_opp_extra_rest_BLK = 0; 
    total_opp_extra_rest_TOV = 0; 
    total_opp_extra_rest_SECONDS = 0; 
    num_opp_extra_rest_games = 0
    total_front_b2b_PTS = 0; 
    total_front_b2b_OREB = 0; 
    total_front_b2b_OREB_PCT = 0; 
    total_front_b2b_DREB = 0; 
    total_front_b2b_DREB_PCT = 0; 
    total_front_b2b_STL = 0; 
    total_front_b2b_AST = 0; 
    total_front_b2b_BLK = 0; 
    total_front_b2b_TOV = 0; 
    total_front_b2b_SECONDS = 0; 
    num_front_b2b_games = 0; 
    total_non_front_b2b_PTS = 0; 
    total_non_front_b2b_OREB = 0; 
    total_non_front_b2b_OREB_PCT = 0; 
    total_non_front_b2b_DREB = 0; 
    total_non_front_b2b_DREB_PCT = 0; 
    total_non_front_b2b_STL = 0; 
    total_non_front_b2b_AST = 0; 
    total_non_front_b2b_BLK = 0; 
    total_non_front_b2b_TOV = 0; 
    total_non_front_b2b_SECONDS = 0; 
    num_non_front_b2b_games = 0; 
    total_opp_b2b_PTS = 0; 
    total_opp_b2b_OREB = 0; 
    total_opp_b2b_OREB_PCT = 0; 
    total_opp_b2b_DREB = 0; 
    total_opp_b2b_DREB_PCT = 0; 
    total_opp_b2b_STL = 0; 
    total_opp_b2b_AST = 0; 
    total_opp_b2b_BLK = 0; 
    total_opp_b2b_TOV = 0; 
    total_opp_b2b_SECONDS = 0; 
    num_opp_b2b_games = 0; 
    total_opp_front_b2b_PTS = 0; 
    total_opp_front_b2b_OREB = 0; 
    total_opp_front_b2b_OREB_PCT = 0; 
    total_opp_front_b2b_DREB = 0; 
    total_opp_front_b2b_DREB_PCT = 0; 
    total_opp_front_b2b_STL = 0; 
    total_opp_front_b2b_AST = 0; 
    total_opp_front_b2b_BLK = 0; 
    total_opp_front_b2b_TOV = 0; 
    total_opp_front_b2b_SECONDS = 0; 
    num_opp_front_b2b_games = 0; 
    total_opp_non_front_b2b_PTS = 0; 
    total_opp_non_front_b2b_OREB = 0; 
    total_opp_non_front_b2b_OREB_PCT = 0; 
    total_opp_non_front_b2b_DREB = 0; 
    total_opp_non_front_b2b_DREB_PCT = 0; 
    total_opp_non_front_b2b_STL = 0; 
    total_opp_non_front_b2b_AST = 0; 
    total_opp_non_front_b2b_BLK = 0; 
    total_opp_non_front_b2b_TOV = 0; 
    total_opp_non_front_b2b_SECONDS= 0; 
    num_opp_non_front_b2b_games = 0; 
    total_opp_non_b2b_PTS = 0; 
    total_opp_non_b2b_OREB = 0; 
    total_opp_non_b2b_OREB_PCT = 0; 
    total_opp_non_b2b_DREB = 0; 
    total_opp_non_b2b_DREB_PCT = 0; 
    total_opp_non_b2b_STL = 0; 
    total_opp_non_b2b_AST = 0; 
    total_opp_non_b2b_BLK = 0; 
    total_opp_non_b2b_TOV = 0; 
    total_opp_non_b2b_SECONDS = 0; 
    num_opp_non_b2b_games = 0; 

    mean_starter_seconds = 0;
    mean_starter_pts = 0;
    mean_starter_oreb = 0;
    mean_starter_dreb = 0;
    mean_starter_ast = 0;
    mean_starter_tov = 0;
    mean_starter_blk = 0;
    mean_starter_stl = 0;
    mean_starter_pts_per_min = 0;
    mean_starter_oreb_per_min = 0;
    mean_starter_dreb_per_min = 0;mean_starter_ast_per_min = 0;mean_starter_tov_per_min = 0;mean_starter_blk_per_min = 0;mean_starter_stl_per_min = 0;starter_pts_effect = 0;starter_oreb_effect = 0;starter_dreb_effect = 0;starter_ast_effect = 0;starter_tov_effect = 0;starter_blk_effect = 0;starter_stl_effect = 0;starter_pts_effect_per_min = 0;starter_oreb_effect_per_min = 0;starter_dreb_effect_per_min = 0;starter_ast_effect_per_min = 0;starter_tov_effect_per_min = 0;starter_blk_effect_per_min = 0;starter_stl_effect_per_min = 0
    mean_bench_seconds = 0;mean_bench_pts = 0;mean_bench_oreb = 0;mean_bench_dreb = 0;mean_bench_ast = 0;mean_bench_tov = 0;mean_bench_blk = 0;mean_bench_stl = 0;mean_bench_pts_per_min = 0;mean_bench_oreb_per_min = 0;mean_bench_dreb_per_min = 0;mean_bench_ast_per_min = 0;mean_bench_tov_per_min = 0;mean_bench_blk_per_min = 0;mean_bench_stl_per_min = 0;bench_pts_effect = 0;bench_oreb_effect = 0;bench_dreb_effect = 0;bench_ast_effect = 0;bench_tov_effect = 0;bench_blk_effect = 0;bench_stl_effect = 0;bench_pts_effect_per_min = 0;bench_oreb_effect_per_min = 0;bench_dreb_effect_per_min = 0;bench_ast_effect_per_min = 0;bench_tov_effect_per_min = 0;bench_blk_effect_per_min = 0;bench_stl_effect_per_min = 0
    mean_starterbench_seconds = 0;mean_starterbench_pts = 0;mean_starterbench_oreb = 0;mean_starterbench_dreb = 0;mean_starterbench_ast = 0;mean_starterbench_tov = 0;mean_starterbench_blk = 0;mean_starterbench_stl = 0;mean_starterbench_pts_per_min = 0;mean_starterbench_oreb_per_min = 0;mean_starterbench_dreb_per_min = 0;mean_starterbench_ast_per_min = 0;mean_starterbench_tov_per_min = 0;mean_starterbench_blk_per_min = 0;mean_starterbench_stl_per_min = 0;starterbench_pts_effect = 0;starterbench_oreb_effect = 0;starterbench_dreb_effect = 0;starterbench_ast_effect = 0;starterbench_tov_effect = 0;starterbench_blk_effect = 0;starterbench_stl_effect = 0;starterbench_pts_effect_per_min = 0;starterbench_oreb_effect_per_min = 0;starterbench_dreb_effect_per_min = 0;starterbench_ast_effect_per_min = 0;starterbench_tov_effect_per_min = 0;starterbench_blk_effect_per_min = 0;starterbench_stl_effect_per_min = 0

    prev_pts = 0;prev_oreb = 0;prev_dreb = 0;prev_stl = 0;prev_blk = 0;prev_ast = 0;prev_tov = 0;prev_seconds = 0;prev_pts_per_min = 0;prev_oreb_per_min = 0;prev_dreb_per_min = 0;prev_stl_per_min = 0;prev_blk_per_min = 0;prev_ast_per_min = 0;prev_tov_per_min = 0
    prev2_pts = 0;prev2_oreb = 0;prev2_dreb = 0;prev2_stl = 0;prev2_blk = 0;prev2_ast = 0;prev2_tov = 0;prev2_seconds = 0;prev2_pts_per_min = 0;prev2_oreb_per_min = 0;prev2_dreb_per_min = 0;prev2_stl_per_min = 0;prev2_blk_per_min = 0;prev2_ast_per_min = 0;prev2_tov_per_min = 0
    prev5_pts = 0;prev5_oreb = 0;prev5_dreb = 0;prev5_stl = 0;prev5_blk = 0;prev5_ast = 0;prev5_tov = 0;prev5_seconds = 0;prev5_pts_per_min = 0;prev5_oreb_per_min = 0;prev5_dreb_per_min = 0;prev5_stl_per_min = 0;prev5_blk_per_min = 0;prev5_ast_per_min = 0;prev5_tov_per_min = 0


    ft_pct = 0;expected_FTA = 0;expected_FTM = 0;expected_FTM_per_min = 0;extra_FTA = 0;ft_effect = 0;ft_effect_per_min = 0;prev_seconds_delta = 0;prev2_seconds_delta = 0;prev5_seconds_delta = 0;prev_pts_delta = 0;prev2_pts_delta = 0;prev5_pts_delta = 0;prev_oreb_delta = 0;prev2_oreb_delta = 0;prev5_oreb_delta = 0;prev_dreb_delta = 0;prev2_dreb_delta = 0;prev5_dreb_delta = 0;prev_ast_delta = 0;prev2_ast_delta = 0;prev5_ast_delta = 0;prev_tov_delta = 0;prev2_tov_delta = 0;prev5_tov_delta = 0;prev_blk_delta = 0;prev2_blk_delta = 0;prev5_blk_delta = 0;prev_stl_delta = 0;prev2_stl_delta = 0;prev5_stl_delta = 0;prev_pts_delta_per_min = 0;prev2_pts_delta_per_min = 0;prev5_pts_delta_per_min = 0;prev_oreb_delta_per_min = 0;prev2_oreb_delta_per_min = 0;prev5_oreb_delta_per_min = 0;prev_dreb_delta_per_min = 0;prev2_dreb_delta_per_min = 0;prev5_dreb_delta_per_min = 0;prev_ast_delta_per_min = 0;prev2_ast_delta_per_min = 0;prev5_ast_delta_per_min = 0;prev_tov_delta_per_min = 0;prev2_tov_delta_per_min = 0;prev5_tov_delta_per_min = 0;prev_blk_delta_per_min = 0;prev2_blk_delta_per_min = 0;prev5_blk_delta_per_min = 0;prev_stl_delta_per_min = 0;prev2_stl_delta_per_min = 0;prev5_stl_delta_per_min = 0

    expected_OREB = 0;expected_OREB_effect = 0;expected_oreb_per_min = 0;expected_oreb_effect_per_min = 0;expected_DREB = 0;expected_DREB_effect = 0;expected_dreb_per_min = 0;expected_dreb_effect_per_min = 0;expected_AST = 0;expected_AST_effect = 0;expected_assist_per_min = 0;expected_assist_effect_per_min = 0;expected_TOV = 0;expected_TOV_effect = 0;expected_turnover_per_min = 0;expected_turnover_effect_per_min = 0;expected_BLK = 0;expected_BLK_effect = 0;expected_block_per_min = 0;expected_block_effect_per_min = 0;expected_STL = 0;expected_STL_effect = 0;expected_steal_per_min = 0;expected_steal_effect_per_min = 0;scaled_oreb_pct = 0;scaled_oreb_pct_effect = 0;scaled_dreb_pct = 0;scaled_dreb_pct_effect = 0;scaled_assist_pct = 0;scaled_assist_pct_effect = 0;scaled_turnover_pct = 0;scaled_turnover_pct_effect = 0;scaled_block_pct = 0;scaled_block_pct_effect = 0;scaled_pct_stl = 0;scaled_pct_stl_effect = 0;scaled_pct_stl_effect_per_min = 0;scaled_oreb = 0;scaled_oreb_effect = 0;scaled_oreb_per_min = 0;scaled_oreb_effect_per_min = 0;scaled_dreb = 0;scaled_dreb_effect = 0;scaled_dreb_per_min = 0;scaled_dreb_effect_per_min = 0;scaled_assist = 0;scaled_assist_effect = 0;scaled_assist_per_min = 0;scaled_assist_effect_per_min = 0;scaled_turnover = 0;scaled_turnover_effect = 0;scaled_turnover_per_min = 0;scaled_turnover_effect_per_min = 0;scaled_block = 0;scaled_block_effect = 0;scaled_block_per_min = 0;scaled_block_effect_per_min = 0;scaled_steal = 0;scaled_steal_effect = 0;scaled_steal_per_min = 0;scaled_steal_effect_per_min = 0;scaled_block_pct_effect = 0;scaled_block_pct_effect_per_min = 0

    modded_oreb = 0;modded_oreb_effect = 0;modded_oreb_per_min = 0;modded_oreb_effect_per_min = 0;modded_dreb = 0;modded_dreb_effect = 0;modded_dreb_per_min = 0;modded_dreb_effect_per_min = 0;modded_assist = 0;modded_assist_effect = 0;modded_assist_per_min = 0;modded_assist_effect_per_min = 0;modded_turnover = 0;modded_turnover_effect = 0;modded_turnover_per_min = 0;modded_turnover_effect_per_min = 0;modded_block = 0;modded_block_effect = 0;modded_block_per_min = 0;modded_block_effect_per_min = 0;modded_steal = 0;modded_steal_effect = 0;modded_steal_per_min = 0;modded_steal_effect_per_min = 0
    @game_id = nil
    @FGM = StatSet.new( :FGM )
    @FGA = StatSet.new( :FGA )
    @FG_PCT = StatSet.new( :FG_PCT )
    @FG3M = StatSet.new( :FG3M )
    @FG3A = StatSet.new( :FG3A )
    @FG3_PCT = StatSet.new( :FG3_PCT )
    @FG2M = StatSet.new( :FG2M )
    @FG2A = StatSet.new( :FG2A )
    @FG2_PCT = StatSet.new( :FG2_PCT )
    @FTM = StatSet.new( :FTM )
    @FTA = StatSet.new( :FTA )
    @FT_PCT = StatSet.new( :FT_PCT )
    @OREB = StatSet.new( :OREB )
    @DREB = StatSet.new( :DREB )
    @REB = StatSet.new( :REB )
    @AST = StatSet.new( :AST )
    @STL = StatSet.new( :STL )
    @BLK = StatSet.new( :BLK )
    @TOV = StatSet.new( :TOV )
    @PF = StatSet.new( :PF )
    @PTS = StatSet.new( :PTS )
    @PLUS_MINUS = StatSet.new( :PLUS_MINUS )
  end

  def +( y )
    @FGM + y.FGM 
    @FGA + y.FGA 
    @FG_PCT + y.FG_PCT 
    @FG3M + y.FG3M 
    @FG3A + y.FG3A 
    @FG3_PCT + y.FG3_PCT 
    @FG2M + y.FG2M 
    @FG2A + y.FG2A 
    @FG2_PCT + y.FG2_PCT 
    @FTM + y.FTM 
    @FTA + y.FTA 
    @FT_PCT + y.FT_PCT 
    @OREB + y.OREB 
    @DREB + y.DREB 
    @REB + y.REB 
    @AST + y.AST 
    @STL + y.STL 
    @BLK + y.BLK 
    @TOV + y.TOV 
    @PF + y.PF 
    @PTS + y.PTS 
    @PLUS_MINUS + y.PLUS_MINUS 
  end

  def calcAvg( y, numGames )
    @FGM.calcAvg( y.FGM , numGames )
    @FGA.calcAvg( y.FGA , numGames )
    @FG_PCT.calcAvg( y.FG_PCT , numGames )
    @FG3M.calcAvg( y.FG3M , numGames )
    @FG3A.calcAvg( y.FG3A , numGames )
    @FG3_PCT.calcAvg( y.FG3_PCT , numGames )
    @FG2M.calcAvg( y.FG2M , numGames )
    @FG2A.calcAvg( y.FG2A , numGames )
    @FG2_PCT.calcAvg( y.FG2_PCT , numGames )
    @FTM.calcAvg( y.FTM , numGames )
    @FTA.calcAvg( y.FTA , numGames )
    @FT_PCT.calcAvg( y.FT_PCT , numGames )
    @OREB.calcAvg( y.OREB , numGames )
    @DREB.calcAvg( y.DREB , numGames )
    @REB.calcAvg( y.REB , numGames )
    @AST.calcAvg( y.AST , numGames )
    @STL.calcAvg( y.STL , numGames )
    @BLK.calcAvg( y.BLK , numGames )
    @TOV.calcAvg( y.TOV , numGames )
    @PF.calcAvg( y.PF , numGames )
    @PTS.calcAvg( y.PTS , numGames )
    @PLUS_MINUS.calcAvg( y.PLUS_MINUS , numGames )
  end

  def to_h
    h = {}

    h = h.merge @FGM.to_h
    h = h.merge @FGA.to_h
    h = h.merge @FG_PCT.to_h
    h = h.merge @FG3M.to_h
    h = h.merge @FG3A.to_h
    h = h.merge @FG3_PCT.to_h
    h = h.merge @FG2M.to_h
    h = h.merge @FG2A.to_h
    h = h.merge @FG2_PCT.to_h
    h = h.merge @FTM.to_h
    h = h.merge @FTA.to_h
    h = h.merge @FT_PCT.to_h
    h = h.merge @OREB.to_h
    h = h.merge @DREB.to_h
    h = h.merge @REB.to_h
    h = h.merge @AST.to_h
    h = h.merge @STL.to_h
    h = h.merge @BLK.to_h
    h = h.merge @TOV.to_h
    h = h.merge @PF.to_h
    h = h.merge @PTS.to_h
    h = h.merge @PLUS_MINUS.to_h
    return h
  end

  def to_opponent_h
    h = {}

    h = h.merge @FGM.to_opponent_h
    h = h.merge @FGA.to_opponent_h
    h = h.merge @FG_PCT.to_opponent_h
    h = h.merge @FG3M.to_opponent_h
    h = h.merge @FG3A.to_opponent_h
    h = h.merge @FG3_PCT.to_opponent_h
    h = h.merge @FG2M.to_opponent_h
    h = h.merge @FG2A.to_opponent_h
    h = h.merge @FG2_PCT.to_opponent_h
    h = h.merge @FTM.to_opponent_h
    h = h.merge @FTA.to_opponent_h
    h = h.merge @FT_PCT.to_opponent_h
    h = h.merge @OREB.to_opponent_h
    h = h.merge @DREB.to_opponent_h
    h = h.merge @REB.to_opponent_h
    h = h.merge @AST.to_opponent_h
    h = h.merge @STL.to_opponent_h
    h = h.merge @BLK.to_opponent_h
    h = h.merge @TOV.to_opponent_h
    h = h.merge @PF.to_opponent_h
    h = h.merge @PTS.to_opponent_h
    h = h.merge @PLUS_MINUS.to_opponent_h
    return h
  end
end


class GamelogStats
  attr_accessor :game_id, :FGM, :FGA, :FG_PCT, :FG3M, :FG3A, :FG3_PCT, :FG2M, :FG2A, :FG2_PCT, :FTM, :FTA, :FT_PCT, :OREB, :DREB, :REB, :AST, :STL, :BLK, :TOV, :PF, :PTS, :PLUS_MINUS

  def initialize
    @game_id = nil
    @FGM = StatSet.new( :FGM )
    @FGA = StatSet.new( :FGA )
    @FG_PCT = StatSet.new( :FG_PCT )
    @FG3M = StatSet.new( :FG3M )
    @FG3A = StatSet.new( :FG3A )
    @FG3_PCT = StatSet.new( :FG3_PCT )
    @FG2M = StatSet.new( :FG2M )
    @FG2A = StatSet.new( :FG2A )
    @FG2_PCT = StatSet.new( :FG2_PCT )
    @FTM = StatSet.new( :FTM )
    @FTA = StatSet.new( :FTA )
    @FT_PCT = StatSet.new( :FT_PCT )
    @OREB = StatSet.new( :OREB )
    @DREB = StatSet.new( :DREB )
    @REB = StatSet.new( :REB )
    @AST = StatSet.new( :AST )
    @STL = StatSet.new( :STL )
    @BLK = StatSet.new( :BLK )
    @TOV = StatSet.new( :TOV )
    @PF = StatSet.new( :PF )
    @PTS = StatSet.new( :PTS )
    @PLUS_MINUS = StatSet.new( :PLUS_MINUS )
  end

  def +( y )
    @FGM + y.FGM 
    @FGA + y.FGA 
    @FG_PCT + y.FG_PCT 
    @FG3M + y.FG3M 
    @FG3A + y.FG3A 
    @FG3_PCT + y.FG3_PCT 
    @FG2M + y.FG2M 
    @FG2A + y.FG2A 
    @FG2_PCT + y.FG2_PCT 
    @FTM + y.FTM 
    @FTA + y.FTA 
    @FT_PCT + y.FT_PCT 
    @OREB + y.OREB 
    @DREB + y.DREB 
    @REB + y.REB 
    @AST + y.AST 
    @STL + y.STL 
    @BLK + y.BLK 
    @TOV + y.TOV 
    @PF + y.PF 
    @PTS + y.PTS 
    @PLUS_MINUS + y.PLUS_MINUS 
  end

  def calcAvg( y, numGames )
    @FGM.calcAvg( y.FGM , numGames )
    @FGA.calcAvg( y.FGA , numGames )
    @FG_PCT.calcAvg( y.FG_PCT , numGames )
    @FG3M.calcAvg( y.FG3M , numGames )
    @FG3A.calcAvg( y.FG3A , numGames )
    @FG3_PCT.calcAvg( y.FG3_PCT , numGames )
    @FG2M.calcAvg( y.FG2M , numGames )
    @FG2A.calcAvg( y.FG2A , numGames )
    @FG2_PCT.calcAvg( y.FG2_PCT , numGames )
    @FTM.calcAvg( y.FTM , numGames )
    @FTA.calcAvg( y.FTA , numGames )
    @FT_PCT.calcAvg( y.FT_PCT , numGames )
    @OREB.calcAvg( y.OREB , numGames )
    @DREB.calcAvg( y.DREB , numGames )
    @REB.calcAvg( y.REB , numGames )
    @AST.calcAvg( y.AST , numGames )
    @STL.calcAvg( y.STL , numGames )
    @BLK.calcAvg( y.BLK , numGames )
    @TOV.calcAvg( y.TOV , numGames )
    @PF.calcAvg( y.PF , numGames )
    @PTS.calcAvg( y.PTS , numGames )
    @PLUS_MINUS.calcAvg( y.PLUS_MINUS , numGames )
  end

  def to_h
    h = {}

    h = h.merge @FGM.to_h
    h = h.merge @FGA.to_h
    h = h.merge @FG_PCT.to_h
    h = h.merge @FG3M.to_h
    h = h.merge @FG3A.to_h
    h = h.merge @FG3_PCT.to_h
    h = h.merge @FG2M.to_h
    h = h.merge @FG2A.to_h
    h = h.merge @FG2_PCT.to_h
    h = h.merge @FTM.to_h
    h = h.merge @FTA.to_h
    h = h.merge @FT_PCT.to_h
    h = h.merge @OREB.to_h
    h = h.merge @DREB.to_h
    h = h.merge @REB.to_h
    h = h.merge @AST.to_h
    h = h.merge @STL.to_h
    h = h.merge @BLK.to_h
    h = h.merge @TOV.to_h
    h = h.merge @PF.to_h
    h = h.merge @PTS.to_h
    h = h.merge @PLUS_MINUS.to_h
    return h
  end

  def to_opponent_h
    h = {}

    h = h.merge @FGM.to_opponent_h
    h = h.merge @FGA.to_opponent_h
    h = h.merge @FG_PCT.to_opponent_h
    h = h.merge @FG3M.to_opponent_h
    h = h.merge @FG3A.to_opponent_h
    h = h.merge @FG3_PCT.to_opponent_h
    h = h.merge @FG2M.to_opponent_h
    h = h.merge @FG2A.to_opponent_h
    h = h.merge @FG2_PCT.to_opponent_h
    h = h.merge @FTM.to_opponent_h
    h = h.merge @FTA.to_opponent_h
    h = h.merge @FT_PCT.to_opponent_h
    h = h.merge @OREB.to_opponent_h
    h = h.merge @DREB.to_opponent_h
    h = h.merge @REB.to_opponent_h
    h = h.merge @AST.to_opponent_h
    h = h.merge @STL.to_opponent_h
    h = h.merge @BLK.to_opponent_h
    h = h.merge @TOV.to_opponent_h
    h = h.merge @PF.to_opponent_h
    h = h.merge @PTS.to_opponent_h
    h = h.merge @PLUS_MINUS.to_opponent_h
    return h
  end
end

class AdvancedStats
  attr_accessor :TS_PCT, :EFG_PCT, :OREB_PCT, :DREB_PCT, :REB_PCT, :AST_PCT, :TO_PCT, :USG_PCT, :OFF_RATING, :DEF_RATING, :NET_RATING, :AST_TOV, :AST_RATIO, :PACE, :PIE

  def initialize
    @TS_PCT = StatSet.new( :TS_PCT )
    @EFG_PCT = StatSet.new( :EFG_PCT )
    @OREB_PCT = StatSet.new( :OREB_PCT )
    @DREB_PCT = StatSet.new( :DREB_PCT )
    @REB_PCT = StatSet.new( :REB_PCT )
    @AST_PCT = StatSet.new( :AST_PCT )
    @TO_PCT = StatSet.new( :TO_PCT )
    @USG_PCT = StatSet.new( :USG_PCT )
    @OFF_RATING = StatSet.new( :OFF_RATING )
    @DEF_RATING = StatSet.new( :DEF_RATING )
    @NET_RATING = StatSet.new( :NET_RATING )
    @AST_TOV = StatSet.new( :AST_TOV )
    @AST_RATIO = StatSet.new( :AST_RATIO )
    @PACE = StatSet.new( :PACE )
    @PIE = StatSet.new( :PIE )
  end

  def +( y )
    @TS_PCT + y.TS_PCT 
    @EFG_PCT + y.EFG_PCT 
    @OREB_PCT + y.OREB_PCT 
    @DREB_PCT + y.DREB_PCT 
    @REB_PCT + y.REB_PCT 
    @AST_PCT + y.AST_PCT 
    @TO_PCT + y.TO_PCT 
    @USG_PCT + y.USG_PCT 
    @OFF_RATING + y.OFF_RATING 
    @DEF_RATING + y.DEF_RATING 
    @NET_RATING + y.NET_RATING 
    @AST_TOV + y.AST_TOV 
    @AST_RATIO + y.AST_RATIO 
    @PACE + y.PACE 
    @PIE + y.PIE 
  end

  def calcAvg( y, numGames )
    @TS_PCT.calcAvg( y.TS_PCT , numGames )
    @EFG_PCT.calcAvg( y.EFG_PCT , numGames )
    @OREB_PCT.calcAvg( y.OREB_PCT , numGames )
    @DREB_PCT.calcAvg( y.DREB_PCT , numGames )
    @REB_PCT.calcAvg( y.REB_PCT , numGames )
    @AST_PCT.calcAvg( y.AST_PCT , numGames )
    @TO_PCT.calcAvg( y.TO_PCT , numGames )
    @USG_PCT.calcAvg( y.USG_PCT , numGames )
    @OFF_RATING.calcAvg( y.OFF_RATING , numGames )
    @DEF_RATING.calcAvg( y.DEF_RATING , numGames )
    @NET_RATING.calcAvg( y.NET_RATING , numGames )
    @AST_TOV.calcAvg( y.AST_TOV , numGames )
    @AST_RATIO.calcAvg( y.AST_RATIO , numGames )
    @PACE.calcAvg( y.PACE , numGames )
    @PIE.calcAvg( y.PIE , numGames )
  end

  def to_h
    h = {}
    h = h.merge @TS_PCT.to_h
    h = h.merge @EFG_PCT.to_h
    h = h.merge @OREB_PCT.to_h
    h = h.merge @DREB_PCT.to_h
    h = h.merge @REB_PCT.to_h
    h = h.merge @AST_PCT.to_h
    h = h.merge @TO_PCT.to_h
    h = h.merge @USG_PCT.to_h
    h = h.merge @OFF_RATING.to_h
    h = h.merge @DEF_RATING.to_h
    h = h.merge @NET_RATING.to_h
    h = h.merge @AST_TOV.to_h
    h = h.merge @AST_RATIO.to_h
    h = h.merge @PACE.to_h
    h = h.merge @PIE.to_h

    return h
  end

  def to_opponent_h
    h = {}
    h = h.merge @TS_PCT.to_opponent_h
    h = h.merge @EFG_PCT.to_opponent_h
    h = h.merge @OREB_PCT.to_opponent_h
    h = h.merge @DREB_PCT.to_opponent_h
    h = h.merge @REB_PCT.to_opponent_h
    h = h.merge @AST_PCT.to_opponent_h
    h = h.merge @TO_PCT.to_opponent_h
    h = h.merge @USG_PCT.to_opponent_h
    h = h.merge @OFF_RATING.to_opponent_h
    h = h.merge @DEF_RATING.to_opponent_h
    h = h.merge @NET_RATING.to_opponent_h
    h = h.merge @AST_TOV.to_opponent_h
    h = h.merge @AST_RATIO.to_opponent_h
    h = h.merge @PACE.to_opponent_h
    h = h.merge @PIE.to_opponent_h

    return h
  end
end

class MiscStats
  attr_accessor :PTS_OFF_TOV, :PTS_2ND_CHANCE, :PTS_FB, :PTS_PAINT, :BLKA, :PFD
  attr_accessor :o_PTS_OFF_TOV, :o_PTS_2ND_CHANCE, :o_PTS_FB, :o_PTS_PAINT, :o_PTS_2PT_MR

  def initialize
    @PTS_OFF_TOV = StatSet.new( :PTS_OFF_TOV )
    @PTS_2ND_CHANCE = StatSet.new( :PTS_2ND_CHANCE )
    @PTS_FB = StatSet.new( :PTS_FB )
    @PTS_PAINT = StatSet.new( :PTS_PAINT )
    @BLKA = StatSet.new( :BLKA )
    @PFD = StatSet.new( :PFD )

    @o_PTS_OFF_TOV = StatSet.new( :o_PTS_OFF_TOV )
    @o_PTS_2ND_CHANCE = StatSet.new( :o_PTS_2ND_CHANCE )
    @o_PTS_FB = StatSet.new( :o_PTS_FB )
    @o_PTS_PAINT = StatSet.new( :o_PTS_PAINT )
    @o_PTS_2PT_MR = StatSet.new( :o_PTS_2PT_MR )
  end

  def +( y )
    @PTS_OFF_TOV + y.PTS_OFF_TOV 
    @PTS_2ND_CHANCE + y.PTS_2ND_CHANCE 
    @PTS_FB + y.PTS_FB 
    @PTS_PAINT + y.PTS_PAINT 
    @BLKA + y.BLKA 
    @PFD + y.PFD 

    @o_PTS_OFF_TOV + y.o_PTS_OFF_TOV
    @o_PTS_2ND_CHANCE + y.o_PTS_2ND_CHANCE
    @o_PTS_FB + y.o_PTS_FB
    @o_PTS_PAINT + y.o_PTS_PAINT
    @o_PTS_2PT_MR + y.o_PTS_2PT_MR
  end

  def calcAvg( y, numGames )
    @PTS_OFF_TOV.calcAvg( y.PTS_OFF_TOV , numGames )
    @PTS_2ND_CHANCE.calcAvg( y.PTS_2ND_CHANCE , numGames )
    @PTS_FB.calcAvg( y.PTS_FB , numGames )
    @PTS_PAINT.calcAvg( y.PTS_PAINT , numGames )
    @BLKA.calcAvg( y.BLKA , numGames )
    @PFD.calcAvg( y.PFD , numGames )

    @o_PTS_OFF_TOV.calcAvg( y.o_PTS_OFF_TOV, numGames )
    @o_PTS_2ND_CHANCE.calcAvg( y.o_PTS_2ND_CHANCE, numGames )
    @o_PTS_FB.calcAvg( y.o_PTS_FB, numGames )
    @o_PTS_PAINT.calcAvg( y.o_PTS_PAINT, numGames )
    @o_PTS_2PT_MR.calcAvg( y.o_PTS_2PT_MR, numGames )
  end

  def to_h
    h = {}
    h = h.merge @PTS_OFF_TOV.to_h
    h = h.merge @PTS_2ND_CHANCE.to_h
    h = h.merge @PTS_FB.to_h
    h = h.merge @PTS_PAINT.to_h
    h = h.merge @BLKA.to_h
    h = h.merge @PFD.to_h
    
    if @o_PTS_OFF_TOV.hash.size > 0
      h = h.merge @o_PTS_OFF_TOV.to_h
    end
    if @o_PTS_2ND_CHANCE.hash.size > 0
      h = h.merge @o_PTS_2ND_CHANCE.to_h
    end
    if @o_PTS_FB.hash.size > 0
      h = h.merge @o_PTS_FB.to_h
    end
    if @o_PTS_PAINT.hash.size > 0
      h = h.merge @o_PTS_PAINT.to_h
    end
    if @o_PTS_2PT_MR.hash.size > 0
      h = h.merge @o_PTS_2PT_MR.to_h
    end

    return h
  end

  def to_opponent_h
    h = {}
    h = h.merge @PTS_OFF_TOV.to_opponent_h
    h = h.merge @PTS_2ND_CHANCE.to_opponent_h
    h = h.merge @PTS_FB.to_opponent_h
    h = h.merge @PTS_PAINT.to_opponent_h
    h = h.merge @BLKA.to_opponent_h
    h = h.merge @PFD.to_opponent_h

    return h
  end
end

class ScoringStats
  attr_accessor :PCT_FGA_2PT, :PCT_FGA_3PT, :PCT_PTS_2PT, :PCT_PTS_2PT_MR, :PCT_PTS_3PT, :PCT_PTS_FB, :PCT_PTS_FT, :PCT_PTS_OFF_TOV, :PCT_PTS_PAINT
  attr_accessor :PTS_2PT_MR
=begin
 :PCT_FGA_2PT=>"0.933",
 :PCT_FGA_3PT=>"0.067",
 :PCT_PTS_2PT=>"0.857",
 :PCT_PTS_2PT_MR=>"0.19",
 :PCT_PTS_3PT=>"0.0",
 :PCT_PTS_FB=>"0.238",
 :PCT_PTS_FT=>"0.143",
 :PCT_PTS_OFF_TOV=>"0.143",
 :PCT_PTS_PAINT=>"0.667",
 :PCT_AST_2PM=>"0.444",
 :PCT_UAST_2PM=>"0.556",
 :PCT_AST_3PM=>"0.0",
 :PCT_UAST_3PM=>"0.0",
 :PCT_AST_FGM=>"0.444",
 :PCT_UAST_FGM=>"0.556"}
=end
  #, :PTS_FB, :PTS_OFF_TOV, :PTS_PAINT
#:FGA_2PT, :PTS_2PT, :PTS_3PT, :PTS_FT, 
  def initialize
    @PCT_FGA_2PT = StatSet.new( :PCT_FGA_2PT )
    @PCT_FGA_3PT = StatSet.new( :PCT_FGA_3PT )
    @PCT_PTS_2PT = StatSet.new( :PCT_PTS_2PT )
    @PCT_PTS_2PT_MR = StatSet.new( :PCT_PTS_2PT_MR )
    @PCT_PTS_3PT = StatSet.new( :PCT_PTS_3PT )
    @PCT_PTS_FB = StatSet.new( :PCT_PTS_FB )
    @PCT_PTS_FT = StatSet.new( :PCT_PTS_FT )
    @PCT_PTS_OFF_TOV = StatSet.new( :PCT_PTS_OFF_TOV )
    @PCT_PTS_PAINT = StatSet.new( :PCT_PTS_PAINT )

    #@FGA_2PT = StatSet.new( :FGA_2PT )
    #@PTS_2PT = StatSet.new( :PTS_2PT )
    @PTS_2PT_MR = StatSet.new( :PTS_2PT_MR )
    #@PTS_3PT = StatSet.new( :PTS_3PT )
    #@PTS_FB = StatSet.new( :PTS_FB )
    #@PTS_FT = StatSet.new( :PTS_FT )
    #@PTS_OFF_TOV = StatSet.new( :PTS_OFF_TOV )
    #@PTS_PAINT = StatSet.new( :PTS_PAINT )
  end

  def +( y )
    @PCT_FGA_2PT + y.PCT_FGA_2PT 
    @PCT_FGA_3PT + y.PCT_FGA_3PT 
    @PCT_PTS_2PT + y.PCT_PTS_2PT 
    @PCT_PTS_2PT_MR + y.PCT_PTS_2PT_MR 
    @PCT_PTS_3PT + y.PCT_PTS_3PT 
    @PCT_PTS_FB + y.PCT_PTS_FB 
    @PCT_PTS_FT + y.PCT_PTS_FT 
    @PCT_PTS_OFF_TOV + y.PCT_PTS_OFF_TOV 
    @PCT_PTS_PAINT + y.PCT_PTS_PAINT 

    #@FGA_2PT = @FGA_2PT + y.FGA_2PT 
    #@PTS_2PT = @PTS_2PT + y.PTS_2PT 
    @PTS_2PT_MR = @PTS_2PT_MR + y.PTS_2PT_MR 
    #@PTS_3PT = @PTS_3PT + y.PTS_3PT 
    #@PTS_FB = @PTS_FB + y.PTS_FB 
    #@PTS_FT = @PTS_FT + y.PTS_FT 
    #@PTS_OFF_TOV = @PTS_OFF_TOV + y.PTS_OFF_TOV 
    #@PTS_PAINT = @PTS_PAINT + y.PTS_PAINT 
  end

  def calcAvg( y, numGames )
    @PCT_FGA_2PT.calcAvg( y.PCT_FGA_2PT , numGames )
    @PCT_FGA_3PT.calcAvg( y.PCT_FGA_3PT , numGames )
    @PCT_PTS_2PT.calcAvg( y.PCT_PTS_2PT , numGames )
    @PCT_PTS_2PT_MR.calcAvg( y.PCT_PTS_2PT_MR , numGames )
    @PCT_PTS_3PT.calcAvg( y.PCT_PTS_3PT , numGames )
    @PCT_PTS_FB.calcAvg( y.PCT_PTS_FB , numGames )
    @PCT_PTS_FT.calcAvg( y.PCT_PTS_FT , numGames )
    @PCT_PTS_OFF_TOV.calcAvg( y.PCT_PTS_OFF_TOV , numGames )
    @PCT_PTS_PAINT.calcAvg( y.PCT_PTS_PAINT , numGames )

    #@FGA_2PT.calcAvg( y.FGA_2PT , numGames )
    #@PTS_2PT.calcAvg( y.PTS_2PT , numGames )
    @PTS_2PT_MR.calcAvg( y.PTS_2PT_MR , numGames )
    #@PTS_3PT.calcAvg( y.PTS_3PT , numGames )
    #@PTS_FB.calcAvg( y.PTS_FB , numGames )
    #@PTS_FT.calcAvg( y.PTS_FT , numGames )
    #@PTS_OFF_TOV.calcAvg( y.PTS_OFF_TOV , numGames )
    #@PTS_PAINT.calcAvg( y.PTS_PAINT , numGames )
  end

  def to_h
    h = {}
    #h = h.merge @PCT_FGA_2PT.to_h
    h = h.merge @PCT_FGA_3PT.to_h
    h = h.merge @PCT_PTS_2PT.to_h
    h = h.merge @PCT_PTS_2PT_MR.to_h
    h = h.merge @PCT_PTS_3PT.to_h
    h = h.merge @PCT_PTS_FB.to_h
    h = h.merge @PCT_PTS_FT.to_h
    h = h.merge @PCT_PTS_OFF_TOV.to_h
    h = h.merge @PCT_PTS_PAINT.to_h

    #h = h.merge @FGA_2PT.to_h
    #h = h.merge @PTS_2PT.to_h
    h = h.merge @PTS_2PT_MR.to_h
    #h = h.merge @PTS_3PT.to_h
    #h = h.merge @PTS_FB.to_h
    #h = h.merge @PTS_FT.to_h
    #h = h.merge @PTS_OFF_TOV.to_h
    #h = h.merge @PTS_PAINT.to_h
    return h
  end

  def to_opponent_h
    h = {}
    #h = h.merge @PCT_FGA_2PT.to_opponent_h
    h = h.merge @PCT_FGA_3PT.to_opponent_h
    h = h.merge @PCT_PTS_2PT.to_opponent_h
    h = h.merge @PCT_PTS_2PT_MR.to_opponent_h
    h = h.merge @PCT_PTS_3PT.to_opponent_h
    h = h.merge @PCT_PTS_FB.to_opponent_h
    h = h.merge @PCT_PTS_FT.to_opponent_h
    h = h.merge @PCT_PTS_OFF_TOV.to_opponent_h
    h = h.merge @PCT_PTS_PAINT.to_opponent_h

    #h = h.merge @FGA_2PT.to_opponent_h
    #h = h.merge @PTS_2PT.to_opponent_h
    h = h.merge @PTS_2PT_MR.to_opponent_h
    #h = h.merge @PTS_3PT.to_opponent_h
    #h = h.merge @PTS_FB.to_opponent_h
    #h = h.merge @PTS_FT.to_opponent_h
    #h = h.merge @PTS_OFF_TOV.to_opponent_h
    #h = h.merge @PTS_PAINT.to_opponent_h
    return h
  end
end

class ScoringDerivedStats
  attr_accessor :AST_2PM, :PCT_AST_2PM, :UAST_2PM, :PCT_UAST_2PM, :AST_3PM, :PCT_AST_3PM, :UAST_3PM, :PCT_UAST_3PM, :AST_FGM, :PCT_AST_FGM, :UAST_FGM, :PCT_UAST_FGM

  def initialize
    @AST_2PM = StatSet.new( :AST_2PM )
    @PCT_AST_2PM = StatSet.new( :PCT_AST_2PM )
    @UAST_2PM = StatSet.new( :UAST_2PM )
    @PCT_UAST_2PM = StatSet.new( :PCT_UAST_2PM )
    @AST_3PM = StatSet.new( :AST_3PM )
    @PCT_AST_3PM = StatSet.new( :PCT_AST_3PM )
    @UAST_3PM = StatSet.new( :UAST_3PM )
    @PCT_UAST_3PM = StatSet.new( :PCT_UAST_3PM )
    @AST_FGM = StatSet.new( :AST_FGM )
    @PCT_AST_FGM = StatSet.new( :PCT_AST_FGM )
    @UAST_FGM = StatSet.new( :UAST_FGM )
    @PCT_UAST_FGM = StatSet.new( :PCT_UAST_FGM )
  end

  def +( y )
    @AST_2PM + y.AST_2PM
    @PCT_AST_2PM + y.PCT_AST_2PM
    @UAST_2PM + y.UAST_2PM
    @PCT_UAST_2PM + y.PCT_UAST_2PM
    @AST_3PM + y.AST_3PM
    @PCT_AST_3PM + y.PCT_AST_3PM
    @UAST_3PM + y.UAST_3PM
    @PCT_UAST_3PM + y.PCT_UAST_3PM
    @AST_FGM + y.AST_FGM
    @PCT_AST_FGM + y.PCT_AST_FGM
    @UAST_FGM + y.UAST_FGM
    @PCT_UAST_FGM + y.PCT_UAST_FGM
  end

  def calcAvg( y, numGames )
    @AST_2PM.calcAvg( y.AST_2PM, numGames )
    @PCT_AST_2PM.calcAvg( y.PCT_AST_2PM, numGames )
    @UAST_2PM.calcAvg( y.UAST_2PM, numGames )
    @PCT_UAST_2PM.calcAvg( y.PCT_UAST_2PM, numGames )
    @AST_3PM.calcAvg( y.AST_3PM, numGames )
    @PCT_AST_3PM.calcAvg( y.PCT_AST_3PM, numGames )
    @UAST_3PM.calcAvg( y.UAST_3PM, numGames )
    @PCT_UAST_3PM.calcAvg( y.PCT_UAST_3PM, numGames )
    @AST_FGM.calcAvg( y.AST_FGM, numGames )
    @PCT_AST_FGM.calcAvg( y.PCT_AST_FGM, numGames )
    @UAST_FGM.calcAvg( y.UAST_FGM, numGames )
    @PCT_UAST_FGM.calcAvg( y.PCT_UAST_FGM, numGames )
  end

  def to_h
    h = {}
    h = h.merge @AST_2PM.to_h
    h = h.merge @PCT_AST_2PM.to_h
    h = h.merge @UAST_2PM.to_h
    h = h.merge @PCT_UAST_2PM.to_h
    h = h.merge @AST_3PM.to_h
    h = h.merge @PCT_AST_3PM.to_h
    h = h.merge @UAST_3PM.to_h
    h = h.merge @PCT_UAST_3PM.to_h
    h = h.merge @AST_FGM.to_h
    h = h.merge @PCT_AST_FGM.to_h
    h = h.merge @UAST_FGM.to_h
    h = h.merge @PCT_UAST_FGM.to_h

    return h
  end

  def to_opponent_h
    h = {}
    h = h.merge @AST_2PM.to_opponent_h
    h = h.merge @PCT_AST_2PM.to_opponent_h
    h = h.merge @UAST_2PM.to_opponent_h
    h = h.merge @PCT_UAST_2PM.to_opponent_h
    h = h.merge @AST_3PM.to_opponent_h
    h = h.merge @PCT_AST_3PM.to_opponent_h
    h = h.merge @UAST_3PM.to_opponent_h
    h = h.merge @PCT_UAST_3PM.to_opponent_h
    h = h.merge @AST_FGM.to_opponent_h
    h = h.merge @PCT_AST_FGM.to_opponent_h
    h = h.merge @UAST_FGM.to_opponent_h
    h = h.merge @PCT_UAST_FGM.to_opponent_h

    return h
  end
end

class TrackingStats
  attr_accessor :DIST, :ORBC, :DRBC, :RBC, :TCHS, :SAST, :FTAST, :PASS, :AST, :CFGM, :CFGA, :CFG_PCT, :UFGM, :UFGA, :UFG_PCT, :DFGM, :DFGA, :DFG_PCT

  def initialize
    @DIST = StatSet.new( :DIST )
    @ORBC = StatSet.new( :ORBC )
    @DRBC = StatSet.new( :DRBC )
    @RBC = StatSet.new( :RBC )
    @TCHS = StatSet.new( :TCHS )
    @SAST = StatSet.new( :SAST )
    @FTAST = StatSet.new( :FTAST )
    @PASS = StatSet.new( :PASS )
    @AST = StatSet.new( :AST )
    @CFGM = StatSet.new( :CFGM )
    @CFGA = StatSet.new( :CFGA )
    @CFG_PCT = StatSet.new( :CFG_PCT )
    @UFGM = StatSet.new( :UFGM )
    @UFGA = StatSet.new( :UFGA )
    @UFG_PCT = StatSet.new( :UFG_PCT )
    @DFGM = StatSet.new( :DFGM )
    @DFGA = StatSet.new( :DFGA )
    @DFG_PCT = StatSet.new( :DFG_PCT )
  end

  def +( y )
    @DIST + y.DIST 
    @ORBC + y.ORBC 
    @DRBC + y.DRBC 
    @RBC + y.RBC 
    @TCHS + y.TCHS 
    @SAST + y.SAST 
    @FTAST + y.FTAST 
    @PASS + y.PASS 
    @AST + y.AST 
    @CFGM + y.CFGM 
    @CFGA + y.CFGA 
    @CFG_PCT + y.CFG_PCT 
    @UFGM + y.UFGM 
    @UFGA + y.UFGA 
    @UFG_PCT + y.UFG_PCT 
    @DFGM + y.DFGM 
    @DFGA + y.DFGA 
    @DFG_PCT + y.DFG_PCT 
  end

  def calcAvg( y, numGames )
    @DIST.calcAvg( y.DIST, numGames )
    @ORBC.calcAvg( y.ORBC, numGames )
    @DRBC.calcAvg( y.DRBC, numGames )
    @RBC.calcAvg( y.RBC, numGames )
    @TCHS.calcAvg( y.TCHS, numGames )
    @SAST.calcAvg( y.SAST, numGames )
    @FTAST.calcAvg( y.FTAST, numGames )
    @PASS.calcAvg( y.PASS, numGames )
    @AST.calcAvg( y.AST, numGames )
    @CFGM.calcAvg( y.CFGM, numGames )
    @CFGA.calcAvg( y.CFGA, numGames )
    @CFG_PCT.calcAvg( y.CFG_PCT, numGames )
    @UFGM.calcAvg( y.UFGM, numGames )
    @UFGA.calcAvg( y.UFGA, numGames )
    @UFG_PCT.calcAvg( y.UFG_PCT, numGames )
    @DFGM.calcAvg( y.DFGM, numGames )
    @DFGA.calcAvg( y.DFGA, numGames )
    @DFG_PCT.calcAvg( y.DFG_PCT, numGames )
  end

  def to_h
    h = {}
    h = h.merge @DIST.to_h
    h = h.merge @ORBC.to_h
    h = h.merge @DRBC.to_h
    h = h.merge @RBC.to_h
    h = h.merge @TCHS.to_h
    h = h.merge @SAST.to_h
    h = h.merge @FTAST.to_h
    h = h.merge @PASS.to_h
    h = h.merge @AST.to_h
    h = h.merge @CFGM.to_h
    h = h.merge @CFGA.to_h
    h = h.merge @CFG_PCT.to_h
    h = h.merge @UFGM.to_h
    h = h.merge @UFGA.to_h
    h = h.merge @UFG_PCT.to_h
    h = h.merge @DFGM.to_h
    h = h.merge @DFGA.to_h
    h = h.merge @DFG_PCT.to_h

    return h
  end

  def to_opponent_h
    h = {}
    h = h.merge @DIST.to_opponent_h
    h = h.merge @ORBC.to_opponent_h
    h = h.merge @DRBC.to_opponent_h
    h = h.merge @RBC.to_opponent_h
    h = h.merge @TCHS.to_opponent_h
    h = h.merge @SAST.to_opponent_h
    h = h.merge @FTAST.to_opponent_h
    h = h.merge @PASS.to_opponent_h
    h = h.merge @AST.to_opponent_h
    h = h.merge @CFGM.to_opponent_h
    h = h.merge @CFGA.to_opponent_h
    h = h.merge @CFG_PCT.to_opponent_h
    h = h.merge @UFGM.to_opponent_h
    h = h.merge @UFGA.to_opponent_h
    h = h.merge @UFG_PCT.to_opponent_h
    h = h.merge @DFGM.to_opponent_h
    h = h.merge @DFGA.to_opponent_h
    h = h.merge @DFG_PCT.to_opponent_h

    return h
  end
end

class FourFactorStats
  attr_accessor :FTA_RATE

  def initialize
    @FTA_RATE = StatSet.new( :FTA_RATE )
  end

  def +( y )
    @FTA_RATE + y.FTA_RATE 
  end

  def calcAvg( y, numGames )
    @FTA_RATE.calcAvg( y.FTA_RATE, numGames )
  end

  def to_h
    h = {}
    h = h.merge @FTA_RATE.to_h

    return h
  end

  def to_opponent_h
    h = {}
    h = h.merge @FTA_RATE.to_opponent_h

    return h
  end
end

class UsageStats
  attr_accessor :USG_PCT, :PCT_FGM, :PCT_FGA, :PCT_FG3M, :PCT_FG3A, :PCT_FTM, :PCT_FTA, :PCT_OREB, :PCT_DREB, :PCT_REB, :PCT_AST, :PCT_TOV, :PCT_STL, :PCT_BLK, :PCT_BLKA, :PCT_PF, :PCT_PFD, :PCT_PTS
  attr_accessor :team_FGM, :team_FGA, :team_FG3M, :team_FG3A, :team_FTM, :team_FTA, :team_OREB, :team_DREB, :team_REB, :team_AST, :team_TOV, :team_STL, :team_BLK, :team_PF, :team_offensive_PTS, :team_defensive_PTS
    #:team_BLKA, :team_PFD, 
  attr_accessor :offensive_possessions, :defensive_possessions, :usage_offensive_possessions
  attr_accessor :o_team_OREB, :o_team_DREB, :o_team_REB

  def initialize
    @USG_PCT = StatSet.new( :USG_PCT )
    @PCT_FGM = StatSet.new( :PCT_FGM )
    @PCT_FGA = StatSet.new( :PCT_FGA )
    @PCT_FG3M = StatSet.new( :PCT_FG3M )
    @PCT_FG3A = StatSet.new( :PCT_FG3A )
    @PCT_FTM = StatSet.new( :PCT_FTM )
    @PCT_FTA = StatSet.new( :PCT_FTA )
    @PCT_OREB = StatSet.new( :PCT_OREB )
    @PCT_DREB = StatSet.new( :PCT_DREB )
    @PCT_REB = StatSet.new( :PCT_REB )
    @PCT_AST = StatSet.new( :PCT_AST )
    @PCT_TOV = StatSet.new( :PCT_TOV )
    @PCT_STL = StatSet.new( :PCT_STL )
    @PCT_BLK = StatSet.new( :PCT_BLK )
    @PCT_BLKA = StatSet.new( :PCT_BLKA )
    @PCT_PF = StatSet.new( :PCT_PF )
    @PCT_PFD = StatSet.new( :PCT_PFD )
    @PCT_PTS = StatSet.new( :PCT_PTS )

    @offensive_possessions = StatSet.new( :offensive_possessions )
    @defensive_possessions = StatSet.new( :defensive_possessions )
    @usage_offensive_possessions = StatSet.new( :usage_offensive_possessions )
    @team_FGM = StatSet.new( :team_FGM )
    @team_FGA = StatSet.new( :team_FGA )
    @team_FG3M = StatSet.new( :team_FG3M )
    @team_FG3A = StatSet.new( :team_FG3A )
    @team_FTM = StatSet.new( :team_FTM )
    @team_FTA = StatSet.new( :team_FTA )
    @team_OREB = StatSet.new( :team_OREB )
    @team_DREB = StatSet.new( :team_DREB )
    @team_REB = StatSet.new( :team_REB )
    @o_team_REB = StatSet.new( :o_team_REB )
    @o_team_OREB = StatSet.new( :o_team_OREB )
    @o_team_DREB = StatSet.new( :o_team_DREB )
    @team_AST = StatSet.new( :team_AST )
    @team_TOV = StatSet.new( :team_TOV )
    @team_STL = StatSet.new( :team_STL )
    @team_BLK = StatSet.new( :team_BLK )
    #@team_BLKA = StatSet.new( :team_BLKA )
    @team_PF = StatSet.new( :team_PF )
    #@team_PFD = StatSet.new( :team_PFD )
    @team_offensive_PTS = StatSet.new( :team_offensive_PTS )
    @team_defensive_PTS = StatSet.new( :team_defensive_PTS )
  end

  def +( y )
    @USG_PCT + y.USG_PCT
    @PCT_FGM + y.PCT_FGM
    @PCT_FGA + y.PCT_FGA
    @PCT_FG3M + y.PCT_FG3M
    @PCT_FG3A + y.PCT_FG3A
    @PCT_FTM + y.PCT_FTM
    @PCT_FTA + y.PCT_FTA
    @PCT_OREB + y.PCT_OREB
    @PCT_DREB + y.PCT_DREB
    @PCT_REB + y.PCT_REB
    @PCT_AST + y.PCT_AST
    @PCT_TOV + y.PCT_TOV
    @PCT_STL + y.PCT_STL
    @PCT_BLK + y.PCT_BLK
    @PCT_BLKA + y.PCT_BLKA
    @PCT_PF + y.PCT_PF
    @PCT_PFD + y.PCT_PFD
    @PCT_PTS + y.PCT_PTS

    @offensive_possessions + y.offensive_possessions
    @defensive_possessions + y.defensive_possessions
    @usage_offensive_possessions + y.usage_offensive_possessions
    @team_FGM + y.team_FGM
    @team_FGA + y.team_FGA
    @team_FG3M + y.team_FG3M
    @team_FG3A + y.team_FG3A
    @team_FTM + y.team_FTM
    @team_FTA + y.team_FTA
    @team_OREB + y.team_OREB
    @team_DREB + y.team_DREB
    @team_REB + y.team_REB
    @o_team_OREB + y.o_team_OREB
    @o_team_DREB + y.o_team_DREB
    @o_team_REB + y.o_team_REB
    @team_AST + y.team_AST
    @team_TOV + y.team_TOV
    @team_STL + y.team_STL
    @team_BLK + y.team_BLK
    #@team_BLKA + y.team_BLKA
    @team_PF + y.team_PF
    #@team_PFD + y.team_PFD
    @team_offensive_PTS + y.team_offensive_PTS
    @team_defensive_PTS + y.team_defensive_PTS
  end

  def calcAvg( y, numGames )
    @USG_PCT.calcAvg( y.USG_PCT, numGames )
    @PCT_FGM.calcAvg( y.PCT_FGM, numGames )
    @PCT_FGA.calcAvg( y.PCT_FGA, numGames )
    @PCT_FG3M.calcAvg( y.PCT_FG3M, numGames )
    @PCT_FG3A.calcAvg( y.PCT_FG3A, numGames )
    @PCT_FTM.calcAvg( y.PCT_FTM, numGames )
    @PCT_FTA.calcAvg( y.PCT_FTA, numGames )
    @PCT_OREB.calcAvg( y.PCT_OREB, numGames )
    @PCT_DREB.calcAvg( y.PCT_DREB, numGames )
    @PCT_REB.calcAvg( y.PCT_REB, numGames )
    @PCT_AST.calcAvg( y.PCT_AST, numGames )
    @PCT_TOV.calcAvg( y.PCT_TOV, numGames )
    @PCT_STL.calcAvg( y.PCT_STL, numGames )
    @PCT_BLK.calcAvg( y.PCT_BLK, numGames )
    @PCT_BLKA.calcAvg( y.PCT_BLKA, numGames )
    @PCT_PF.calcAvg( y.PCT_PF, numGames )
    @PCT_PFD.calcAvg( y.PCT_PFD, numGames )
    @PCT_PTS.calcAvg( y.PCT_PTS, numGames )

    @offensive_possessions.calcAvg( y.offensive_possessions, numGames )
    @defensive_possessions.calcAvg( y.defensive_possessions, numGames )
    @usage_offensive_possessions.calcAvg( y.usage_offensive_possessions, numGames )
    @team_FGM.calcAvg( y.team_FGM, numGames )
    @team_FGA.calcAvg( y.team_FGA, numGames )
    @team_FG3M.calcAvg( y.team_FG3M, numGames )
    @team_FG3A.calcAvg( y.team_FG3A, numGames )
    @team_FTM.calcAvg( y.team_FTM, numGames )
    @team_FTA.calcAvg( y.team_FTA, numGames )
    @team_OREB.calcAvg( y.team_OREB, numGames )
    @team_DREB.calcAvg( y.team_DREB, numGames )
    @team_REB.calcAvg( y.team_REB, numGames )
    @o_team_OREB.calcAvg( y.o_team_OREB, numGames )
    @o_team_DREB.calcAvg( y.o_team_DREB, numGames )
    @o_team_REB.calcAvg( y.o_team_REB, numGames )
    @team_AST.calcAvg( y.team_AST, numGames )
    @team_TOV.calcAvg( y.team_TOV, numGames )
    @team_STL.calcAvg( y.team_STL, numGames )
    @team_BLK.calcAvg( y.team_BLK, numGames )
    #@team_BLKA.calcAvg( y.team_BLKA, numGames )
    @team_PF.calcAvg( y.team_PF, numGames )
    #@team_PFD.calcAvg( y.team_PFD, numGames )
    @team_offensive_PTS.calcAvg( y.team_offensive_PTS, numGames )
    @team_defensive_PTS.calcAvg( y.team_defensive_PTS, numGames )
  end

  def to_h
    h = {}
    h = h.merge @USG_PCT.to_h
    h = h.merge @PCT_FGM.to_h
    h = h.merge @PCT_FGA.to_h
    h = h.merge @PCT_FG3M.to_h
    h = h.merge @PCT_FG3A.to_h
    h = h.merge @PCT_FTM.to_h
    h = h.merge @PCT_FTA.to_h
    h = h.merge @PCT_OREB.to_h
    h = h.merge @PCT_DREB.to_h
    h = h.merge @PCT_REB.to_h
    h = h.merge @PCT_AST.to_h
    h = h.merge @PCT_TOV.to_h
    h = h.merge @PCT_STL.to_h
    h = h.merge @PCT_BLK.to_h
    h = h.merge @PCT_BLKA.to_h
    h = h.merge @PCT_PF.to_h
    h = h.merge @PCT_PFD.to_h
    h = h.merge @PCT_PTS.to_h

    h = h.merge @offensive_possessions.to_h
    h = h.merge @defensive_possessions.to_h
    h = h.merge @usage_offensive_possessions.to_h
    h = h.merge @team_FGM.to_h
    h = h.merge @team_FGA.to_h
    h = h.merge @team_FG3M.to_h
    h = h.merge @team_FG3A.to_h
    h = h.merge @team_FTM.to_h
    h = h.merge @team_FTA.to_h
    h = h.merge @team_OREB.to_h
    h = h.merge @team_DREB.to_h
    h = h.merge @team_REB.to_h
    h = h.merge @o_team_OREB.to_h
    h = h.merge @o_team_DREB.to_h
    h = h.merge @o_team_REB.to_h
    h = h.merge @team_AST.to_h
    h = h.merge @team_TOV.to_h
    h = h.merge @team_STL.to_h
    h = h.merge @team_BLK.to_h
    #h = h.merge @team_BLKA.to_h
    h = h.merge @team_PF.to_h
    #h = h.merge @team_PFD.to_h
    h = h.merge @team_offensive_PTS.to_h
    h = h.merge @team_defensive_PTS.to_h
    return h
  end

  def to_opponent_h
    h = {}
    h = h.merge @USG_PCT.to_opponent_h
    h = h.merge @PCT_FGM.to_opponent_h
    h = h.merge @PCT_FGA.to_opponent_h
    h = h.merge @PCT_FG3M.to_opponent_h
    h = h.merge @PCT_FG3A.to_opponent_h
    h = h.merge @PCT_FTM.to_opponent_h
    h = h.merge @PCT_FTA.to_opponent_h
    h = h.merge @PCT_OREB.to_opponent_h
    h = h.merge @PCT_DREB.to_opponent_h
    h = h.merge @PCT_REB.to_opponent_h
    h = h.merge @PCT_AST.to_opponent_h
    h = h.merge @PCT_TOV.to_opponent_h
    h = h.merge @PCT_STL.to_opponent_h
    h = h.merge @PCT_BLK.to_opponent_h
    h = h.merge @PCT_BLKA.to_opponent_h
    h = h.merge @PCT_PF.to_opponent_h
    h = h.merge @PCT_PFD.to_opponent_h
    h = h.merge @PCT_PTS.to_opponent_h

    h = h.merge @offensive_possessions.to_opponent_h
    h = h.merge @defensive_possessions.to_opponent_h
    h = h.merge @team_FGM.to_opponent_h
    h = h.merge @team_FGA.to_opponent_h
    h = h.merge @team_FG3M.to_opponent_h
    h = h.merge @team_FG3A.to_opponent_h
    h = h.merge @team_FTM.to_opponent_h
    h = h.merge @team_FTA.to_opponent_h
    h = h.merge @team_OREB.to_opponent_h
    h = h.merge @team_DREB.to_opponent_h
    h = h.merge @team_REB.to_opponent_h
    h = h.merge @team_AST.to_opponent_h
    h = h.merge @team_TOV.to_opponent_h
    h = h.merge @team_STL.to_opponent_h
    h = h.merge @team_BLK.to_opponent_h
    #h = h.merge @team_BLKA.to_opponent_h
    h = h.merge @team_PF.to_opponent_h
    #h = h.merge @team_PFD.to_opponent_h
    h = h.merge @team_offensive_PTS.to_opponent_h
    h = h.merge @team_defensive_PTS.to_opponent_h
    return h
  end
end

class StatSplits
  attr_accessor :valid, :derivedStats, :perMinStats, :timeStats, :gamelogStats, :advancedStats, :miscStats, :scoringStats, :scoringDerivedStats, :trackingStats, :fourfactorStats, :usageStats

  def initialize
    @valid = 0
    @derivedStats = DerivedStats.new
    @perMinStats = PerMinStats.new
    @timeStats = TimeStats.new
    @gamelogStats = GamelogStats.new
    @advancedStats = AdvancedStats.new
    @miscStats = MiscStats.new
    @scoringStats = ScoringStats.new
    @scoringDerivedStats = ScoringDerivedStats.new
    @trackingStats = TrackingStats.new
    @fourfactorStats = FourFactorStats.new
    @usageStats = UsageStats.new
  end

  def +( y )
    #@valid = @valid + y.valid 
    @timeStats + y.timeStats 
    @gamelogStats + y.gamelogStats 
    @advancedStats + y.advancedStats 
    @miscStats + y.miscStats 
    @scoringStats + y.scoringStats 
    @trackingStats + y.trackingStats 
    @fourfactorStats + y.fourfactorStats 
    @usageStats + y.usageStats 
    @derivedStats + y.derivedStats 
    @perMinStats + y.perMinStats 
    @scoringDerivedStats + y.scoringDerivedStats 
  end

  def calcAvg( y, numGames )
    @timeStats.calcAvg( y.timeStats, numGames )
    @gamelogStats.game_id = y.gamelogStats.game_id
    @gamelogStats.calcAvg( y.gamelogStats, numGames )
    @advancedStats.calcAvg( y.advancedStats, numGames )
    @miscStats.calcAvg( y.miscStats, numGames )
    @trackingStats.calcAvg( y.trackingStats, numGames )
    @fourfactorStats.calcAvg( y.fourfactorStats, numGames )
    @usageStats.calcAvg( y.usageStats, numGames )
    @scoringStats.calcAvg( y.scoringStats, numGames )
    @derivedStats.calcAvg( y.derivedStats, numGames )
    @perMinStats.calcAvg( y.perMinStats, numGames )
    @scoringDerivedStats.calcAvg( y.scoringDerivedStats, numGames )
  end

  def to_h
    h = {}
    #h[:valid]
    h = h.merge @derivedStats.to_h
    h = h.merge @perMinStats.to_h
    h = h.merge @timeStats.to_h
    h = h.merge @gamelogStats.to_h
    h = h.merge @advancedStats.to_h
    h = h.merge @miscStats.to_h
    h = h.merge @scoringStats.to_h
    h = h.merge @scoringDerivedStats.to_h
    h = h.merge @trackingStats.to_h
    h = h.merge @fourfactorStats.to_h
    h = h.merge @usageStats.to_h

    return h
  end

  def to_opponent_h
    h = {}
    #h = h + @total_games.to_opponent_h
    #h = h + @valid.to_opponent_h
    h = h.merge @derivedStats.to_opponent_h
    h = h.merge @perMinStats.to_opponent_h
    h = h.merge @timeStats.to_opponent_h
    h = h.merge @gamelogStats.to_opponent_h
    h = h.merge @advancedStats.to_opponent_h
    h = h.merge @miscStats.to_opponent_h
    h = h.merge @scoringStats.to_opponent_h
    h = h.merge @scoringDerivedStats.to_opponent_h
    h = h.merge @trackingStats.to_opponent_h
    h = h.merge @fourfactorStats.to_opponent_h
    h = h.merge @usageStats.to_opponent_h

    return h
  end
end
=begin
class PrevGamesSet
  attr_accessor :prev_game_split, :prev_2games_split, :prev_5games_split

  def initialize
    @prev_game_split = StatSplits.new
    @prev_2games_split = StatSplits.new
    @prev_5games_split = StatSplits.new
  end

  def +( y )
    @prev_game_split = @prev_game_split + y.prev_game_split 
    @prev_2games_split = @prev_2games_split + y.prev_2games_split 
    @prev_5games_split = @prev_5games_split + y.prev_5games_split 
  end

  def to_h
    h = {}
    h = h + @prev_game_split.to_h
    h = h + @prev_2games_split.to_h
    h = h + @prev_5games_split.to_h

    return h
  end

  def to_opponent_h
    h = {}
    h = h + @prev_game_split.to_opponent_h
    h = h + @prev_2games_split.to_opponent_h
    h = h + @prev_5games_split.to_opponent_h

    return h
  end
end
=end

class SplitSet
  attr_accessor :split, :away_split, :home_split, :starter_split, :bench_split, :total_games_with_rest_split, :three_in_four_split, :four_in_six_split

  def initialize
    @split = StatSplits.new
    @away_split = StatSplits.new
    @home_split = StatSplits.new
    @starter_split = StatSplits.new
    @bench_split = StatSplits.new
    @total_games_with_rest_split = [ StatSplits.new, StatSplits.new, StatSplits.new, StatSplits.new, StatSplits.new, StatSplits.new, StatSplits.new ]
    @three_in_four_split = StatSplits.new
    @four_in_six_split = StatSplits.new
  end

  def +( y )
    @split + y.split 
    @away_split + y.away_split 
    @home_split + y.home_split 
    @starter_split + y.starter_split 
    @bench_split + y.bench_split 

    for i in 0...@total_games_with_rest_split.size
      @total_games_with_rest_split[ i ] + y.total_games_with_rest_split[ i ] 
    end

    @three_in_four_split + y.three_in_four_split 
    @four_in_six_split + y.four_in_six_split 
  end

  def calcAvg( y, numGames )
    @split.calcAvg( y.split, numGames )
    @away_split.calcAvg( y.away_split, numGames )
    @home_split.calcAvg( y.home_split, numGames )
    @starter_split.calcAvg( y.starter_split, numGames )
    @bench_split.calcAvg( y.bench_split, numGames )

    for i in 0...@total_games_with_rest_split.size
      @total_games_with_rest_split[ i ].calcAvg( y.total_games_with_rest_split[ i ], numGames )
    end

    @three_in_four_split.calcAvg( y.three_in_four_split, numGames )
    @four_in_six_split.calcAvg( y.four_in_six_split, numGames )
  end

  def to_h
    h = {}
    h = h.merge @split.to_h
    h = h.merge @away_split.to_h
    h = h.merge @home_split.to_h
    h = h.merge @starter_split.to_h
    h = h.merge @bench_split.to_h
    h = h.merge @total_games_with_rest_split[0].to_h
    h = h.merge @total_games_with_rest_split[1].to_h
    h = h.merge @total_games_with_rest_split[2].to_h
    h = h.merge @total_games_with_rest_split[3].to_h
    h = h.merge @total_games_with_rest_split[4].to_h
    h = h.merge @total_games_with_rest_split[5].to_h
    h = h.merge @total_games_with_rest_split[6].to_h
    h = h.merge @three_in_four_split.to_h
    h = h.merge @four_in_six_split.to_h

    return h
  end

  def to_opponent_h
    h = {}
    h = h.merge @split.to_opponent_h
    h = h.merge @away_split.to_opponent_h
    h = h.merge @home_split.to_opponent_h
    h = h.merge @starter_split.to_opponent_h
    h = h.merge @bench_split.to_opponent_h
    h = h.merge @total_games_with_rest_split[0].to_opponent_h
    h = h.merge @total_games_with_rest_split[1].to_opponent_h
    h = h.merge @total_games_with_rest_split[2].to_opponent_h
    h = h.merge @total_games_with_rest_split[3].to_opponent_h
    h = h.merge @total_games_with_rest_split[4].to_opponent_h
    h = h.merge @total_games_with_rest_split[5].to_opponent_h
    h = h.merge @total_games_with_rest_split[6].to_opponent_h
    h = h.merge @three_in_four_split.to_opponent_h
    h = h.merge @four_in_six_split.to_opponent_h

    return h
  end
end

=begin
games_played = 0
total_games_started = 0
total_home_games = 0
total_away_games = 0
total_wins = 0
losses = 0
ties = 0
total_time_played = Duration.new
h_seconds_played = Hash.new
=end
