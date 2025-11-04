from __future__ import print_function
import sqlite3
import numpy as np
import pandas as pd
import re
import sys
import pdb
import datetime
import time
import getopt
from datetime import timedelta
from ortools.linear_solver import pywraplp
from statistics import mean, median
from datetime import timedelta
from pytz import timezone
import csv

def main():
  under = 0
  twoSixty = 0
  twoSeventy = 0
  twoSeventyFive = 0
  twoEighty = 0
  twoNinety = 0
  threeHundred = 0
  threeHundredFive = 0
  threeTen = 0
  threeTwenty = 0
  threeThirty = 0
  infeas = 0

  seasons ={ 
          "2014-15" :[ "2014-10-30", "2015-4-16", "2015-6-17" ],
          "2015-16" :[ "2015-10-27", "2016-4-14", "2016-6-20" ],
          "2016-17" :[ "2016-10-25", "2017-4-13", "2017-6-13" ],
          "2017-18" :[ "2017-10-17", "2018-4-12", "2018-6-18" ],
          "2018-19" :[ "2018-10-16", "2019-4-11", "2019-6-19" ],
          "2019-20" :[ "2019-10-22", "2020-3-11", "2020-10-12" ],
          "2020-21" :[ "2020-12-22", "2021-05-17", "2021-10-12" ]}

  #seasons = {"2019-20" :[ "2019-10-22", "2020-3-11", "2020-10-12" ]}

  if len(sys.argv) < 2:
    print("need to provide db")
    exit()

  if 4 == len(sys.argv):
      seasons[ "2020-21" ][ 0 ] = sys.argv[2]
      seasons[ "2020-21" ][ 1 ] = sys.argv[3]

  from_date = None
  to_date = None
  dbFile = None
  days = None
  season = None
  predict = None
  actual = None

  argv = sys.argv[1:] 
  try: 
    opts, args = getopt.getopt(argv, "f:t:d:y:s:p:a:",
                                 ["from=", 
                                  "to="]) 
    
  except Exception as e:
      pdb.set_trace()
      print("Error") 

  for opt, arg in opts: 
    if opt in ['-f', '--from']: 
      from_date = arg 
    elif opt in ['-t', '--to']: 
      to_date = arg 
    elif opt in ['-d', '--database']: 
      dbFile = arg 
    elif opt in ['-y', '--days']: 
      days = arg 
    elif opt in ['-p', '--predict']: 
      predict = arg 
    elif opt in ['-a', '--actual']: 
      actual = arg 
    elif opt in ['-s', '--season']: 
      season = arg 
      seasons = {season:seasons[season]}

  if None == season or None == dbFile:
    print('No season or db...exiting')
    exit()

  if days:
    tzNY = timezone("America/New_York")
    now = datetime.datetime.now()
    #dt = tzNY.localize(now)
    dt = now.astimezone(tzNY)
    if predict:
      startDate = dt.date()
      endDate = startDate + timedelta(days=1)
    else:
      endDate = dt.date()
      startDate = endDate - timedelta(days=1)

    seasons[ season ][ 0 ] = startDate.strftime('%Y-%m-%d')
    seasons[ season ][ 1 ] = endDate.strftime('%Y-%m-%d')

  print('from: {} to: {} dbFile: {} days: {} season: {}'.format(seasons[season][0],seasons[season][1],dbFile,days,season))

  #conn = sqlite3.connect( sys.argv[1] )
  conn = sqlite3.connect( dbFile )
  #conn = sqlite3.connect("allXY.db")
  #conn.row_factory = dict_factory#sqlite3.Row
  #conn.row_factory = sqlite3.Row
  cursor = conn.cursor()

  positions = [
    ['C', 1],
    ['PF', 2],
    ['SF', 2],
    ['SG', 2],
    ['PG', 2],
    ]
  salaryC = ['Salary',60000]
  removed = ['Removed',0]
  numLineupsPerDay = 10

  for season,seasonDates in seasons.items():
      date = datetime.datetime.strptime( seasonDates[0], "%Y-%m-%d" )
      endDate = datetime.datetime.strptime( seasonDates[1], "%Y-%m-%d" )
    
      under = 0
      twoSixty = 0
      twoSeventy = 0
      twoSeventyFive = 0
      twoEighty = 0
      twoNinety = 0
      threeHundred = 0
      threeHundredFive = 0
      threeTen = 0
      threeTwenty = 0
      threeThirty = 0
      threeFourty = 0
      infeas = 0

      projectedScores = []
      actualScores = []
      meanProjectedScores = []
      meanActualScores = []
      medianProjectedScores = []
      medianActualScores = []

      projectedScores1 = []
      actualScores1 = []
      projectedScores2 = []
      actualScores2 = []
      projectedScores3 = []
      actualScores3 = []
      projectedScores4 = []
      actualScores4 = []
      projectedScores5 = []
      actualScores5 = []
      projectedScores6 = []
      actualScores6 = []
      projectedScores7 = []
      actualScores7 = []
      projectedScores8 = []
      actualScores8 = []
      projectedScores9 = []
      actualScores9 = []
      projectedScores10 = []
      actualScores10 = []
      while date < endDate:
        selectedPlayers = []
        dayProjectedScores = []
        dayActualScores = []

        playerFDDict = {}

        '''
        filename = "../fd_" + date.strftime("%m%d") + "_mod.csv"
        #if (path.exists(filename)):
        with open(filename, mode='r') as csv_file:
          playerFDDict = {row['player_id']: {'player_name': row['player_name'],'Must': row['Must'],'Remove': row['Remove'],'output': (row['Id'] + ':' + row['First Name'] + ' ' + row['Last Name'])}
              for row in csv.DictReader(csv_file, skipinitialspace=True)}
        '''

        dateStr = date.strftime("%Y-%m-%d")
        print(dateStr)
        cursor.execute("select player_name,player_id,date,projected_fdscore,actual_fdscore,projected_seconds,actual_seconds,pos,fd_salary,projected_pts,projected_oreb,projected_dreb,projected_ast,projected_stl,projected_blk,projected_tov,projected_starter from results_{} where date = '{}'".format(re.sub('-','_',season),dateStr))
        playerEntries = cursor.fetchall()

        playerEntries = [list(elem) for elem in playerEntries]

        for x in playerEntries:
          if isinstance(x[8],str):
            x[8] = 70000.0
        playerEntries = sorted(playerEntries, key = lambda x: x[3]/x[8])
        playerEntries.reverse()

        '''
        0 player_name
        1 player_id
        2 date
        3 projected_fdscore
        4 actual_fdscore
        5 projected_seconds
        6 actual_seconds
        7 pos
        8 fd_salary
        9 projected_pts
        10 projected_oreb
        11 projected_dreb
        12 projected_ast
        13 projected_stl
        14 projected_blk
        15 projected_tov
        16 projected_starter
        '''
        print("Order by Projected X: ")
        idx = 0
        for p in playerEntries:
          if 0 == p[8]:
            p[8] = 1
          print(f"{idx:>3}: {p[0]:>25} {p[7]:>2} pX/aX: {1000*p[3]/p[8]:>4.2f} / {1000*p[4]/p[8]:>4.2f} pFP/aFP: {p[3]:>4.2f} / {p[4]:>4.2f} pMins/aMins: {p[5]/60:>5.2f} / {p[6]/60:>5.2f} pLine: {p[9]:.1f}pts {p[10]+p[11]:.1f}reb {p[12]:.1f}ast {p[13]:.1f}stl {p[14]:.1f}blk {p[15]:.1f}tov salary: {'$' + str(int(p[8])):>6}")
          idx = idx + 1

        if None == predict:
          cursor.execute("select player_name,player_id,date,projected_fdscore,actual_fdscore,projected_seconds,actual_seconds,pos,fd_salary,projected_pts,actual_pts,projected_oreb,actual_oreb,projected_dreb,actual_dreb,projected_ast,actual_ast,projected_stl,actual_stl,projected_blk,actual_blk,projected_tov,actual_tov,projected_starter from results_{} where date = '{}'".format(re.sub('-','_',season),dateStr))
          playerEntries2 = cursor.fetchall()
          playerEntries2 = [list(elem) for elem in playerEntries2]
          for x in playerEntries2:
            if isinstance(x[8],str):
              x[8] = 70000.0
          playerEntries2 = sorted(playerEntries2, key = lambda x: x[4]/x[8])
          playerEntries2.reverse()

          print("Order by Actual X: ")
          idx = 0
          for p in playerEntries2:
            if 0 == p[8]:
              p[8] = 1
            print(f"{idx:>3}: {p[0]:>25} {p[7]:>2} pX/aX: {1000*p[3]/p[8]:>4.2f}/{1000*p[4]/p[8]:>4.2f} pFP/aFP: {p[3]:>4.2f}/{p[4]:>4.2f} pMins/aMins: {p[5]/60:>5.2f}/{p[6]/60:>5.2f} pLine/aLine: {p[9]:>4.1f}/{p[10]:>2} pts {p[11]+p[13]:>4.1f}/{p[12]+p[14]:2} reb {p[15]:>4.1f}/{p[16]:2} ast {p[17]:>4.1f}/{p[18]:2} stl {p[19]:>3.1f}/{p[20]:2} blk {p[21]:>3.1f}/{p[22]:2} tov salary: {'$' + str(int(p[8])):>6}")
            idx = idx + 1


        #pdb.set_trace()
        data = []
        rowsToDelete = []
        for j in range(0,len(playerEntries)):
          #data[j] = []
          data.append( [] )
          for i in range(0,7):
              #data[j][i] = playerEntries[j][i]
              data[j].append( playerEntries[j][i] )

          data[j][3] = float(data[j][3]) 
          data[j][4] = float(data[j][4])
          data[j][5] = float(data[j][5])
          data[j][6] = float(data[j][6])

          data[j].append( 1 if("C" == playerEntries[j][7]) else 0 ) #7
          data[j].append( 1 if("PF" == playerEntries[j][7]) else 0 ) #8
          data[j].append( 1 if("SF" == playerEntries[j][7]) else 0 ) #9
          data[j].append( 1 if("SG" == playerEntries[j][7]) else 0 ) #10
          data[j].append( 1 if("PG" == playerEntries[j][7]) else 0 ) #11
          try:
              data[j].append( float(playerEntries[j][8]) ) #12
          except:
              #TODO delete these entries from the list
              #rowsToDelete.append( j )
              data[j].append( 70000 ) #"delete" it for now by giving exorbitant salary

          if days:
            if data[j][1] in playerFDDict.keys():
              if '1' == playerFDDict[ data[j][1] ]['Remove']:
                print(f"Remove {data[j][0]}")
                data[j].append( 1 ) #Append 0 to REMOVED column #13
              else:
                data[j].append( 0 ) #Append 0 to REMOVED column #13

              if '1' == playerFDDict[ data[j][1] ]['Must']:
                print(f"Must {data[j][0]}")
                data[j].append( 1 ) #Append 0 to MUST column #14
              else:
                data[j].append( 0 ) #Append 0 to MUST column #14
            else:
              data[j].append( 1 ) #Append 1 to REMOVED column #13
              data[j].append( 0 ) #Append 0 to MUST column #14
              print(f"No FanDuel info for {data[j][0]} {data[j][1]}, REMOVING")
          else:
              data[j].append( 0 ) #Append 1 to REMOVED column #13
              data[j].append( 0 ) #Append 0 to MUST column #14

        for index in sorted(rowsToDelete, reverse=True):
            del data[ index ]
        '''
        for d in data:
            if 70000 == d[12]:
                #pdb.set_trace()
                print("didn't work")
        '''

        #pdb.set_trace()
        for lineupIdx in range(numLineupsPerDay):
            solver = pywraplp.Solver.CreateSolver('SCIP')
            # Declare an array to hold our nutritional data.
            players = [[]] * len(data)

            # Objective: minimize the sum of (price-normalized) foods.
            objective = solver.Objective()

            for i in range(0, len(data)):
              players[i] = solver.IntVar(0.0, 1.0, data[i][0])
              if actual:
                objective.SetCoefficient(players[i], data[i][4])#actual score
              else:
                objective.SetCoefficient(players[i], data[i][3])#projected score
            objective.SetMaximization()

            try:
              constraints = [0] * (len(positions) + 3)
              constraints[0] = solver.Constraint(positions[0][1], positions[0][1]) #Positions Constraint
              constraints[1] = solver.Constraint(positions[1][1], positions[1][1]) #Positions Constraint
              constraints[2] = solver.Constraint(positions[2][1], positions[2][1]) #Positions Constraint
              constraints[3] = solver.Constraint(positions[3][1], positions[3][1]) #Positions Constraint
              constraints[4] = solver.Constraint(positions[4][1], positions[4][1]) #Positions Constraint
              constraints[5] = solver.Constraint(0, 60000) #Salary Constraint
              constraints[6] = solver.Constraint(0, 0) #"REMOVED" Constraint
              constraints[7] = solver.Constraint(0, 0) #"MUST" Constraint
              
              for i in range(0,len(positions)):
                for j in range(0, len(data)):
                  constraints[i].SetCoefficient(players[j], data[j][i+7]) #Position Coefficient
             
            except Exception as e:
              pdb.set_trace()
              print(e)

            for j in range(0, len(data)):
              constraints[5].SetCoefficient(players[j], data[j][12]) #Salary Coefficient
              try:
                constraints[6].SetCoefficient(players[j], data[j][13]) #REMOVED Coefficient
                constraints[7].SetCoefficient(players[j], data[j][14]) #MUST Coefficient
              except Exception as e:
                pdb.set_trace()

            # Solve!
            status = solver.Solve()

            if status == solver.OPTIMAL:
              # Display the amounts (in dollars) to purchase of each food.
              totalPlayers = 0
              num_positions = 5 #len(data[i]) - 6

              totalProjectedPoints = 0
              totalActualPoints = 0
              totalSalary = 0
              totalPG = 0
              totalSG = 0
              totalSF = 0
              totalPF = 0
              totalC = 0
              totalNA = 0
              print("lineup {} / {} ({} total players)".format(lineupIdx + 1,numLineupsPerDay,len(data)))

              #pdb.set_trace()
              outStrings = {'PG': [],'SG': [],'SF': [],'PF': [],'C': []}
              for i in range(0, len(data)):
                totalPlayers += players[i].solution_value()
                totalProjectedPoints += players[i].solution_value() * data[i][3]
                totalActualPoints += players[i].solution_value() * data[i][4]

                if players[i].solution_value() > 0:
                    totalSalary = totalSalary + data[i][12]
                    if "C" == playerEntries[i][7]:
                      totalC += 1
                    elif "PF" == playerEntries[i][7]:
                      totalPF += 1
                    elif "SF" == playerEntries[i][7]:
                      totalSF += 1
                    elif "SG" == playerEntries[i][7]:
                      totalSG += 1
                    elif "PG" == playerEntries[i][7]:
                      totalPG += 1
                    elif "NA" == playerEntries[i][7]:
                      totalNA += 1

                    name = data[i][0]
                    projectedFP = data[i][3] 
                    actualFP = data[i][4] 
                    projectedMins = data[i][5]/60
                    actualMins = data[i][6]/60
                    projectedPPM = projectedFP * 60 / projectedMins
                    actualPPM = actualFP * 60 / actualMins
                    gainPPM = ((actualPPM / projectedPPM) - 1) * 100
                    gainMinutes = ((actualMins / projectedMins) - 1) * 100
                    '''
                    0 player_name
                    1 player_id
                    2 date
                    3 projected_fdscore
                    4 actual_fdscore
                    5 projected_seconds
                    6 actual_seconds
                    7 pos
                    8 fd_salary
                    9 projected_pts
                    10 projected_oreb
                    11 projected_dreb
                    12 projected_ast
                    13 projected_stl
                    14 projected_blk
                    15 projected_tov
                    16 projected_starter
                    '''
                    starter = 'bench'
                    if 1 == playerEntries[i][16]:
                      starter = 'starter'
                    #players[i].solution_value()
                    tablestr = f"{i:>3}: {starter:>7} {playerEntries[i][7]:>2} {name:>20.20} {('$'+str(int(data[i][12]))):>6} projectedFP: {projectedFP:>.2f} actualFP: {actualFP:>.2f} projectedMins: {projectedMins:>.2f} actualMins: {actualMins:>.2f} projectedPPM: {projectedPPM:>6.2f} actualPPM: {actualPPM:>6.2f} gainPPM: {gainPPM:>6.2f}% gainMins: {gainMinutes:>6.2f}%"
                    
                    if days:
                      outStrings[ playerEntries[i][7] ].append( {'tablestr': tablestr, 'output': playerFDDict[ data[i][1] ]['output']} )
                    else:
                      print(f"{i:>3}: {starter:>7} {playerEntries[i][7]:>2} {name:>20.20} {('$'+str(int(data[i][12]))):>6} projectedFP: {projectedFP:>.2f} actualFP: {actualFP:>.2f} projectedMins: {projectedMins:>.2f} actualMins: {actualMins:>.2f} projectedPPM: {projectedPPM:>6.2f} actualPPM: {actualPPM:>6.2f} gainPPM: {gainPPM:>6.2f}% gainMins: {gainMinutes:>6.2f}%")

                    if 0 == lineupIdx:
                      if days:
                        if '1' != playerFDDict[ data[i][1] ]['Must']:
                          selectedPlayers.append( [i,projectedFP] )
                      else:
                        selectedPlayers.append( [i,projectedFP] )


              outputStr = ''
              for pos in ['PG','SG','SF','PF','C']:
                for d in outStrings[ pos ]:
                  print( d['tablestr'] )
                  outputStr = outputStr + d['output']

              #outputStr = PGs[0]['output'] + ',' + PGs[1]['output'] + ',' + SGs[0]['output'] + ',' + SGs[1]['output'] + ',' + SFs[0]['output'] + ',' + SFs[1]['output'] + ',' + PFs[0]['output'] + ',' + PFs[1]['output'] + ',' + Cs[0]['output']
              print( f"FD outputStr: {outputStr}" )
              print("")
              #Zero out REMOVED column in data
              for i in range(len(data)):
                if days:
                  if data[i][1] in playerFDDict.keys():
                    if '1' != playerFDDict[ data[i][1] ]['Remove']:
                      data[i][13] = 0
                else:
                  data[i][13] = 0


              if totalC != 1 or totalPF != 2 or totalSF != 2 or totalSG != 2 or totalPG != 2:
                pdb.set_trace()
                print("POSITIONS ERROR!")
              if totalNA != 0:
                print("NA FIX!!!!!!!!")

              if 9 != totalPlayers:
                pdb.set_trace()
                print("Solution has wrong number of players.")
              
              print(f"Salary: ${int(totalSalary)} Projected: {totalProjectedPoints:.2f} ({1000*totalProjectedPoints/totalSalary:.2f}) Actual: {totalActualPoints:.2f} ({1000*totalActualPoints/totalSalary:.2f})")
              #print('Total Players: {:.2f}'.format(totalPlayers))
              #print('Total Salary: {:.2f}'.format(totalSalary))
              #print('Total Projected: {:.2f}'.format(totalProjectedPoints))
              #print('Total Actual: {:.2f}'.format(totalActualPoints))

              if lineupIdx < (numLineupsPerDay-1):
                try:
                  if( len( selectedPlayers ) > 0 ):
                    playerToRemove = selectedPlayers.pop()
                    print("Removing {} from lineup\n".format( data[ playerToRemove[0] ][0] ))
                    data[ playerToRemove[0] ][ 13 ] = 1
                  else:
                    print("no players to remove")
                except Exception as e:
                  pdb.set_trace()
                  print("ERROR")

              dayProjectedScores.append( totalProjectedPoints )
              dayActualScores.append( totalActualPoints )

              projectedScores.append( totalProjectedPoints )
              actualScores.append( totalActualPoints )
              if 0 == lineupIdx:
                  projectedScores1.append( totalProjectedPoints )
                  actualScores1.append( totalActualPoints )
              if 1 == lineupIdx:
                  projectedScores2.append( totalProjectedPoints )
                  actualScores2.append( totalActualPoints )
              if 2 == lineupIdx:
                  projectedScores3.append( totalProjectedPoints )
                  actualScores3.append( totalActualPoints )
              if 3 == lineupIdx:
                  projectedScores4.append( totalProjectedPoints )
                  actualScores4.append( totalActualPoints )
              if 4 == lineupIdx:
                  projectedScores5.append( totalProjectedPoints )
                  actualScores5.append( totalActualPoints )
              if 5 == lineupIdx:
                  projectedScores6.append( totalProjectedPoints )
                  actualScores6.append( totalActualPoints )
              if 6 == lineupIdx:
                  projectedScores7.append( totalProjectedPoints )
                  actualScores7.append( totalActualPoints )
              if 7 == lineupIdx:
                  projectedScores8.append( totalProjectedPoints )
                  actualScores8.append( totalActualPoints )
              if 8 == lineupIdx:
                  projectedScores9.append( totalProjectedPoints )
                  actualScores9.append( totalActualPoints )
              if 9 == lineupIdx:
                  projectedScores10.append( totalProjectedPoints )
                  actualScores10.append( totalActualPoints )
            else:  # No optimal solution was found.
              if status == solver.FEASIBLE:
                print('A potentially suboptimal solution was found.')
              else:
                infeas = infeas + 1
                print('The solver could not solve the problem.')


        if dayActualScores:
          medianDayActualScore = median( dayActualScores )
          medianDayProjectedScore = median( dayProjectedScores )
          meanDayActualScore = mean( dayActualScores )
          meanDayProjectedScore = mean( dayProjectedScores )

          meanProjectedScores.append( mean( dayProjectedScores ) )
          meanActualScores.append( mean( dayActualScores ) )
          medianProjectedScores.append( median( dayProjectedScores ) )
          medianActualScores.append( median( dayActualScores ) )
          print("median Actual Score for {}: {}".format(dateStr,medianDayActualScore))
          print("median Projected Score for {}: {}".format(dateStr,medianDayProjectedScore))
          print("mean Actual Score for {}: {}".format(dateStr,meanDayActualScore))
          print("mean Projected Score for {}: {}".format(dateStr,meanDayProjectedScore))

        #projectedScores.append( totalProjectedPoints )
        #actualScores.append( totalActualPoints )

          if medianDayActualScore <= 260:
              under = under + 1
          if medianDayActualScore > 260:
              twoSixty = twoSixty + 1
          if medianDayActualScore > 270:
              twoSeventy = twoSeventy + 1
          if medianDayActualScore > 275:
              twoSeventyFive = twoSeventyFive + 1
          if medianDayActualScore > 280:
              twoEighty = twoEighty + 1
          if medianDayActualScore > 290:
              twoNinety = twoNinety + 1
          if medianDayActualScore > 300:
              threeHundred = threeHundred + 1
          if medianDayActualScore > 305:
              threeHundredFive = threeHundredFive + 1
          if medianDayActualScore > 310:
              threeTen = threeTen + 1
          if medianDayActualScore > 320:
              threeTwenty = threeTwenty + 1
          if medianDayActualScore > 330:
              threeThirty = threeThirty + 1

        date = date + timedelta(days=1)

      print("total days: {}".format(len(projectedScores)))

      print("mean projected: {}".format( mean( projectedScores ) ))
      print("mean actual: {}".format( mean( actualScores ) ))
      print("median projected: {}".format( median( projectedScores ) ))
      print("median actual: {}".format( median( actualScores ) ))

      print("mean projected1: {}".format( mean( projectedScores1 ) ))
      print("mean actual1: {}".format( mean( actualScores1 ) ))
      print("median projected1: {}".format( median( projectedScores1 ) ))
      print("median actual1: {}".format( median( actualScores1 ) ))
      print("mean projected2: {}".format( mean( projectedScores2 ) ))
      print("mean actual2: {}".format( mean( actualScores2 ) ))
      print("median projected2: {}".format( median( projectedScores2 ) ))
      print("median actual2: {}".format( median( actualScores2 ) ))
      print("mean projected3: {}".format( mean( projectedScores3 ) ))
      print("mean actual3: {}".format( mean( actualScores3 ) ))
      print("median projected3: {}".format( median( projectedScores3 ) ))
      print("median actual3: {}".format( median( actualScores3 ) ))
      print("mean projected4: {}".format( mean( projectedScores4 ) ))
      print("mean actual4: {}".format( mean( actualScores4 ) ))
      print("median projected4: {}".format( median( projectedScores4 ) ))
      print("median actual4: {}".format( median( actualScores4 ) ))
      print("mean projected5: {}".format( mean( projectedScores5 ) ))
      print("mean actual5: {}".format( mean( actualScores5 ) ))
      print("median projected5: {}".format( median( projectedScores5 ) ))
      print("median actual5: {}".format( median( actualScores5 ) ))
      print("mean projected6: {}".format( mean( projectedScores6 ) ))
      print("mean actual6: {}".format( mean( actualScores6 ) ))
      print("median projected6: {}".format( median( projectedScores6 ) ))
      print("median actual6: {}".format( median( actualScores6 ) ))
      print("mean projected7: {}".format( mean( projectedScores7 ) ))
      print("mean actual7: {}".format( mean( actualScores7 ) ))
      print("median projected7: {}".format( median( projectedScores7 ) ))
      print("median actual7: {}".format( median( actualScores7 ) ))
      print("mean projected8: {}".format( mean( projectedScores8 ) ))
      print("mean actual8: {}".format( mean( actualScores8 ) ))
      print("median projected8: {}".format( median( projectedScores8 ) ))
      print("median actual8: {}".format( median( actualScores8 ) ))
      print("mean projected9: {}".format( mean( projectedScores9 ) ))
      print("mean actual9: {}".format( mean( actualScores9 ) ))
      print("median projected9: {}".format( median( projectedScores9 ) ))
      print("median actual9: {}".format( median( actualScores9 ) ))
      print("mean projected10: {}".format( mean( projectedScores10 ) ))
      print("mean actual10: {}".format( mean( actualScores10 ) ))
      print("median projected10: {}".format( median( projectedScores10 ) ))
      print("median actual10: {}".format( median( actualScores10 ) ))


      print("under TwoSixtys: {}".format(under))
      print("TwoSixtys: {}".format(twoSixty))
      print("TwoSeventys: {}".format(twoSeventy))
      print("TwoSeventyFives: {}".format(twoSeventyFive))
      print("TwoEightys: {}".format(twoEighty))
      print("TwoNinetys: {}".format(twoNinety))
      print("ThreeHundreds: {}".format(threeHundred))
      print("ThreeHundredFives: {}".format(threeHundredFive))
      print("ThreeTens: {}".format(threeTen))
      print("ThreeTwentys: {}".format(threeTwenty))
      print("ThreeThirtys: {}".format(threeThirty))
      print("infeas: {}".format(infeas))
      print("total days: {}".format(under+infeas+twoSixty))
      print("total denom days: {}".format(under+twoSixty))
      #time.sleep(5)

if __name__ == '__main__':
  main()
