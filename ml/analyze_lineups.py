import sqlite3
import numpy as np
import pandas as pd
import re
import sys
import pdb
from pandas import read_csv
import datetime
from datetime import timedelta
from statistics import mean

'''
conn2 = sqlite3.connect( dbFile )
conn2.row_factory = dict_factory#sqlite3.Row
cursor2 = conn2.cursor()
'''

#date = sys.argv[1]

date = datetime.date(2021,1,1)
endDate = datetime.date(2021,1,26)
myMedian = 0
myMax = 0
totalDays = 0
myMedians = []
myMaxes = []
fcMedians = []
fcMaxes = []
while date < endDate:
    dateStr = date.strftime("%m%d")
    date = date + timedelta(days=1)
    if "0123" == dateStr:
        continue

    filename = 'fc/' + dateStr + '_FC_18_80.csv'
    filename2 = 'fc/' + dateStr + '_My_18_80.csv'

    dataset = read_csv(filename, delimiter=",")
    dfFC = dataset.fillna(0)

    dataset2 = read_csv(filename2, delimiter=",")
    dfMy = dataset2.fillna(0)
    sortedFC = dfFC.sort_values('Actual',ascending=False)
    sortedMy = dfMy.sort_values('Actual',ascending=False)
    medianFC = sortedFC.iloc[75]['Actual']
    medianMy = sortedMy.iloc[75]['Actual']
    maxFC = sortedFC.iloc[0]['Actual']
    maxMy = sortedMy.iloc[0]['Actual']
    myMedians.append(medianMy)
    myMaxes.append(maxMy)
    fcMedians.append(medianFC)
    fcMaxes.append(maxFC)

    if medianMy > medianFC:
        myMedian = myMedian + 1
    if maxMy > maxFC:
        myMax = myMax + 1
    print(f"{date.strftime('%m %d')}:")
    print(f"{'Median FC':>10}: {medianFC:.2f}")
    print(f"{'Median My':>10}: {medianMy:.2f}")
    print(f"{'Max FC':>10}: {maxFC:.2f}")
    print(f"{'Max My':>10}: {maxMy:.2f}")
    print("")

    totalDays = totalDays + 1
print(f"My median larger {myMedian} / {totalDays}")
print(f"My max larger {myMax} / {totalDays}")
print(f"{'mean mymedians':15}: {mean(myMedians)}")
print(f"{'mean fcMedians':15}: {mean(fcMedians)}")
print(f"{'mean myMaxes':15}: {mean(myMaxes)}")
print(f"{'mean fcMaxes':15}: {mean(fcMaxes)}")
#mean(myMaxes)
#mean(fcMedians)
#mean(fcMaxes)

pdb.set_trace()

