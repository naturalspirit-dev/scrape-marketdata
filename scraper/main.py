import os
import sys
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from dataharvesting.harvestData import HarvestData
from dataprocessing.processor import Processor
import datetime 
from auth.auth import auth
import pytz
import time
from utils.timeToOpen import timeToOpen
import robin_stocks.robinhood as rs


def waitForOpen():
    est = datetime.datetime.now(pytz.timezone("US/Eastern")).time()
    if (est < datetime.time(16) and est > datetime.time(9)):
        return
    else:
        time.sleep(timeToOpen())

auth()
dataHarvester = HarvestData()
processor = Processor()
currEst = int(str(datetime.datetime.now(pytz.timezone('US/Eastern')).time()).split(':')[0])

while(True):
    currEst = int(str(datetime.datetime.now(pytz.timezone('US/Eastern')).time()).split(':')[0])
    if (datetime.datetime.now().weekday() == 5 and currEst > 16): #if friday wait 48 hours
        print("It's Friday - waiting till market open")
        time.sleep(172100)
    if (datetime.datetime.now().weekday() == 6): #if saturday wait 24 hours
        print("It's Saturday - waiting till market open")
        time.sleep(86100)
    waitForOpen()
    if (not rs.get_market_today_hours('XNYS')['is_open']):
        print("Market is currently closed - waiting 24 hours")
        time.sleep(86400)
    else:
        print("--- Running data harvester ", datetime.datetime.now(), "---")
        while (datetime.datetime.now(pytz.timezone("US/Eastern")).time() < datetime.time(16, 20)): ##until market closes (EST), run bot every 5 minute
            dataHarvester.harvestDailyDataToDB("spy")
            dataHarvester.harvestDailyDataToDB("aapl")
            dataHarvester.harvestDailyDataToDB("meta")
            dataHarvester.harvestDailyDataToDB("amc")
            time.sleep(300)
        print("--- Finished running data harvester ---")
