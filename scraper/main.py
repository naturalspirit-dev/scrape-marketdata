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
currEst = datetime.datetime.now(pytz.timezone('US/Eastern'))
currEstHour = int(str(currEst.time()).split(':')[0])

if (currEst.weekday() == 5 or currEst.weekday() == 6):
    print("It's the weekend - exiting...")
    exit()

waitForOpen()
if (not rs.get_market_today_hours('XNYS')['is_open']):
    print("Market is currently closed - exiting...")
    exit()
else:
    print("--- Running data harvester ", datetime.datetime.now(), "---")
    while (currEst.time() < datetime.time(16, 20)): ##until market closes (EST), run bot every 5 minute
        dataHarvester.harvestDailyDataToDB("spy")
        dataHarvester.harvestDailyDataToDB("aapl")
        dataHarvester.harvestDailyDataToDB("meta")
        dataHarvester.harvestDailyDataToDB("amc")
        dataHarvester.harvestDailyDataToDB("nvda")
        dataHarvester.harvestDailyDataToDB("tsla")
        dataHarvester.harvestDailyDataToDB("amzn")
        dataHarvester.harvestDailyDataToDB("goog")
        dataHarvester.harvestDailyDataToDB("mrna")
        dataHarvester.harvestDailyDataToDB("twtr")
        dataHarvester.harvestDailyDataToDB("msft")
        time.sleep(300)
    print("--- Finished running data harvester ---")
