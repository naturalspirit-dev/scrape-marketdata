import os
import sys
import pytz
import json
import time
import datetime 
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from dataharvesting.harvestData import HarvestData
from auth.auth import auth
from utils.timeToOpen import timeToOpen
import robin_stocks.robinhood as rs
from errors.scraperError import ScraperError
from logs.logger import logMsg

def waitForOpen():
    est = datetime.datetime.now(pytz.timezone("US/Eastern")).time()
    if (est < datetime.time(16) and est > datetime.time(9)):
        return
    else:
        time.sleep(timeToOpen())

def run():
    logMsg("", "initial")
    logMsg("Running data harvester ", "msg")
    logMsg(str(datetime.datetime.now().date()), "msg")
    if(auth()):
        logMsg("Login successful", "msg")
    else:
        err = ScraperError("auth error", "main.py", "login unsuccessful")
        logMsg(str(err), "err")
        logMsg("", "final")
        raise err

    dataHarvester = HarvestData()
    currEst = datetime.datetime.now(pytz.timezone('US/Eastern'))
    # currEstHour = int(str(currEst.time()).split(':')[0])   
    stocks = open("./stocksToScrape.json")
    stocks = json.load(stocks)

    if (currEst.weekday() == 5 or currEst.weekday() == 6):
        logMsg("It's the weekend - exiting...", "marketclosed")
        logMsg("", "final")
        return

    waitForOpen()

    if (not rs.get_market_today_hours('XNYS')['is_open']):
        logMsg("Market is currently closed - exiting...", "marketclosed")
        logMsg("", "final")
        return
    else:
        while (currEst.time() < datetime.time(16, 20, 0, 0, pytz.timezone('US/Eastern'))): ##until market closes (EST), run bot every 5 minute
            for ticker in stocks["stocks"]["currentlyScraping"]:
                dataHarvester.harvestDailyDataToDB(ticker)
                time.sleep(300)
        logMsg("Finished running data harvester succesfully", "msg")
        logMsg("", "final")
        return

try:
    run()
except:
    raise ScraperError("main.py run() method", "main.py")
