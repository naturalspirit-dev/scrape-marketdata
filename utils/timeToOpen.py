import datetime
from time import timezone
import pytz
from dateutil.relativedelta import relativedelta
from errors.scraperError import ScraperError
from logs.logger import logMsg

def timeToOpen():
  if (timeZoneToOpen(False) > 86400):
    return timeAmbigToOpen()
  elif (timeAmbigToOpen(False) > 86400):
    return timeZoneToOpen()
  else: return timeZoneToOpen()

def timeZoneToOpen(displaytime=True):
  est = pytz.timezone('US/Eastern')
  totalSeconds = 0.0
  currEst = datetime.datetime.now(est)

  nextOpen = currEst.date() + datetime.timedelta(days=1)
  nextOpen = datetime.datetime.combine(nextOpen, datetime.time(9,30,0))
  timeTillOpen = relativedelta(nextOpen, currEst.date())

  if(timeTillOpen.days > 1 or timeTillOpen.days < 0):
    err = ScraperError("time till open error", "timeToOpen.py", "on line 25")
    logMsg(str(err), "err")
    logMsg("", "final")
    raise err
  if (timeTillOpen.days == 1):
    currMidnight = datetime.datetime.combine(currEst.date(), datetime.time(0,0,0))
    timeTillMidnight = est.localize(currMidnight) - currEst
    secondsTillMidnight = timeTillMidnight.total_seconds() + 86400
    totalSeconds += secondsTillMidnight
  totalSeconds += (timeTillOpen.hours * 3600)
  totalSeconds += (timeTillOpen.minutes * 60)
  remaining = str(datetime.timedelta(seconds=totalSeconds)).split(':')
  if(displaytime):
    print("--- Time till 3:30am HST / 9:30AM EST | ", remaining[0]+' Hours', remaining[1]+' Minutes', remaining[2]+' Seconds')

  return totalSeconds

def timeAmbigToOpen(displaytime=True):
  now = datetime.datetime.now() ##current time in MT Date
  now_hour = int(now.strftime("%I"))  ##current hour in MT 
  now_minute = int(now.strftime("%M"))  ##current minute in MT
  am_pm = now.strftime("%p")  ##am or pm

  market_open = datetime.datetime.strptime("3:30", "%H:%M")  ##market open time in MT Date
  market_open_hour = int(market_open.strftime("%I"))  ##market open hour in MT
  market_open_minute = int(market_open.strftime("%M"))  ##market open minute in MT

  #PM
  if am_pm == "PM":
    difference_hours = (market_open_hour - now_hour) + 12

  #AM
  if am_pm == "AM":
    difference_hours = (market_open_hour - now_hour) + 24  ##time since 3:30am + 24 hours

  difference_minutes = market_open_minute - now_minute  ##time since 3:30am in minutes

  if difference_minutes < 0:
    difference_minutes = (difference_minutes + 60)  ##compensate for negative minutes
    difference_hours = difference_hours - 1

  difference_seconds = (difference_hours * 3600) + (difference_minutes * 60)

  if(displaytime):
    print("--- Time till 3:30am: ", difference_hours, " hours and ", difference_minutes, " minutes")

  return abs(difference_seconds)
