import os
import robin_stocks.robinhood as rs
import pyotp
from dotenv import load_dotenv
from pathlib import Path
basepath = Path()
basedir = str(basepath.cwd())
envars = basepath.cwd() / '.env'
load_dotenv(envars)

## if it doesnt log in try deleteing pickle file from C:\Users\{username}\.tokens

def auth():
  # TOTP_KEY = os.getenv('RH_TOTP')
  USER = os.getenv('RH_USER')
  PASS = os.getenv('RH_PASSWORD')

  # totp = pyotp.TOTP(TOTP_KEY).now() 

  login = rs.login(USER, PASS)
  if(login['expires_in'] > 0):
    return True
  elif(login['expires_in'] < 0):
    return False
    
