import logging
import os
import datetime
date = str(datetime.datetime.now().date())
year = date.split('-')[0]
from pathlib import Path
basedir = str(Path().cwd())

def logMsg(msg, type):
    op = 'a'
    if(not os.path.exists(basedir + f'\logs\scraper_{year}.txt')): op = 'w'

    with open(basedir + f'\logs\scraper_{year}.txt', op) as txt:
        # txt.write(type)
        if(type == 'initial'):
            txt.write('-------------\n')
        elif(type == 'msg'):
            txt.write(f'--- {msg}\n')
        elif(type == 'marketclosed'):
            txt.write(f'MARKET CLOSED {msg}\n')
        elif(type == 'err'):
            txt.write(f'ERROR : {msg}\n')
        elif(type == 'final'):
            txt.write('-------------\n\n')

