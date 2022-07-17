import datetime 
import robin_stocks.robinhood as rs
import os
import csv
from database.interface import getCollection
from logs.logger import logMsg


class HarvestData():

    def validateLocation(self,stock, path, date, fieldNames):
        if(not os.path.exists(path)): #if path doesnt exists, create path
            os.makedirs(path)
        if(not os.path.exists(path + f'{date}_{stock.upper()}.csv')): #if file doesnt exists, create file
            with open(path + f'{date}_{stock.upper()}.csv', 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=fieldNames, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()

    # Appends last 5 minute trade data to DB collection in /$year/$stock
    # Meant for data harvesting DURING MARKET HOURS ONLY. Run every 5 minutes. 
    def harvestDailyDataToDB(self, stock):
        date = str(datetime.datetime.now().date())
        year = date.split('-')[0]
        try:
            dataDict = rs.get_stock_historicals(stock, '5minute', 'day')[-1]
            date = str(dataDict['begins_at']).split('T')[0]
            time = str(dataDict['begins_at']).split('T')[1]
            marketData = {
                            'Time': time,
                            'Open': float(dataDict['open_price']), 
                            'High': float(dataDict['high_price']), 
                            'Low': float(dataDict['low_price']), 
                            'Close': float(dataDict['close_price']), 
                            'Volume': float(dataDict['volume'])
                        }
            initialRecord = { 'Date': date, 'MarketData': [ marketData ] }

            collection = getCollection(str(year), stock.upper())
            todayRecord = collection.find_one({'Date': date})
            if (not todayRecord):
                collection.insert_one(initialRecord)
            else:
                updatedRecord = todayRecord['MarketData']
                updatedRecord.append(marketData)
                updated = collection.find_one_and_replace({'Date': date}, {'Date': date, 'MarketData': updatedRecord})
                if not updated: logMsg('failure to update record for f{date} f{time}: ', "err")
        except:
            return

    # Appends last 5 minute trade data to csv in /data/daily
    # Meant for data harvesting DURING MARKET HOURS ONLY. Run every 5 minutes. 
    def harvestDailyDataToCsv(self, stock):
        date = str(datetime.datetime.now().date())
        year = date.split('-')[0]
        path = f'../../data/daily/{year}/{stock.upper()}/'
        fieldNames = ['Date','Time','Open','High','Low','Close','Volume']
        self.validateLocation(stock, path, date, fieldNames)

        try:
            dataDict = rs.get_stock_historicals(stock, '5minute', 'day')[-1]
            date = str(dataDict['begins_at']).split('T')[0]
            time = str(dataDict['begins_at']).split('T')[1]
            with open(path + f'{date}_{stock.upper()}.csv', 'a', newline='') as csvfile: 
                writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=fieldNames)
                writer.writerow({
                    'Date': date, 
                    'Time': time, 
                    'Open': float(dataDict['open_price']), 
                    'High': float(dataDict['high_price']), 
                    'Low': float(dataDict['low_price']), 
                    'Close': float(dataDict['close_price']), 
                    'Volume': float(dataDict['volume'])
                })
                csvfile.close()
        except:
            return
        
    # Writes all 5 minute trade data from day to record in /data/daily
    # Meant to be a ONE RUN csv writer.   
    def writeCurrentDataToCsv(self, stock):
        date = str(datetime.datetime.now().date())
        year = date.split('-')[0]
        path = f'../../data/daily/{year}/{stock.upper()}/'
        fieldNames = ['Date','Time','Open','High','Low','Close','Volume']
        self.validateLocation(stock, path, date, fieldNames) ##create file if it doesnt exist

        with open(path + f'{date}_{stock.upper()}.csv', 'r+') as f: ##remove anything in file that may exist, except headers
            f.readline()
            f.truncate(f.tell())

        dataDict = rs.get_stock_historicals(stock, '5minute', 'day') ##add all 5min data from day
        for item in dataDict:
            date = str(item['begins_at']).split('T')[0]
            time = str(item['begins_at']).split('T')[1]
            with open(path + f'{date}_{stock.upper()}.csv', 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=fieldNames)
                writer.writerow({
                    'Date': date, 
                    'Time': time, 
                    'Open': float(item['open_price']), 
                    'High': float(item['high_price']), 
                    'Low': float(item['low_price']), 
                    'Close': float(item['close_price']), 
                    'Volume': float(item['volume'])
                })
                csvfile.close()

    # Writes all 5 minute trade data from day to DB collection in /$year/$stock
    # Meant to be a ONE RUN writer.   
    def writeCurrentDataToDB(self, stock):
        date = str(datetime.datetime.now().date())
        year = date.split('-')[0]
        try:
            dataDict = rs.get_stock_historicals(stock, '5minute', 'day')
            marketData = []
            for item in dataDict:
                time = str(item['begins_at']).split('T')[1]
                itemData = {
                                'Time': time,
                                'Open': float(item['open_price']), 
                                'High': float(item['high_price']), 
                                'Low': float(item['low_price']), 
                                'Close': float(item['close_price']), 
                                'Volume': float(item['volume'])
                            }
                marketData.append(itemData)
            
            newRecord = {'Date': date, 'MarketData': marketData}
            collection = getCollection(str(year), stock.upper())
            todayRecord = collection.find_one({'Date': date})
            if (not todayRecord):
                collection.insert_one(newRecord)
            else:
                updated = collection.find_one_and_replace({'Date': date}, newRecord)
                if not updated: logMsg('failure to update record for f{date} f{time}: ', "err")
        except:
            return

    def harvestTestData(self, stock, data):
        date = str(datetime.datetime.now().date())
        year = date.split('-')[0]
        path = f'../../data/daily/{year}/{stock.upper()}/'
        fieldNames = ['Date','Time','Open','High','Low','Close','Volume']
        self.validateLocation(stock, path, date, fieldNames)
        date = str(data['begins_at']).split('T')[0]
        time = str(data['begins_at']).split('T')[1]
        with open(path + f'{date}_{stock.upper()}.csv', 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=fieldNames)
            writer.writerow({
                'Date': date, 
                'Time': time, 
                'Open': float(data['open_price']), 
                'High': float(data['high_price']), 
                'Low': float(data['low_price']), 
                'Close': float(data['close_price']), 
                'Volume': float(data['volume'])
            })
            csvfile.close()
