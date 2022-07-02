import csv
import datetime
from database.interface import getCollection

class Processor():

    def processCsvForBt(self, fromPath, toPath):
        fieldNames = ['Date','Open','High','Low','Close','Volume']
        with open(toPath, 'w', newline='') as csvfile: #write headers
            writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=fieldNames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()

        with open(fromPath, 'r', newline='') as csvfile: # read original csv data
            reader = csv.DictReader(csvfile, delimiter=',')
            for row in reader: #copy data to new csv line by line, excluding 'Time'
                btData = { 
                    'Date': row['Date'],
                    'Open': row['Open'],
                    'High': row['High'],
                    'Low': row['Low'],
                    'Close': row['Close'],
                    'Volume': row['Volume']
                }
                with open(toPath, 'a', newline='') as btCsvfile: 
                    writer = csv.DictWriter(btCsvfile, delimiter=',', fieldnames=fieldNames)
                    writer.writerow(btData)
            btCsvfile.close()

    # writes one file to DB as a new record
    # fromPath must be a relative path to a csv file with no '/' at the end. ex: '../data/daily/2022/SPY/2022-06-23_SPY.csv'
    def writeCsvToDB(self, fromPath):
        stock = fromPath.split('/')[-2]
        year = fromPath.split('/')[-3]
        marketData = []
        date = ''
        print("writing ", stock, fromPath.split('/')[-1])
        with open(fromPath, 'r', newline='') as csvfile: # read original csv data
            reader = csv.DictReader(csvfile, delimiter=',')
            date = next(reader)['Date']
            for row in reader:
                rowData = { 
                    'Time': row['Time'],
                    'Open': row['Open'],
                    'High': row['High'],
                    'Low': row['Low'],
                    'Close': row['Close'],
                    'Volume': row['Volume']
                }
                marketData.append(rowData)
        newRecord = { 'Date': date, 'MarketData': marketData }
        collection = getCollection(str(year), stock.upper())
        todayRecord = collection.find_one({'Date': date})
        if (not todayRecord):
            collection.insert_one(newRecord)
        else:
            updated = collection.find_one_and_replace({'Date': date}, newRecord)
            if not updated: print('failure to update record: ', date)

