from database.connect import getDatabase

def getCollection(name, stock):
    client = getDatabase()
    db = client[str(name)]
    return db[str(stock.upper())]
