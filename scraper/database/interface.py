from database.connect import getDatabase

def getCollection(name, stock):
    client = getDatabase()
    db = client[name]
    return db[stock]