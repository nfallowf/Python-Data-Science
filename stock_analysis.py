"""
Author Nathan Fallowfield
Python script to lookup and plot stock information
Data can be store in an sqllite database, CRUD functionality is provided in database_operations.py
Script uses pandas to create dataframes from the csv fiels allowing easy data manipulation.
Data is plotted via Matplotlib, supporting functions are found in plotHelpers.py
Program assumes .csv files containing stock or ETF data can be found in ./Stocks and ./ETFs respectively.
"""

import pandas as pd
import plotHelpers as plot
import database_operations as db
from pathlib import Path

useDate = False                     # Option to limit stock info to a date range
SAVE_TO_DB = False                 # Set to true to insert stock info to db after lookup
database = r".\db\pythonsqlite.db" # Location of database, must create db folder if it doesn't exist

# If the stocks table doesn't already exist in the database, create it
def createStocksTable():
    sqlCreateStocksTable = """ CREATE TABLE IF NOT EXISTS stocks (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        date text NOT NULL,
                                        open real,
                                        high real,
                                        low real, 
                                        close real,
                                        volume real
                                    ); """
    conn = db.createConnection(database)
    db.createTable(conn, sqlCreateStocksTable)
    conn.commit()
    conn.close()
    
def createETFsTable():
    sqlCreateETFsTable = """ CREATE TABLE IF NOT EXISTS etfs (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        date text NOT NULL,
                                        open real,
                                        high real,
                                        low real, 
                                        close real,
                                        volume real
                                    ); """
    conn = db.createConnection(database)
    db.createTable(conn, sqlCreateETFsTable)
    conn.commit()
    conn.close()
# Ask user to input a ticker symbol to lookup
def getUserSymbol():
    type = input("Do you want to lookup a stock or an ETF?: ")
    if (type == "stock"):
        selectedType = "Stocks/"
    else:
        selectedType = "ETFs/"

    stock = input("Enter ticker symbol: ")
    return (selectedType, stock)
    
# Create a panadas dataframe containing info about the requested ticker symbol
def createDataframe(selectedType, stock):
    df = pd.read_csv('./' + selectedType + stock + '.us.txt', parse_dates=True, index_col=0) # Fill dataframe with data from csv
    df['RA'] = df['Close'].rolling(window=100, min_periods=0).mean()                         # Create 100 day moving average in new column of dataframe
    return df
   
# Get range of dates to limit data to   
def getDateRange():
    beginDate = input("Enter beginning date (YYYY-MM-DD): ")
    endDate = input("Enter end date (YYYY-MM-DD): ")
    return (beginDate, endDate)

# Prints all stocks in database
def getAllStocks():
    conn = db.createConnection(database)
    stocks = db.selectAllStocks(conn)
    conn.commit()
    conn.close()
    for item in stocks:
        list = [isNumber(i) for i in item]
        print(list)

# Return all data about a particular stock
def getStockByTicker(stock):
    conn = db.createConnection(database)
    stocks = db.selectStockByTicker(conn, stock)
    conn.commit()
    conn.close()
    for item in stocks:
        list = [isNumber(i) for i in item]
        print(list)

# Insert stock to database
def insertStock(stock):
    conn = db.createConnection(database)
    count = db.assertStockEntry(conn, stock[0], stock[1])
    if (len(count) == 0):
        stocks = db.createStock(conn, stock)
    conn.commit()
    conn.close()

# Insert etf to database
def insertETF(etf):
    conn = db.createConnection(database)
    count = db.assertETFEntry(conn, etf[0], etf[1])
    if (len(count) == 0):
        stocks = db.createETF(conn, etf)
    conn.commit()
    conn.close()
    
# Given a float, return it rounded to 2 decimal places, return the input value for all other inputs    
def isNumber(s):
    try:
        float(s)
        return round(s, 2)
    except ValueError:
        return s
        
def main():
    Path("./db").mkdir(exist_ok=True)           # Create db folder if it doesn't exist
    createStocksTable()                         # Create stocks table if it doesn't exist
    selectedType, stock = getUserSymbol()       # Get the stock to lookup from the user
    df = createDataframe(selectedType, stock)   # Create dataframe containing data about a stock, pulled from a csv file
    
    # Optionally limit the data to a specific date range (improves database performance by limiting data if using large dataset)
    if useDate:
        beginDate, endDate = getDateRange()
        df = (df.loc[beginDate:endDate])
        
    # Plot the closing daily price as well as the 100 day rolling average closing price
    """
    if(len(df.index) > 100):
        plot.plotData(df['Close'], df['RA'])
    else:
        plot.plotData(df['Close'])
    """
    plot.plotVolume(df)
    
    if (SAVE_TO_DB):
        # Inserting the selected stock into the database
        for index, row in df.iterrows():
            stockData = (stock, index.date().strftime("%Y/%m/%d") ,row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
            if (selectedType == 'Stocks/'):
                insertStock(stockData)
            else:
                insertETF(stockData)
    getAllStocks()
    
if __name__ == '__main__':
    main()
