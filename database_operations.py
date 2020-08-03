import sqlite3
from sqlite3 import Error

def createConnection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES)
    except Error as e:
        print(e)

    return conn

def selectAllStocks(conn):
    """
    Query all rows in the stocks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM stocks")

    rows = cur.fetchall()

    return rows

def selectStockByTicker(conn, stock):
    """
    Query all rows in database matching a specific stock ticker symbol
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM stocks WHERE name ='" + stock +"'")

    rows = cur.fetchall()

    return rows
        
def createTable(conn, createTableSQL):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(createTableSQL)
    except Error as e:
        print(e)
        


def createStock(conn, stock):
    """
    Add stock data to to the database
    :param conn:
    :param task:
    :return:
    """

    sql = ''' INSERT INTO stocks(name,date,open,high,low,close,volume)
              VALUES(?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    
    cur.execute(sql, stock)
    conn.commit()

    return cur.lastrowid
    
def assertStockEntry(conn, stock, date):
    """
    Query all rows in database matching a specific stock ticker symbol and date
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM stocks WHERE name ='" + stock + "' AND date='"+date+"'")

    rows = cur.fetchall()

    return rows