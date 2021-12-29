from typing import Counter
import mysql.connector
import re
import logging

logging.basicConfig(filename='debug_log.log',level=logging.DEBUG)

def connect():
    """database connection"""
    db_config = {
        'host': 'localhost',
        'database': 'BIWOCO',
        'user': 'root',
        'password': 'Hoangdat77ct'
    }
    conn = None
    try:
        conn = mysql.connector.MySQLConnection(**db_config)
        if conn.is_connected():
            return conn
    except mysql.connector.Error as error:
        logging.error(error)
    return conn


def insert_mysql(url,authors,title,tags,summary,publish_date,image_url,domain):
    """ add data to mysql database """
    query = "INSERT INTO articles(url,authors,title,tags,summary,publish_date,image_url,domain) " \
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
    args = (url,authors,title,tags,summary,publish_date,image_url,domain)
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(query,args)
        conn.commit()
    except mysql.connector.Error as e:
        logging.error(e)
    finally:
        cursor.close()
        conn.close()


def iter_row(cursor, size=10):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row


def get_data(query,key1,temp):
    query += key1 + temp
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            logging.info(row)
    except mysql.connector.Error as e:
        logging.error(e)
    finally:
        # Đóng kết nối
        cursor.close()
        conn.close()


def get_latest_news(top=10):
    query = "SELECT title,authors,summary,tags,publish_date,image_url " \
    "FROM articles ORDER BY publish_date DESC LIMIT" + str(top)
    get_data(query,"","")


def get_news_by_tag(key,*keys):
    temp =''
    key = 'WHERE tags LIKE "%'+key+'%"'
    if keys:
        for i in keys:
            temp += ' or tags LIKE "%'+str(i)+'%"'
    query = "SELECT title,authors,summary,tags,publish_date,image_url FROM articles "
    get_data(query,key,temp)

def get_news_by_domain(key,*keys):
    temp =''
    key = 'WHERE domain LIKE "%'+key+'%"'
    if keys:
        for i in keys:
            temp += ' or domain LIKE "%'+str(i)+'%"'
    query = "SELECT title,authors,summary,tags,publish_date,image_url,domain FROM articles "
    get_data(query,key,temp)

def get_topkeywords():
    query = "SELECT tags FROM articles WHERE publish_date > DATE_SUB(NOW(), INTERVAL 7 DAY)"
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(query)
        rs = []
        rows = cursor.fetchall()
        for row in rows:
            rs += re.sub(r"([\s\(\)\'])","",str(row)).split(",")
            rs.remove("")
        temp = sorted(Counter(rs).items(), key=lambda i: i[1], reverse=True)
        for i in range(0,10):
            logging.error(temp[i][0])
    except mysql.connector.Error as e:
        logging.error(e)
    finally:
        cursor.close()
        conn.close()