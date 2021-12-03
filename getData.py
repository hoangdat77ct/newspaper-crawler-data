from typing import Counter
from newspaper import Article
import requests
from pyquery import PyQuery as pq
import re
import mysql.connector
from urllib.parse import urlparse
import datetime



def read_url(url):
    """Read given Url , Returns pyquery object for page content"""
    pageSource = requests.get(url).content
    return pq(pageSource)

def get_url(page):
    """read 'page' url  items to database"""
    response = read_url(page)
    articles = response.find("tr.athing > td.title > a.titlelink")
    for article in articles.items():
        link = article.attr('href')
        if re.search(r"\bhttp",link):
            #dataUrl.append(link)
            try:
                get_details(link)
            except:
                continue


def get_details(page):
    '''get details in each URL '''
    article = Article(page)
    article.download()
    article.parse()
    article.nlp()
    authors = article.authors
    title =  article.title
    tags = article.keywords
    summary = article.summary
    publish_date = article.publish_date
    if publish_date is not None:
        publish_date = publish_date.strftime('%Y-%m-%d')
    else:
        publish_date = datetime.date.today().strftime('%Y-%m-%d')
    image_url = article.top_image
    domain = urlparse(page).netloc
    #dataSet.append([page,authors,title,tags,summary,publish_date,image_url,domain])
    insert_mysql(page,re.sub(r"[\[\]\']", "", str(authors)),title,re.sub(r"[\[\]\']", "", str(tags)),summary,publish_date,image_url,domain)


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
        print(error)
 
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
        print(e)
    finally:
        cursor.close()
        conn.close()


def iter_row(cursor, size=10):
    """ get 10(size) rows"""
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row

def get_data(query,key1,temp):
    ''' func query'''
    query += key1 + temp
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(query)
 
        #for row in iter_row(cursor, 10):
        rows = cursor.fetchall()
        for row in rows:
            print(row)
 
    except mysql.connector.Error as e:
        print(e)
 
    finally:
        # Đóng kết nối
        cursor.close()
        conn.close()




#title,summary,publish_date,
def get_latest_news(top=10):
    query = "SELECT title,authors,summary,tags,publish_date,image_url FROM articles ORDER BY publish_date DESC LIMIT " + str(top)
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

        #for row in iter_row(cursor, 10):
        rows = cursor.fetchall()
        for row in rows:
            rs += re.sub(r"([\s\(\)\'])","",str(row)).split(",") 
            #.replace(r"[(\']","").split(", ")
            rs.remove("")
        temp = sorted(Counter(rs).items(), key=lambda i: i[1], reverse=True)
        for i in range(0,10):
            print(temp[i][0])
    except mysql.connector.Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    url = "https://news.ycombinator.com/news?p="

    for page in range(1,11):
        get_url(url+str(page))

    #get_news_by_tag("images","os")
    #get_news_by_domain("github.com")
    #get_latest_news(15)
    #get_topkeywords()    
        