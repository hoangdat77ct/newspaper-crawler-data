from newspaper import Article
import requests
from pyquery import PyQuery as pq
import re
from urllib.parse import urlparse
import datetime
import query_sql


def read_url(url):
    pageSource = requests.get(url).content
    return pq(pageSource)


def get_url(page):
    response = read_url(page)
    articles = response.find("tr.athing > td.title > a.titlelink")
    for article in articles.items():
        link = article.attr('href')
        if re.search(r"\bhttp",link):
            try:
                get_details(link)
            except:
                continue


def get_details(page):
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
    query_sql.insert_mysql(page,re.sub(r"[\[\]\']", "", str(authors)), \
        title,re.sub(r"[\[\]\']", "",str(tags)),summary,publish_date,image_url,domain)
