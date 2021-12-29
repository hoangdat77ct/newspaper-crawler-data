import crawl_data as crawl
import query_sql

URL = "https://news.ycombinator.com/news?p="
if __name__ == '__main__':

    for page in range(1,11):
        crawl.get_url(URL+str(page))

    query_sql.get_news_by_tag("images","os")
    query_sql.get_news_by_domain("github.com")
    query_sql.get_latest_news(15)
    query_sql.get_topkeywords()