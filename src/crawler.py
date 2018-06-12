#!/usr/bin/env python3
import requests, mimetypes, json, os, sys, logging, datetime, traceback, time, hashlib
import abc

current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

from model import Page, FetchQueue
from pyquery import PyQuery as pq

last_requst_time = datetime.datetime.now()

class Crawler(object):
    __metaclass__ = abc.ABCMeta
    __config_dir = os.path.join(parent_dir, 'config')
    __config_path = os.path.abspath(os.path.join(__config_dir, 'crawler.yaml'))
    __db_dir = os.path.abspath(os.path.join(parent_dir, 'db'))
    __db_crawler_path = os.path.abspath(os.path.join(parent_dir, 'db', 'crawler.db'))
    __db_fetch_queue_path = os.path.abspath(os.path.join(parent_dir, 'db', 'fetch_queue.db'))
    __data_dir = os.path.abspath(os.path.join(parent_dir, 'data'))
    try: os.makedirs(__config_dir)
    except: pass
    try: os.makedirs(__db_dir)
    except: pass
    try: os.makedirs(__data_dir)
    except: pass

    
    def __init__(self):
        #self.__config = Config(self.__config_path)

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        self.__engine_crawler = create_engine('sqlite:///{}'.format(self.__db_crawler_path), echo=False)
        SessionCrawler = sessionmaker()
        SessionCrawler.configure(bind=self.__engine_crawler)
        Page.metadata.create_all(self.__engine_crawler)
        self.__session_crawler = SessionCrawler()

        self.__engine_fetch_queue = create_engine('sqlite:///{}'.format(self.__db_fetch_queue_path), echo=False)
        SessionFetchQueue = sessionmaker()
        SessionFetchQueue.configure(bind=self.__engine_fetch_queue)
        FetchQueue.metadata.create_all(self.__engine_fetch_queue)
        self.__session_fetch_queue = SessionFetchQueue()

        #self.__last_requst_time = datetime.datetime.now()
    
    @abc.abstractmethod
    def get_cookies(self, url):
        return {}

    @abc.abstractmethod
    def url_to_file_path(self, url, content_hash):
        return hashlib.md5('{0}{1}'.format(url, content_hash).encode('utf-8')).hexdigest() + '.html'

    def request_page(self, url):
        global last_requst_time
        import hashlib
        from sqlalchemy import desc
        query_result = self.__session_crawler.query(Page).filter_by(url=url).order_by(desc(Page.mtime)).first()
        if query_result and os.path.isfile(os.path.join(self.__data_dir, query_result.file_path)):
            return open(os.path.join(self.__data_dir, query_result.file_path), 'rb').read()
        now = datetime.datetime.now()
        print('[{0}] Request {1}'.format(now.strftime("%Y-%m-%d %H:%M:%S.%f"),url))
        requst_timedelta = (now - last_requst_time).microseconds
        if requst_timedelta < 500000: # 0.5s
            time.sleep(0.5 - requst_timedelta/1000000)
        request_args = {
            'cookies': self.get_cookies(url)
        }
        r = requests.get(url, **request_args)
        #print('[{0}] Receive {1}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),url))
        
        last_requst_time = now
        if r is None or r.status_code != 200 or r.content is None:
            return '<html></html>'

        content_hash = hashlib.md5(r.content).hexdigest()
        file_path = os.path.abspath(os.path.join(self.__data_dir, self.url_to_file_path(url, content_hash)))
        try: os.makedirs(os.path.dirname(file_path))
        except: pass
        page = Page(url=url, size=len(r.content), file_path=file_path, content_hash=content_hash, mtime=now)
        self.__session_crawler.add(page)
        self.__session_crawler.commit()
        open(os.path.join(self.__data_dir, file_path), 'wb').write(r.content)
        return r.content        

    def start(self, restart=False):
        while True:
            if not restart:
                query_result = self.__session_fetch_queue.query(FetchQueue).filter_by(mtime=None).first()
                if query_result is None:
                    restart = True
            
            if restart:
                self.__session_fetch_queue.query(FetchQueue).delete()
                entry_points = self.entry_points()
                if entry_points is None:
                    restart = False
                    continue
                if isinstance(entry_points, str):
                    entry_points = [entry_points]
                for entry_url in entry_points:
                    self.__session_fetch_queue.add(FetchQueue(url=entry_url))
                self.__session_fetch_queue.commit()
            
            while True:
                query_result = self.__session_fetch_queue.query(FetchQueue).filter_by(mtime=None).first()
                if query_result is None:
                    break
                url = query_result.url            
                page = self.request_page(url)
                links = self.parse_following_links(url, page)
                if links and len(links) > 0:
                    for link in links:
                        if self.__session_fetch_queue.query(FetchQueue).filter_by(url=link).first() is None:
                            self.__session_fetch_queue.add(FetchQueue(url=link))
                now = datetime.datetime.now()
                query_result.mtime = now
                self.__session_fetch_queue.commit()
                print('[{0}] Parsing Done! {1}'.format(now.strftime("%Y-%m-%d %H:%M:%S.%f"),url))
            restart = False
    
    @abc.abstractmethod
    def entry_points(self):
        return NotImplemented

    @abc.abstractmethod
    def parse_following_links(self, url, page):
        return NotImplemented

class PTTWebCrawler(Crawler):
    base_url = 'https://www.ptt.cc'
    hotboard_url = '{0}/bbs/hotboards.html'.format(base_url)

    def __init__(self):
        super().__init__()
        
    def get_cookies(self, url):
        return { 'over18': '1'}

    def entry_points(self):
        hotboard_page = self.request_page(self.hotboard_url)
        doc = pq(hotboard_page)
        ret = []
        for board_tag in doc('.board').items():
            if board_tag.attr('href') is None:
                continue
            ret.append('{0}{1}'.format(self.base_url, board_tag.attr('href')))
        return ret
    
    def url_to_file_path(self, url, content_hash):
        dirname = url.replace('{0}/bbs/'.format(self.base_url), '').split('/')
        dirname = dirname[0] if len(dirname) > 1 else 'common'
        return os.path.join(dirname, hashlib.md5('{0}{1}'.format(url, content_hash).encode('utf-8')).hexdigest() + '.html')
    
    def parse_following_links(self, url, page):
        if '/index' not in url or not url.endswith('hotboards.html'):
            return []
        doc = pq(page)
        ret = []
        for page_tag in doc('div.btn-group-paging > a.btn').items():
            if page_tag.attr('href') is None:
                continue
            ret.append('{0}{1}'.format(self.base_url, page_tag.attr('href')))
        for post_tag in doc('div.r-ent > div.title > a').items():
            if post_tag.attr('href') is None:
                continue
            ret.append('{0}{1}'.format(self.base_url, post_tag.attr('href')))
        return ret

if __name__ == '__main__':
    PTTWebCrawler().start()
    
    
    
    
    
    