#!/usr/bin/env python3
import requests, mimetypes, json, os, sys, logging, datetime, traceback, time, hashlib
import yaml
import abc

current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
Base = declarative_base()

from pyquery import PyQuery as pq

class Config(object):
    def __init__(self, path=None):
        if path is None:
            raise Exception('Config file is not specified.')
        self.__path = path
        with open(path, 'r') as stream:
            self.__content = yaml.load(stream)
    
    def save(self, path=None):
        if path is None:
            path = self.__path
        with open(path, 'w') as stream:
            stream.write(yaml.dump(self.__content))
        self.__path = path
    
    def __getitem__(self, key):
        if key not in self.__content:
            return None
        return self.__content[key]

class Page(Base):
    __tablename__ = 'page'
    id = Column(Integer, primary_key=True)
    url = Column(String, index=True)
    size = Column(Integer)
    file_path = Column(String, index=True)
    content_hash = Column(String)
    mtime = Column(DateTime, index=True)

class FetchQueue(Base):
    __tablename__ = 'fetch_queue'
    id = Column(Integer, primary_key=True)
    url = Column(String, index=True)
    mtime = Column(DateTime, index=True)

class Crawler(object):
    __metaclass__ = abc.ABCMeta
    __config_dir = os.path.join(parent_dir, 'config')
    __config_path = os.path.abspath(os.path.join(__config_dir, 'crawler.yaml'))
    __db_dir = os.path.abspath(os.path.join(parent_dir, 'db'))
    __db_path = os.path.abspath(os.path.join(parent_dir, 'db', 'crawler.db'))
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
        
        self.__engine = create_engine('sqlite:///{}'.format(self.__db_path), echo=True)
        Session = sessionmaker()
        Session.configure(bind=self.__engine)
        Base.metadata.create_all(self.__engine)
        self.__session = Session()
        self.__last_requst_time = datetime.datetime.now()
    
    @abc.abstractmethod
    def get_cookies(self, url):
        return {}

    @abc.abstractmethod
    def url_to_file_path(self, url, content_hash):
        return hashlib.md5('{0}{1}'.format(url, content_hash).encode('utf-8')).hexdigest() + '.html'

    def request_page(self, url):
        import hashlib
        from sqlalchemy import desc
        query_result = self.__session.query(Page).filter_by(url=url).order_by(desc(Page.mtime)).first()
        if query_result:
            return open(os.path.join(self.__data_dir, query_result.file_path), 'rb').read()
        now = datetime.datetime.now()
        print('[{0}] Request {1}'.format(now.strftime("%Y-%m-%d %H:%M:%S"),url))
        requst_timedelta = (now - self.__last_requst_time).microseconds
        if requst_timedelta < 500000: # 0.5s
            time.sleep(0.5 - requst_timedelta/1000000)
        request_args = {
            'cookies': self.get_cookies(url)
        }
        r = requests.get(url, **request_args)
        if r is None or r.status_code != 200 or r.content is None:
            return ''

        content_hash = hashlib.md5(r.content).hexdigest()
        file_path = os.path.abspath(os.path.join(self.__data_dir, self.url_to_file_path(url, content_hash)))
        try: os.makedirs(os.path.dirname(file_path))
        except: pass
        page = Page(url=url, size=len(r.content), file_path=file_path, content_hash=content_hash, mtime=now)
        self.__session.add(page)
        self.__session.commit()
        open(os.path.join(self.__data_dir, file_path), 'wb').write(r.content)
        return r.content        

    def start(self, restart=False):
        if not restart:
            query_result = self.__session.query(FetchQueue).filter(FetchQueue.mtime==None).first()
            if query_result is None:
                restart = True
        
        if restart:
            self.__session.query(FetchQueue).delete()
            entry_points = self.entry_points()
            if entry_points is None:
                return
            if isinstance(entry_points, str):
                entry_points = [entry_points]
            for entry_url in entry_points:
                self.__session.add(FetchQueue(url=entry_url))
            self.__session.commit()
        
        while True:
            query_result = self.__session.query(FetchQueue).filter(FetchQueue.mtime==None).first()
            if query_result is None:
                break
            url = query_result.url            
            page = self.request_page(url)
            links = self.parse_following_links(page)
            if links and len(links) > 0:
                for link in links:
                    if self.__session.query(FetchQueue).filter(url==link).first() is None:
                        self.__session.add(FetchQueue(url=link))
            query_result.mtime = datetime.datetime.now()
            self.__session.commit()
    
    @abc.abstractmethod
    def entry_points(self):
        return NotImplemented

    @abc.abstractmethod
    def parse_following_links(self, page):
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
    
    def parse_following_links(self, page):
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
    pttWebCrawler = PTTWebCrawler()
    while True:
        pttWebCrawler.start()
        time.sleep(3600)
    