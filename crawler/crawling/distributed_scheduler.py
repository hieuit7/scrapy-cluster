from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from past.builtins import basestring
from six import string_types
from builtins import object
from scrapy.http import Request
from scrapy.settings import Settings as settings 
from scrapy.utils.python import to_unicode
from scrapy.utils.reqser import request_to_dict, request_from_dict

import redis
import random
import time
import tldextract
import urllib.request, urllib.error, urllib.parse
import re
import yaml
import sys
import uuid
import socket
import re
import ujson
import json

from crawling.redis_dupefilter import RFPDupeFilter
from crawling.redis_global_page_per_domain_filter import RFGlobalPagePerDomainFilter
from crawling.redis_domain_max_page_filter import RFDomainMaxPageFilter
from kazoo.handlers.threading import KazooTimeoutError

from scutils.zookeeper_watcher import ZookeeperWatcher
from scutils.redis_queue import RedisPriorityQueue
from scutils.redis_throttled_queue import RedisThrottledQueue
from scutils.log_factory import LogFactory


class DistributedScheduler(object):
    '''
    Scrapy request scheduler that utilizes Redis Throttled Priority Queues
    to moderate different domain scrape requests within a distributed scrapy
    cluster
    '''
    redis_conn = None # the redis connection
    queue_dict = None # the dict of throttled queues
    spider = None # the spider using this scheduler
    queue_keys = None # the list of current queues
    queue_class = None # the class to use for the queue
    dupefilter = None # the redis dupefilter
    global_page_per_domain_filter = None  # the global redis page per domain filter, applied to all domains.
    domain_max_page_filter = None  # the individual domain's redis max page filter.
    update_time = 0 # the last time the queues were updated
    update_ip_time = 0 # the last time the ip was updated
    update_interval = 0 # how often to update the queues
    extract = None # the tld extractor
    hits = 0 # default number of hits for a queue
    window = 0 # default window to calculate number of hits
    my_ip = None # the ip address of the scheduler (if needed)
    old_ip = None # the old ip for logging
    ip_update_interval = 0 # the interval to update the ip address
    add_type = None # add spider type to redis throttle queue key
    add_ip = None # add spider public ip to redis throttle queue key
    item_retries = 0 # the number of extra tries to get an item
    my_uuid = None # the generated UUID for the particular scrapy process
    # Zookeeper Dynamic Config Vars
    domain_config = {}  # The list of domains and their configs
    my_id = None  # The id used to read the throttle config
    config_flag = False  # Flag to reload queues if settings are wiped too
    assign_path = None  # The base assigned configuration path to read
    zoo_client = None  # The KazooClient to manage the config
    my_assignment = None  # Zookeeper path to read actual yml config
    black_domains = [] # the domains to ignore thanks to zookeeper config

    def __init__(self, server, persist, update_int, timeout, retries, logger,
                 hits, window, mod, ip_refresh, add_type, add_ip, ip_regex,
                 backlog_blacklist, queue_timeout, global_page_per_domain_limit,
                 global_page_per_domain_limit_timeout, domain_max_page_timeout, settings=None):
        '''
        Initialize the scheduler
        '''
        
        self.redis_conn = server
        self.persist = persist
        self.queue_dict = {}
        self.update_interval = update_int
        self.hits = hits
        self.window = window
        self.moderated = mod
        self.rfp_timeout = timeout
        self.ip_update_interval = ip_refresh
        self.add_type = add_type
        self.add_ip = add_ip
        self.item_retries = retries
        self.logger = logger
        print("OKL")
        self.ip_regex = re.compile(ip_regex)
        print("OKL")
        self.backlog_blacklist = backlog_blacklist
        self.queue_timeout = queue_timeout
        self.global_page_per_domain_limit = global_page_per_domain_limit
        self.global_page_per_domain_limit_timeout = global_page_per_domain_limit_timeout
        self.domain_max_page_timeout = domain_max_page_timeout

        # set up tldextract
        print("OKL")
        self.extract = tldextract.TLDExtract()
        print("OKL")
        self.update_ipaddress()
        self.settings = settings
        # if we need better uuid's mod this line
        self.my_uuid = str(uuid.uuid4()).split('-')[4]

    def setup_zookeeper(self):
        print(self.settings.get('ZOOKEEPER_ASSIGN_PATH', ""))
        self.assign_path = self.settings.get('ZOOKEEPER_ASSIGN_PATH', "")
        print("làla")
        self.my_id = self.settings.get('ZOOKEEPER_ID', 'all')
        self.logger.debug("Trying to establish Zookeeper connection")
        try:
            print("làla")
            self.zoo_watcher = ZookeeperWatcher(
                                hosts=self.settings.get('ZOOKEEPER_HOSTS'),
                                filepath=self.assign_path + self.my_id,
                                config_handler=self.change_config,
                                error_handler=self.error_config,
                                pointer=False, ensure=True, valid_init=True)
        except KazooTimeoutError:
            self.logger.error("Could not connect to Zookeeper")
            sys.exit(1)

        if self.zoo_watcher.ping():
            self.logger.debug("Successfully set up Zookeeper connection")
        else:
            self.logger.error("Could not ping Zookeeper")
            sys.exit(1)

    def change_config(self, config_string):
        if config_string and len(config_string) > 0:
            loaded_config = yaml.safe_load(config_string)
            self.logger.info("Zookeeper config changed", extra=loaded_config)
            self.load_domain_config(loaded_config)
            self.update_domain_queues()
        elif config_string is None or len(config_string) == 0:
            self.error_config("Zookeeper config wiped")

        self.create_queues()

    def load_domain_config(self, loaded_config):
        '''
        Loads the domain_config and sets up queue_dict
        @param loaded_config: the yaml loaded config dict from zookeeper
        '''
        self.domain_config = {}
        # vetting process to ensure correct configs
        if loaded_config:
            if 'domains' in loaded_config:
                for domain in loaded_config['domains']:
                    item = loaded_config['domains'][domain]
                    # check valid
                    if 'window' in item and 'hits' in item:
                        self.logger.debug("Added domain {dom} to loaded config"
                                          .format(dom=domain))
                        self.domain_config[domain] = item
            if 'blacklist' in loaded_config:
                self.black_domains = loaded_config['blacklist']

        self.config_flag = True

    def update_domain_queues(self):
        '''
        Check to update existing queues already in memory
        new queues are created elsewhere
        '''
        for key in self.domain_config:
            final_key = "{name}:{domain}:queue".format(
                    name=self.spider.name,
                    domain=key)
            # we already have a throttled queue for this domain, update it to new settings
            if final_key in self.queue_dict:
                self.queue_dict[final_key][0].window = float(self.domain_config[key]['window'])
                self.logger.debug("Updated queue {q} with new config"
                                  .format(q=final_key))
                # if scale is applied, scale back; otherwise use updated hits
                if 'scale' in self.domain_config[key]:
                    # round to int
                    hits = int(self.domain_config[key]['hits'] * self.fit_scale(
                               self.domain_config[key]['scale']))
                    self.queue_dict[final_key][0].limit = float(hits)
                else:
                    self.queue_dict[final_key][0].limit = float(self.domain_config[key]['hits'])

    def error_config(self, message):
        extras = {}
        extras['message'] = message
        extras['revert_window'] = self.window
        extras['revert_hits'] = self.hits
        extras['spiderid'] = self.spider.name
        self.logger.info("Lost config from Zookeeper", extra=extras)
        # lost connection to zookeeper, reverting back to defaults
        for key in self.domain_config:
            final_key = "{name}:{domain}:queue".format(
                    name=self.spider.name,
                    domain=key)
            self.queue_dict[final_key][0].window = self.window
            self.queue_dict[final_key][0].limit = self.hits

        self.domain_config = {}

    def fit_scale(self, scale):
        '''
        @return: a scale >= 0 and <= 1
        '''
        if scale >= 1:
            return 1.0
        elif scale <= 0:
            return 0.0
        else:
            return scale

    def create_queues(self):
        '''
        Updates the in memory list of the redis queues
        Creates new throttled queue instances if it does not have them
        '''
        # new config could have loaded between scrapes
        newConf = self.check_config()

        self.queue_keys = self.redis_conn.keys(self.spider.name + ":*:queue")

        for key in self.queue_keys:
            # build final queue key, depending on type and ip bools
            throttle_key = ""

            if self.add_type:
                throttle_key = self.spider.name + ":"
            if self.add_ip:
                throttle_key = throttle_key + self.my_ip + ":"

            # add the tld from the key `type:tld:queue`
            the_domain = re.split(':', key)[1]
            throttle_key = throttle_key + the_domain

            if key not in self.queue_dict or newConf:
                self.logger.debug("Added new Throttled Queue {q}"
                                  .format(q=key))
                q = RedisPriorityQueue(self.redis_conn, key, encoding=ujson)

                # use default window and hits
                if the_domain not in self.domain_config:
                    # this is now a tuple, all access needs to use [0] to get
                    # the object, use [1] to get the time
                    self.queue_dict[key] = [RedisThrottledQueue(self.redis_conn,
                    q, self.window, self.hits, self.moderated, throttle_key,
                    throttle_key, True), time.time()]
                # use custom window and hits
                else:
                    window = self.domain_config[the_domain]['window']
                    hits = self.domain_config[the_domain]['hits']

                    # adjust the crawl rate based on the scale if exists
                    if 'scale' in self.domain_config[the_domain]:
                        hits = int(hits * self.fit_scale(self.domain_config[the_domain]['scale']))

                    self.queue_dict[key] = [RedisThrottledQueue(self.redis_conn,
                    q, window, hits, self.moderated, throttle_key,
                    throttle_key, True), time.time()]

    def expire_queues(self):
        '''
        Expires old queue_dict keys that have not been used in a long time.
        Prevents slow memory build up when crawling lots of different domains
        '''
        curr_time = time.time()
        for key in list(self.queue_dict):
            diff = curr_time - self.queue_dict[key][1]
            if diff > self.queue_timeout:
                self.logger.debug("Expiring domain queue key " + key)
                del self.queue_dict[key]
                if key in self.queue_keys:
                    self.queue_keys.remove(key)

    def check_config(self):
        '''
        Controls configuration for the scheduler
        @return: True if there is a new configuration
        '''
        if self.config_flag:
            self.config_flag = False
            return True

        return False

    def update_ipaddress(self):
        '''
        Updates the scheduler so it knows its own ip address
        '''
        # assign local ip in case of exception
        self.old_ip = self.my_ip
        self.my_ip = '127.0.0.1'
        print("sâsas")
        # print("OK1")
        # try:
        #     obj = urllib.request.urlopen(self.settings.get('PUBLIC_IP_URL',
        #                           'http://ip.42.pl/raw'))
        #     results = self.ip_regex.findall(obj.read().decode('utf-8'))
        #     if len(results) > 0:
        #         self.my_ip = results[0]
        #         print("OKL")
        #     else:
        #         raise IOError("Could not get valid IP Address")
        #     obj.close()
        #     self.logger.debug("Current public ip: {ip}".format(ip=self.my_ip))
        # except IOError:
        #     self.logger.error("Could not reach out to get public ip")
        #     pass

        # if self.old_ip != self.my_ip:
        #     self.logger.info("Changed Public IP: {old} -> {new}".format(
        #                      old=self.old_ip, new=self.my_ip))

    def report_self(self):
        '''
        Reports the crawler uuid to redis
        '''
        self.logger.debug("Reporting self id", extra={'uuid':self.my_uuid})
        key = "stats:crawler:{m}:{s}:{u}".format(
            m=socket.gethostname(),
            s=self.spider.name,
            u=self.my_uuid)
        self.redis_conn.set(key, time.time())
        self.redis_conn.expire(key, self.ip_update_interval * 2)

    @classmethod
    def from_settings(cls, settings):
        server = redis.Redis(host=settings.get('REDIS_HOST'),
                             port=settings.get('REDIS_PORT'),
                             db=settings.get('REDIS_DB'),
                             password=settings.get('REDIS_PASSWORD'),
                             decode_responses=True,
                             socket_timeout=settings.get('REDIS_SOCKET_TIMEOUT'),
                             socket_connect_timeout=settings.get('REDIS_SOCKET_TIMEOUT'))
        persist = settings.get('SCHEDULER_PERSIST', True)
        up_int = settings.get('SCHEDULER_QUEUE_REFRESH', 10)
        hits = settings.get('QUEUE_HITS', 10)
        window = settings.get('QUEUE_WINDOW', 60)
        mod = settings.get('QUEUE_MODERATED', False)
        timeout = settings.get('DUPEFILTER_TIMEOUT', 600)
        ip_refresh = settings.get('SCHEDULER_IP_REFRESH', 60)
        add_type = settings.get('SCHEDULER_TYPE_ENABLED', False)
        add_ip = settings.get('SCHEDULER_IP_ENABLED', False)
        retries = settings.get('SCHEUDLER_ITEM_RETRIES', 3)
        ip_regex = settings.get('IP_ADDR_REGEX', '.*')
        backlog_blacklist = settings.get('SCHEDULER_BACKLOG_BLACKLIST', True)
        queue_timeout = settings.get('SCHEDULER_QUEUE_TIMEOUT', 3600)


        my_level = settings.get('SC_LOG_LEVEL', 'INFO')
        my_name = settings.get('SC_LOGGER_NAME', 'sc-logger')
        my_output = settings.get('SC_LOG_STDOUT', True)
        my_json = settings.get('SC_LOG_JSON', False)
        my_dir = settings.get('SC_LOG_DIR', 'logs')
        my_bytes = settings.get('SC_LOG_MAX_BYTES', '10MB')
        my_file = settings.get('SC_LOG_FILE', 'main.log')
        my_backups = settings.get('SC_LOG_BACKUPS', 5)
        print(settings.get('REDIS_HOST'))
        logger = LogFactory.get_instance(json=my_json,
                                         name=my_name,
                                         stdout=my_output,
                                         level=my_level,
                                         dir=my_dir,
                                         file=my_file,
                                         bytes=my_bytes,
                                         backups=my_backups)

        global_page_per_domain_limit = settings.get('GLOBAL_PAGE_PER_DOMAIN_LIMIT', None)
        global_page_per_domain_limit_timeout = settings.get('GLOBAL_PAGE_PER_DOMAIN_LIMIT_TIMEOUT', 600)
        domain_max_page_timeout = settings.get('DOMAIN_MAX_PAGE_TIMEOUT', 600)
        print(cls)
        return cls(server, persist, up_int, timeout, retries, logger, hits,
                   window, mod, ip_refresh, add_type, add_ip, ip_regex,
                   backlog_blacklist, queue_timeout, global_page_per_domain_limit,
                   global_page_per_domain_limit_timeout, domain_max_page_timeout, settings=settings)

    @classmethod
    def from_crawler(cls, crawler):
        print("from settings")
        return cls.from_settings(crawler.settings)

    def open(self, spider):
        self.spider = spider
        self.spider.set_logger(self.logger)
        print('12312')
        self.create_queues()
        print('1231211111')
        
        self.setup_zookeeper()
        print('sadsas')
        self.dupefilter = RFPDupeFilter(self.redis_conn,
                                        self.spider.name + ':dupefilter',
                                        self.rfp_timeout)
        self.global_page_per_domain_filter = RFGlobalPagePerDomainFilter(self.redis_conn,
                                                                         self.spider.name + ':global_page_count_filter',
                                                                         self.global_page_per_domain_limit,
                                                                         self.global_page_per_domain_limit_timeout)
        self.domain_max_page_filter = RFDomainMaxPageFilter(self.redis_conn,
                                                            self.spider.name + ':domain_max_page_filter',
                                                            self.domain_max_page_timeout)

    def close(self, reason):
        self.logger.info("Closing Spider", {'spiderid':self.spider.name})
        if not self.persist:
            self.logger.warning("Clearing crawl queues")
            self.dupefilter.clear()
            self.global_page_per_domain_filter.clear()
            self.domain_max_page_filter.clear()
            for key in self.queue_keys:
                self.queue_dict[key][0].clear()

    def is_blacklisted(self, appid, crawlid):
        '''
        Checks the redis blacklist for crawls that should not be propagated
        either from expiring or stopped
        @return: True if the appid crawlid combo is blacklisted
        '''
        key_check = '{appid}||{crawlid}'.format(appid=appid,
                                                crawlid=crawlid)
        redis_key = self.spider.name + ":blacklist"
        return self.redis_conn.sismember(redis_key, key_check)

    def enqueue_request(self, request):
        '''
        Pushes a request from the spider into the proper throttled queue
        '''

        # # # # # # # # # # # # # # # # # # Duplicate link Filter # # # # # # # # # # # # # # #
        if not request.dont_filter and self.dupefilter.request_seen(request):
            self.logger.debug("Request not added back to redis")
            return

        # An individual crawling request of a domain's page
        req_dict = request_to_dict(request, self.spider)

        # # # # # # # # # # # # # # # # # # Page Limit Filters # # # # # # # # # # # # # # #
        # Max page filter per individual domain
        if 'meta' in req_dict and 'domain_max_pages' in req_dict['meta'] and req_dict['meta']['domain_max_pages'] and self.domain_max_page_filter.request_page_limit_reached(
                request=request,
                spider=self.spider):
            self.logger.debug("Request {0} reached domain's page limit of {1}".format(
                request.url,
                req_dict['meta']['domain_max_pages']))
            return

        # Global - cluster wide - max page filter
        if self.global_page_per_domain_limit and self.global_page_per_domain_filter.request_page_limit_reached(
                    request=request,
                    spider=self.spider):
            self.logger.debug("Request {0} reached global page limit of {1}".format(
                    request.url,
                    self.global_page_per_domain_limit))
            return


        # # # # # # # # # # # # # # # # # # Blacklist Filter # # # # # # # # # # # # # # #
        if 'meta' in req_dict and 'appid' in req_dict['meta'] and not self.is_blacklisted(req_dict['meta']['appid'],
                                   req_dict['meta']['crawlid']):
            # grab the tld of the request
            ex_res = self.extract(req_dict['url'])
            key = "{sid}:{dom}.{suf}:queue".format(
                sid=req_dict['meta']['spiderid'],
                dom=ex_res.domain,
                suf=ex_res.suffix)

            curr_time = time.time()

            domain = "{d}.{s}".format(d=ex_res.domain, s=ex_res.suffix)

            # allow only if we want all requests or we want
            # everything but blacklisted domains
            # insert if crawl never expires (0) or time < expires
            if (self.backlog_blacklist or
                    (not self.backlog_blacklist and
                    domain not in self.black_domains)) and \
                    (req_dict['meta']['expires'] == 0 or
                    curr_time < req_dict['meta']['expires']):
                # we may already have the queue in memory
                if key in self.queue_keys:
                    self.queue_dict[key][0].push(req_dict,
                                              req_dict['meta']['priority'])
                else:
                    # shoving into a new redis queue, negative b/c of sorted sets
                    # this will populate ourself and other schedulers when
                    # they call create_queues
                    self.redis_conn.zadd(key, {ujson.dumps(req_dict): -req_dict['meta']['priority']})
                self.logger.debug("Crawlid: '{id}' Appid: '{appid}' added to queue"
                    .format(appid=req_dict['meta']['appid'],
                            id=req_dict['meta']['crawlid']))
            else:
                self.logger.debug("Crawlid: '{id}' Appid: '{appid}' expired"
                                  .format(appid=req_dict['meta']['appid'],
                                          id=req_dict['meta']['crawlid']))
        else:
            self.logger.debug("Crawlid: '{id}' Appid: '{appid}' blacklisted"
                              .format(appid=req_dict['meta']['appid'] if 'meta' in req_dict and 'appid' in req_dict['meta'] else "",
                                      id=req_dict['meta']['crawlid'] if 'meta' in req_dict  and 'crawlid' in req_dict['meta'] else ""))

    def find_item(self):
        '''
        Finds an item from the throttled queues
        '''
        random.shuffle(self.queue_keys)
        count = 0

        while count <= self.item_retries:
            for key in self.queue_keys:
                print(f'{key}')
                # skip if the whole domain has been blacklisted in zookeeper
                if key.split(':')[1] in self.black_domains:
                    self.logger.info("key in black domain")
                    continue
                # the throttled queue only returns an item if it is allowed
                item = self.queue_dict[key][0].pop()
                self.logger.info(f"Have item: {item}{self.queue_dict}")
                if item:
                    # update timeout and return
                    self.queue_dict[key][1] = time.time()
                    return item

            count = count + 1

        return None

    def next_request(self):
        '''
        Logic to handle getting a new url request, from a bunch of
        different queues
        '''
        t = time.time()
        self.logger.info("have next request")
        # update the redis queues every so often
        if t - self.update_time > self.update_interval:
            self.update_time = t
            self.create_queues()
            self.expire_queues()

        # update the ip address every so often
        if t - self.update_ip_time > self.ip_update_interval:
            self.update_ip_time = t
            self.update_ipaddress()
            self.report_self()

        item = self.find_item()
        if item:
            self.logger.debug(u"Found url to crawl {url}" \
                    .format(url=item['url']))
            if 'meta' in item:
                # item is a serialized request
                req = request_from_dict(item, self.spider)
            else:
                # item is a feed from outside, parse it manually
                req = self.request_from_feed(item)

            # extra check to add items to request
            if 'useragent' in req.meta and req.meta['useragent'] is not None:
                req.headers['User-Agent'] = req.meta['useragent']
            if 'cookie' in req.meta and req.meta['cookie'] is not None:
                if isinstance(req.meta['cookie'], dict):
                    req.cookies = req.meta['cookie']
                elif isinstance(req.meta['cookie'], string_types):
                    req.cookies = self.parse_cookie(req.meta['cookie'])

            return req

        return None

    def request_from_feed(self, item):
        try:
            req = Request(item['url'])
        except ValueError:
            # need absolute url
            # need better url validation here
            req = Request('http://' + item['url'])

        # defaults not in schema
        if 'curdepth' not in item:
            item['curdepth'] = 0
        if "retry_times" not in item:
            item['retry_times'] = 0

        for key in list(item.keys()):
            req.meta[key] = item[key]

        # extra check to add items to request
        if 'cookie' in item and item['cookie'] is not None:
            if isinstance(item['cookie'], dict):
                req.cookies = item['cookie']
            elif isinstance(item['cookie'], string_types):
                req.cookies = self.parse_cookie(item['cookie'])
        return req

    def parse_cookie(self, string):
        '''
        Parses a cookie string like returned in a Set-Cookie header
        @param string: The cookie string
        @return: the cookie dict
        '''
        results = re.findall('([^=]+)=([^\;]+);?\s?', string)
        my_dict = {}
        for item in results:
            my_dict[item[0]] = item[1]

        return my_dict

    def has_pending_requests(self):
        '''
        We never want to say we have pending requests
        If this returns True scrapy sometimes hangs.
        '''
        return False
class YoutubeScheduler(DistributedScheduler):
    
    def find_item(self):
        '''
        Finds an item from the throttled queues
        '''
        random.shuffle(self.queue_keys)
        count = 0
        self.logger.info("Bug")
        while count <= self.item_retries:
            for key in self.queue_keys:
                # skip if the whole domain has been blacklisted in zookeeper
                if key.split(':')[1] in self.black_domains:
                    self.logger.info("key in black domain")
                    continue
                # the throttled queue only returns an item if it is allowed
                self.logger.info(f"Item key {self.queue_dict[key][0]}")
                item = self.queue_dict[key][0].pop()
                self.logger.info(f"Have item: {item} - {self.queue_dict[key][0]}")
                if item:
                    # update timeout and return
                    self.queue_dict[key][1] = time.time()
                    return item

            count = count + 1

        return None

    def next_request(self):
        '''
        Logic to handle getting a new url request, from a bunch of
        different queues
        '''
        t = time.time()
        self.logger.debug(u"Find item {url}" \
                    .format(url=123))
        # update the redis queues every so often
        if t - self.update_time > self.update_interval:
            self.update_time = t
            self.create_queues()
            self.expire_queues()

        # update the ip address every so often
        if t - self.update_ip_time > self.ip_update_interval:
            self.update_ip_time = t
            self.update_ipaddress()
            self.report_self()

        item = self.find_item()
        if item:
            self.logger.debug(u"Found Profile youtube to crawl {url}" \
                    .format(url=item['url']))
            if 'meta' in item:
                # item is a serialized request
                req = request_from_dict(item, self.spider)
            else:
                # item is a feed from outside, parse it manually
                req = self.request_from_feed(item)
                req.meta['channel_id'] = item['url']

            # extra check to add items to request
            if 'useragent' in req.meta and req.meta['useragent'] is not None:
                req.headers['User-Agent'] = req.meta['useragent']
            if 'cookie' in req.meta and req.meta['cookie'] is not None:
                if isinstance(req.meta['cookie'], dict):
                    req.cookies = req.meta['cookie']
                elif isinstance(req.meta['cookie'], string_types):
                    req.cookies = self.parse_cookie(req.meta['cookie'])

            return req

        return None
    def request_from_feed(self, item):
        try:
            self.logger.info(f"Start crawling from feed")
            api = "https://www.youtube.com/youtubei/v1/browse?key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
            payload = self._payload(item['url'])
            headers = self._headers()
            req = Request(url=api, callback=self.spider.parse, body=payload, headers=headers, method="POST")
        except ValueError:
            # need absolute url
            # need better url validation here
            req = None

        # defaults not in schema
        if 'curdepth' not in item:
            item['curdepth'] = 0
        if "retry_times" not in item:
            item['retry_times'] = 0

        for key in list(item.keys()):
            req.meta[key] = item[key]

        # extra check to add items to request
        if 'cookie' in item and item['cookie'] is not None:
            if isinstance(item['cookie'], dict):
                req.cookies = item['cookie']
            elif isinstance(item['cookie'], string_types):
                req.cookies = self.parse_cookie(item['cookie'])
        return req
    def _payload(self, channel_id):
        payload = json.dumps({
                "context": {
                    "client": {
                        "hl": "en",
                        "gl": "VN",
                        "remoteHost": "2405:4800:5287:7e:6cce:7d5:43f2:c8b1",
                        "deviceMake": "",
                        "deviceModel": "",
                        "visitorData": "CgtESkM4UHk5a3JuQSjGp_mLBg%3D%3D",
                        "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36,gzip(gfe)",
                        "clientName": "WEB",
                        "clientVersion": "2.20211029.00.00",
                        "osName": "Windows",
                        "osVersion": "10.0",
                        "originalUrl": f"https://www.youtube.com/channel/{channel_id}/videos",
                        "platform": "DESKTOP",
                        "clientFormFactor": "UNKNOWN_FORM_FACTOR",
                        "configInfo": {
                            "appInstallData": "CMan-YsGEJLVrQUQ39atBRDMv_0SELfLrQUQsNStBRDku_0SEPTNrQUQ2L6tBRCR-PwS"
                        },
                        "browserName": "Chrome",
                        "browserVersion": "95.0.4638.69",
                        "screenWidthPoints": 1333,
                        "screenHeightPoints": 904,
                        "screenPixelDensity": 1,
                        "screenDensityFloat": 1,
                        "utcOffsetMinutes": 420,
                        "userInterfaceTheme": "USER_INTERFACE_THEME_LIGHT",
                        "mainAppWebInfo": {
                            "graftUrl": f"/channel/{channel_id}/videos",
                            "webDisplayMode": "WEB_DISPLAY_MODE_BROWSER",
                            "isWebNativeShareAvailable": True
                        },
                        "timeZone": "Asia/Saigon"
                    },
                    "user": {
                        "lockedSafetyMode": False
                    },
                    "request": {
                        "useSsl": True,
                        "internalExperimentFlags": [],
                        "consistencyTokenJars": []
                    },
                    "clickTracking": {
                        "clickTrackingParams": "CCEQ8JMBGAYiEwjJnevKo_TzAhXsRfUFHUp2DSI="
                    },
                    "adSignalsInfo": {
                        "params": [
                            {
                                "key": "dt",
                                "value": "1635668933852"
                            },
                            {
                                "key": "flash",
                                "value": "0"
                            },
                            {
                                "key": "frm",
                                "value": "0"
                            },
                            {
                                "key": "u_tz",
                                "value": "420"
                            },
                            {
                                "key": "u_his",
                                "value": "17"
                            },
                            {
                                "key": "u_h",
                                "value": "1080"
                            },
                            {
                                "key": "u_w",
                                "value": "1920"
                            },
                            {
                                "key": "u_ah",
                                "value": "1032"
                            },
                            {
                                "key": "u_aw",
                                "value": "1920"
                            },
                            {
                                "key": "u_cd",
                                "value": "24"
                            },
                            {
                                "key": "bc",
                                "value": "31"
                            },
                            {
                                "key": "bih",
                                "value": "904"
                            },
                            {
                                "key": "biw",
                                "value": "1317"
                            },
                            {
                                "key": "brdim",
                                "value": "0,0,0,0,1920,0,1920,1032,1333,904"
                            },
                            {
                                "key": "vis",
                                "value": "1"
                            },
                            {
                                "key": "wgl",
                                "value": "true"
                            },
                            {
                                "key": "ca_type",
                                "value": "image"
                            }
                        ]
                    }
                },
                "browseId": f"{channel_id}",
                "params": "EgZ2aWRlb3M%3D"
            })
        return payload
    def _headers(self):
        headers = {
                'Content-Type': 'application/json'
            }
        return headers