import scrapy
import sys
import time, json

import logging
from crawling.items import RawResponseItem
from crawling.spiders.redis_spider import RedisSpider
class YoutubeSpider(RedisSpider):
    name = 'youtube'
    channel_id = ""
    def start_requests(self):
        print("start")
        return super().start_requests()
    def item_parse(self, data):
        item = data
        return item
    def countinue_item(self, token):
        api = "https://www.youtube.com/youtubei/v1/browse?key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
        
        continue_token = token

        print("Have token and countinue")
        """
        Process for loop
        """
        headers = {
            'Content-Type': 'application/json',
            'referer': f'https://www.youtube.com/channel/{self.channel_id}/videos'
        }
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
                    "originalUrl": f"https://www.youtube.com/channel/{self.channel_id}/videos",
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
                        "graftUrl": "/channel/UCv3DwFQMm3fwlPsu3k8xw2w/videos",
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
            "browseId": f"{self.channel_id}",
            "params": "EgZ2aWRlb3M%3D",
            "continuation": continue_token
        })
        return scrapy.Request(url=api, callback=self.parse, body=payload,
                             headers=headers, method="POST")
    def parse(self, response):
        data_populate = json.loads(response.body)
        continue_token = None
        item = RawResponseItem()
        # populated from response.meta
        item['appid'] = response.meta['appid']
        item['crawlid'] = response.meta['crawlid']
        item['attrs'] = response.meta['attrs']

        # populated from raw HTTP response
        item["url"] = response.request.url
        item["response_url"] = response.url
        item["status_code"] = response.status
        item["status_msg"] = "OK"
        item["response_headers"] = self.reconstruct_headers(response)
        item["request_headers"] = response.request.headers
        item["body"] = response.body
        item["encoding"] = response.encoding
        item["links"] = []
        if 'contents' in data_populate:
            if 'twoColumnBrowseResultsRenderer' in data_populate['contents'] and 'tabs' in data_populate['contents']['twoColumnBrowseResultsRenderer']:
                for tab in data_populate['contents']['twoColumnBrowseResultsRenderer']['tabs']:
                    if 'tabRenderer' in tab and 'selected' in tab['tabRenderer'] and tab['tabRenderer']['selected'] and 'content' in tab['tabRenderer'] and 'sectionListRenderer' in tab['tabRenderer']['content'] and 'contents' in tab['tabRenderer']['content']['sectionListRenderer']:
                        for content in tab['tabRenderer']['content']['sectionListRenderer']['contents']:
                            if 'itemSectionRenderer' in content and 'contents' in content['itemSectionRenderer']:
                                for contentRender in content['itemSectionRenderer']['contents']:
                                    if 'gridRenderer' in contentRender and 'items' in contentRender['gridRenderer']:
                                        item_index = 0
                                        for item_ret in contentRender['gridRenderer']['items']:
                                            if 'continuationItemRenderer' in item_ret:
                                                print("Have token")
                                                continue_token = item_ret['continuationItemRenderer']['continuationEndpoint']['continuationCommand']['token']
                                                if continue_token:
                                                    yield self.countinue_item(continue_token)
                                                else:
                                                    print("No token, shutdown")
                                            else:
                                                print("item processed")
                                                item['index'] = item_index
                                                item['value'] = item_ret
                                                yield item_ret
                                    else:
                                        logging.info("No gridRender in contentRender")
                            else:
                                logging.info("No itemSectionRender")
                    else:
                        logging.info("No tabrender")
            else:
                logging.info("No twoColumn")
        else:
            logging.info("No contents in root")
            if 'onResponseReceivedActions' in data_populate:
                for res in data_populate['onResponseReceivedActions']:
                    if 'appendContinuationItemsAction' in res and 'continuationItems' in res['appendContinuationItemsAction']:
                        item_index = 0
                        for item_ret in res['appendContinuationItemsAction']['continuationItems']:
                            if 'continuationItemRenderer' in item_ret:
                                print("Have token")
                                continue_token = item_ret['continuationItemRenderer']['continuationEndpoint']['continuationCommand']['token']
                                if continue_token:
                                    yield self.countinue_item(continue_token)
                                else:
                                    print("No token, shutdown")
                            else:
                                print("item processed")
                                item['index'] = item_index
                                item['value'] = item_ret
                                yield item_ret