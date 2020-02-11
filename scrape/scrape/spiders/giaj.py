# -*- coding: utf-8 -*-
import re
import os
import scrapy
from datetime import datetime
from scrapy.selector import Selector
from scrape.items import ScrapeItem


class GiajSpider(scrapy.Spider):
    name = 'giaj'
    allowed_domains = ['saigai.gsi.go.jp']
    start_urls = ['https://saigai.gsi.go.jp/jusho/download/index.html']
    save_dir = "dats"  # os.path.join(settings.BASE_DIR, "dats")
    infos = {}

    def parse(self, response, **kwargs):
        for anchor in response.xpath('//a').getall():
            s = Selector(text=anchor)
            t = s.css('a::text').get()
            href = s.css('a::attr(href)').get()
            if re.match("^.*[都道府県]$", t):
                # ダウンロードページ
                yield scrapy.Request(response.urljoin(href), callback=self.parse, cb_kwargs={'pref': t, 'page': href})
            elif re.match(".*\.zip", href):
                dl_link = response.urljoin(href)
                kwargs['city'] = t
                yield scrapy.Request(dl_link, callback=self.save_zip, cb_kwargs=kwargs) 

    def save_zip(self, response, **kwargs):
        m = re.match(".*/(\d+)\.html?", kwargs['page'])
        if m is None:
            return
        pref_id = int(m.groups()[0])
        fname = response.url.split('/')[-1]
        item = ScrapeItem()
        item['fname'] = fname
        item['pref_id'] = pref_id
        item['city_id'] = fname.split('.')[0]
        item['pref_name'] = kwargs['pref']
        item['city_name'] = kwargs['city']
        item['created_at'] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        yield item
        if pref_id not in self.infos:
            self.infos[pref_id] = []
        self.infos[pref_id].append(item)

        # ファイルのダウンロード
        _dir = os.path.join(self.save_dir, str(pref_id))
        os.makedirs(_dir, exist_ok=True)
        path = os.path.join(_dir, fname)
        if not os.path.exists(path):
            with open(path, 'wb') as f:
                f.write(response.body)

    def close(self, reason):
        info_path = os.path.join("infos.txt")
        with open(info_path, "w") as wf:
            for pref_id in sorted(self.infos.keys()):
                for item in self.infos[pref_id]:
                    wf.write(
                        "{},{},{},{},{}\n".format(
                            item['pref_id'],
                            item['city_id'],
                            item['pref_name'],
                            item['city_name'],
                            item['fname'],
                            item['created_at'])
                    )
