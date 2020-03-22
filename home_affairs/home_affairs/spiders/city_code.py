import scrapy
import re
import os
from scrapy.selector import Selector
from home_affairs.items import ExcelItem


class CityCodeSpider(scrapy.Spider):
    name = 'CityCode'
    allowed_domains = ['www.soumu.go.jp']
    start_urls = [
        'https://www.soumu.go.jp/denshijiti/code.html',
    ]
    save_dir = "dats"

    def parse(self, response, **kwargs):
        for blk_li in  response.xpath('//li'):
            t = blk_li.css('li::text').get()
            if t is not None:
                t = t.strip()                
            if not t:
                continue

            if re.match('^「都道府県コード及び市区町村コード」$', t):
                for excel_a in blk_li.xpath('ul/li/a'):
                    t_a = excel_a.css('a::text').get()
                    if re.match('Excelファイル', t_a):
                        excel_path = excel_a.css('a::attr(href)').get()
                        excel_url = response.urljoin(excel_path)
                        yield scrapy.Request(excel_url,  callback=self.save_excel, cb_kwargs={'page': excel_path})

    def save_excel(self, response, **kwargs):
        _dir = os.path.join(self.save_dir, kwargs['page'])
        save_file = os.path.split(_dir)[-1]
        save_path = os.path.join(self.save_dir, save_file)
        if not os.path.exists(save_path):
            with open(save_path, 'wb') as f:
                f.write(response.body)

        item = ExcelItem()
        item['dat_path'] = save_file
        yield item
