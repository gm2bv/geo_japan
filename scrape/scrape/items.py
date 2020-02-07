# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScrapeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    fname = scrapy.Field()
    pref_id = scrapy.Field()
    pref_name = scrapy.Field()
    city_name = scrapy.Field()
    created_at = scrapy.Field()
