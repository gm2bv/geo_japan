# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os
import zipfile
import re
import shutil
import pandas as pd


class ScrapePipeline(object):
    def process_item(self, item, spider):
        print('PIPELINE', item['pref_id'], item['fname'])
        return item



    def extract_csv(self, dat_path):
        """
        前提：パラメータで渡されたzipファイルにCSVファイルは一つしか含まれてない
        """
        _paths = os.path.split(dat_path)
        ext_path = os.path.join(_paths[0], 'ext')
        os.makedirs(ext_path, exist_ok=True)

        # 解凍
        with zipfile.ZipFile(dat_path) as z:
            for z_name in z.namelist():
                if re.match(".*\.csv", z_name) is None:
                    continue

                z.extract(z_name, ext_path)
                __path = os.path.split(z_name)
                z_dir = __path[0]
                org_name = __path[1]
                if not org_name:
                    # 解凍ディレクトリ
                    continue
                dec_name = org_name.encode('cp437').decode('cp932')
                csv_path = os.path.join(ext_path, dec_name)
                shutil.move(os.path.join(ext_path, z_dir, org_name), csv_path)
        return csv_path
