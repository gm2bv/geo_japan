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
from kanjize import kanji2int


class ScrapePipeline(object):
    output_dir = "output"
    
    def process_item(self, item, spider):
#        print('PIPELINE', item['pref_id'], item['fname'])

        zip_path = os.path.join(spider.save_dir, str(item['pref_id']), item['fname'])
        (ext_path, csv_path) = self.extract_csv(zip_path)
        dat = pd.read_csv(csv_path, dtype=str, encoding='cp932', header=None)

        kanji_num = ['一', '二', '三', '四', '五', '六', '七', '八', '九', '十']
        def split_town(dat):
            m = re.match("(.*[^一^二^三^四^五^六^七^八^九^十])([一二三四五六七八九十]+)丁目", dat)
            if m is None:
                return dat
            return m.groups()[0]
        
        def split_town_num(dat):
            m = re.match("(.*[^一^二^三^四^五^六^七^八^九^十])([一二三四五六七八九十]+)丁目", dat)
            if m is None:
                return None
            town_num = m.groups()[1]
            return kanji2int(town_num) if town_num else None

        dat['town'] = dat.apply(lambda row: split_town(row[1]), axis=1)
        dat['town_num'] = dat.apply(lambda row: split_town_num(row[1]), axis=1)
        dat['pref_name'] = item['pref_name']
        dat['city_name'] = item['city_name']

        city_id = item['fname'].split('.')[0]
        output_path = os.path.join(self.output_dir, "{}_{}.csv.gz".format(item['pref_id'], city_id))
        cols = [1, 2, 3, 6, 7]  # 町名,番地,号,緯度,経度
        dat[['pref_name', 'city_name', 'town', 'town_num'] + cols].to_csv(output_path, index=False, header=None, compression='gzip')
        del dat

        # 作業後のファイル・ディレクトリの削除
        os.remove(csv_path)
        for p in os.listdir(ext_path):
            os.removedirs(os.path.join(ext_path, p))

        # DLしてきたZIPは削除する（消したくないならこの行をコメントアウトする）
        os.remove(zip_path)

        return item

    def extract_csv(self, dat_path):
        """
        前提：パラメータで渡されたzipファイルにCSVファイルは一つしか含まれてない
        """
        _paths = os.path.split(dat_path)
        city_num = _paths[1].split('.')[0]
        ext_path = os.path.join(_paths[0], city_num)
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
        return (ext_path, csv_path)
