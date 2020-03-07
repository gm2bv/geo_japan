import os
import pandas as pd
import requests
import urllib
import zipfile
import shutil
import re
import xml.etree.ElementTree as etree
from datetime import datetime
from kanjize import kanji2int

DAT=os.path.join("infos", "R1.5.1現在の団体-表1.csv")
URL='http://nlftp.mlit.go.jp/isj/api/1.0b/index.php/app/getISJURL.xml'
PARAMS={
    'appId': 'isjapibeta1',
    'fiscalyear': "'平成30年'",
    'posLevel': 0,
}
DAT_DIR = "dats"
OUT_DIR = "output"

def extract_to_ext( dat_path):
    _paths = os.path.split(dat_path)
    ext_path = os.path.join(_paths[0], 'ext')
    if not os.path.exists(ext_path):
        os.makedirs(ext_path)

    # 解凍
    with zipfile.ZipFile(dat_path) as z:
        for z_name in z.namelist():
            z.extract(z_name, ext_path)
            __path = os.path.split(z_name)
            z_dir = __path[0]
            org_name = __path[1]
            if not org_name:
                # 解凍ディレクトリ
                continue
            dec_name = org_name.encode('cp437').decode('cp932')
            shutil.move(os.path.join(ext_path, z_dir, org_name), os.path.join(ext_path, dec_name))

    return ext_path


def split_town(dat):
    if dat != dat:
        return ""
    m = re.match("(.*[^一^二^三^四^五^六^七^八^九^十])([一二三四五六七八九十]+)丁目", dat)
    if m is None:
        return dat
    return m.groups()[0]


def split_town_num(dat):
    if dat != dat:
        return ""
    m = re.match("(.*[^一^二^三^四^五^六^七^八^九^十])([一二三四五六七八九十]+)丁目", dat)
    if m is None:
        return None
    town_num = m.groups()[1]
    return "{}".format(kanji2int(town_num)) if town_num else ""


def main(*args, **kwargs):
    info = pd.read_csv(DAT, dtype=str)
    info = info[['団体コード', '都道府県名\n（漢字）', '市区町村名\n（漢字）']]
    info['code'] = info.apply(lambda row: row['団体コード'][:5], axis=1)
    info = info[info['市区町村名\n（漢字）'].isnull()]  # 全域のレコードを抽出
    info = info[['code', '都道府県名\n（漢字）', '市区町村名\n（漢字）']].copy()
    info.columns = ['code', 'pref', 'city']
#    print(info.head(2), info.iloc[0]['code'])
    city_indices = []

    cnt = len(info)
    num = 0
    while num < cnt:
        print("=====", info.iloc[num]['pref'])
        print(datetime.now(), "APIから情報取得")
        areaCode = info.iloc[num]['code']
        _params = PARAMS
        _params['areaCode'] = areaCode
        ret = requests.get(url=URL, params=_params)

        print(datetime.now(), "ZIPファイルのパスを抽出")
        zip_url = None
        data = etree.fromstring(ret.content)
        for child in data.iter('zipFileUrl'):
            zip_url = child.text
            break

        if zip_url is None:
            print("ZIP情報が取得できず")
            next

        print(datetime.now(), "ZIPファイルをダウンロード")
        file_name = zip_url.split('/')[-1]
        dl_path = os.path.join(DAT_DIR, file_name)
        urllib.request.urlretrieve(zip_url, dl_path)

        def gen_infos(row):
            if row['town'] != row['town'] or not row['town']:
                return ""
            
            ret = [row['town']]
            if row['town_num']:
                ret.append(row['town_num'])
#            print(ret)
            if row['小字・通称名'] and row['小字・通称名'] == row['小字・通称名']:
                ret.append(str(row['小字・通称名']))
#            print(ret)
            if row['街区符号・地番'] and row['街区符号・地番'] == row['街区符号・地番']:
                ret.append(str(row['街区符号・地番']))
#            print(ret)
            return " ".join(ret)
        
        print(datetime.now(), "UNZIPしてCSVをクレンジング")
        ext_dir = extract_to_ext(dl_path)
        for _f in os.listdir(ext_dir):
            m = re.match("(\d+)_.*\.csv", _f)
            if m is None:
                continue
            pref_num = m.groups()[0]

            #CSVファイルを解析
            _df = pd.read_csv(os.path.join(ext_dir, _f), encoding='cp932', dtype=str)
            _df = _df[(_df['都道府県名'].notnull()) & (_df['市区町村名'].notnull())]
            _df['town'] = _df.apply(lambda row: split_town(row['大字・丁目名']), axis=1)
            _df['town_num'] = _df.apply(lambda row: split_town_num(row['大字・丁目名']), axis=1)
            _df['town_infos'] = _df.apply(gen_infos, axis=1)
            _df['blk_num'] = ""  # 「号」は空白で
            _cols = [
                '都道府県名',
                '市区町村名',
#                'town',
#                'town_num',
#                '小字・通称名',
#                '街区符号・地番',
#                'blk_num',
                'town_infos',
                '経度',
                '緯度'
            ]
            _gz = "{}.gz".format(_f)
            _df[_cols].to_csv(os.path.join(OUT_DIR, _gz), compression='gzip', index=False, header=None)
            print(datetime.now(), "{}を出力完了".format(_f))

            for city_index in _df.groupby(['都道府県名','市区町村名']).groups.keys():
                city_indices.append([pref_num] + list(city_index))

            break

        shutil.rmtree(ext_dir)
        num += 1

    print(datetime.now(), "情報ファイルを出力")
    with open("infos.txt", "w") as wf:
        for city_index in city_indices:
            wf.write("{}\n".format(','.join(city_index)))


if __name__ == "__main__":
    main()
