import os
import pandas as pd
import numpy as np
import nagisa
import re
from kanjize import kanji2int
import difflib
import mojimoji
from datetime import datetime
from pymongo import MongoClient


def now_time():
    return datetime.now()


class GeoEngineMongo():
    def coding(self, address):
        print(now_time(), "111")
        token = nagisa.tagging(address)
        print(now_time(), "222")
        (_pref, _city, _pid) = self.get_pref_city(token.words)
        print(now_time(), "333")

        ## 誤解析を防ぐために都道府県・市町村をぬいてから再度トークンを分割する
        ptn = re.compile(r"^{}\s*{}(.*)$".format(_pref, _city))
        m = ptn.search(address)
        if m is None:
            raise Exception("ERROR", "文言の解析エラー1")
        _rest_words = m.groups()[0]
        rest_words = nagisa.tagging(_rest_words).words
        cnt = len(rest_words)
        if cnt == 0:
            # パラメータ情報が少なすぎ
            raise Exception("ERROR", "パラメータ情報が少なすぎです")
        print(now_time(), "444", _pref, _city)

        # 町名の判定
        num = 0
        if rest_words[num]:
            # 町名を抽出
            (_temp, _town, num) = self.select_town(_pid, _city, rest_words)                
            if not _temp or _temp.count() == 0:
                # 念のため"字"で検索
                (_temp, _town, num) = self.select_town(_pid, _city, rest_words, "字")
                if not _temp or _temp.count() == 0:
                    raise Exception('ERROR', '指定の町名が見つかりません')

        else:
            # 町名以下がない
            raise Exception('ERROR', '町名の指定がありません')

        if not _town or len(rest_words[num:]) == 0:
            raise Exception('ERROR', '指定のパラメータでは抽出できません')

        ## 半角数字にして分割、リスト化する
        print(now_time(), "666", rest_words, num)
        num_infos = GeoEngineMongo.parse_num_infos(rest_words[num:])
        
        print(now_time(), "777")
        # もう少し絞り込む
        _hit = _town
        q = None
        is_aza = False
        for _info in num_infos:
            _q = self.find_like(_pid, _city, _town, "{} {}".format(_hit, _info))
            if _q.count() == 0:
                if is_aza:
                    # 字を考慮済みなら終了
                    break
                
                # "字"を考慮
                if re.match('^字.*', _info):
                    _info = _info.replace('字', '')
                    _q = self.find_like(_pid, _city, _town, "{} {}".format(_hit, _info))
                elif re.match('\d+', _info) is None:
                    _info = "字{}".format(_info)
                    _q = self.find_like(_pid, _city, _town, "{} {}".format(_hit, _info))
                is_aza = True
                if _q.count() == 0:
                    break

            _hit += " " + _info
            q = _q

        if q is None:
            # 町名以下がない
            raise Exception('ERROR', '指定の丁が見つかりません')

        if q.count() > 1:
            # 2桁以上の数字での誤検知を除外
            # 例）「2 4 4」-> "2 40 4" で誤検知
            _q = self.find(_pid, _city, _town, _hit)
            if _q.count() != 0:
                q = _q
            
        print(now_time(), "888", _hit, q.count())

        # 最後は合致率を計算して最大値のものを抽出
        _infos = "{} {}".format(_town, " ".join(num_infos))
        ret = {}
        max_ratio = None
        for _q in q:
#            print(_q)
            ratio = difflib.SequenceMatcher(None, _q['town_infos'], _infos).ratio()
            if not max_ratio or max_ratio < ratio:
                max_ratio = ratio
            if ratio not in ret:
                ret[ratio] = []
            ret[ratio].append((_q['lat'], _q['lng']))

        latitude = np.mean([float(row[0]) for row in ret[max_ratio]])
        longitude = np.mean([float(row[1]) for row in ret[max_ratio]])        

        print(now_time(), "999")
        
        return {
            "pref": _pref,
            "city": _city,
            "town": _town,
            "infos": num_infos,
            "latitude": latitude,
            "longitude": longitude,
            "search_info": {
                'hit_words': _infos,
                'ratio': max_ratio,
            }
        }
                
    def get_pref_city(self, words):
        _pref = ""
        _city = ""
        _q_temp = None
        cnt = len(words)
        num = 0

        if cnt == 0:
            raise Exception("ERROR", "値が指定されていません")

        # 都道府県判定
        _w = words[num]
        while True :
            _filter = {'pref': {'$regex': '^{}'.format(_w)}}
            _q = self._mongo_find('infos', _filter)
            if _q.count() == 0 or num >= (cnt - 1):
                break
            num += 1
            _pref = _w
            _w = _w + words[num]
        if not _pref:
            raise Exception("ERROR", "都道府県の指定が不正です")

        # 市区判定
        _filter = {'$and': [ {'pref': _pref} ]}
        _w = words[num]
        while True:
            _filter['$and'].append({'city': {'$regex': '^{}'.format(_w)}})
            _q = self._mongo_find('infos', _filter)
            if _q.count() == 0 or num >= (cnt - 1):
                break
            num += 1
            _city = _w
            _w = _w + words[num]

        if not _city:
            raise Exception("ERROR", "市町村区の指定が不正です")

        # 決定
        _filter = {'$and': [ {'pref': _pref, 'city': _city} ]}
        _q = self._mongo_find('infos', _filter)
        if _q.count() != 1:
            raise Exception("ERROR", "該当する市町村区が複数存在します")

        return (_pref, _city, _q[0]['pid'])

    def _db(self):
        client = MongoClient(host='localhost')
        return client['geo_japan']

    def select_town(self, pref_id, city, rest_words, prefix=""):
        _len = len(rest_words)
        _temp = None
        _town = prefix
        num = 0
        while True or num < (_len - 1):
            q = self.find_like(pref_id, city, "{}{}".format(_town, rest_words[num]))
            if q.count() == 0:
                break
            _town = "{}{}".format(_town, rest_words[num])
            _temp = q
            num += 1
        return (_temp, _town, num)

    def find_like(self, pref_id, city, town_name, town_info=None):
        _filter = {
            '$and': [ {'city': city} ]
        }
        if town_info is None:
            _filter['$and'].append(
                {'town_name': {'$regex': '^{}'.format(town_name)}}
            )
        else:
            _filter['$and'].append({'town_name': town_name})
            _filter['$and'].append(
                {'town_infos': {'$regex': '^{}'.format(town_info)}}
            )
        return self._mongo_find_pref(pref_id, _filter)

    def find(self, pref_id, city, town_name, town_info=None):
        _filter = {
            '$and': [ {'city': city} ]
        }
        if town_info is None:
            _filter['$and'].append(
                {'town_name': {'$regex': '^{}$'.format(town_name)}}
            )
        else:
            _filter['$and'].append({'town_name': town_name})
            _filter['$and'].append(
                {'town_infos': {'$regex': '^{}$'.format(town_info)}}
            )
        return self._mongo_find_pref(pref_id, _filter)

    def _mongo_find_pref(self, pref_id, _filter):
        collection = "p{:02}".format(int(pref_id))
        return self._mongo_find(collection, _filter)

    def _mongo_find(self, collection, _filter):
        db = self._db()
        return db[collection].find(filter=_filter)

    @staticmethod
    def parse_num_infos(words):
        # 町番号・番地・号の判定
        ## 半角数字にして分割、リスト化する
        i = 0
        cnt = len(words)
        while i < cnt:
            _w = words[i]
            _w = mojimoji.zen_to_han(_w)
            _w = _w.replace("丁目", "-")
            _w = _w.replace("番地", "-")
            _w = _w.replace("番", "-")
            _w = _w.replace("の", "-")
            _w = _w.replace("号", "")
            _w = _w.replace("ー", "-")  # 様々なハイフン１
            _w = _w.replace("−", "-")   # 様々なハイフン２
            _w = _w.replace("ｰ", "-")   # 様々なハイフン３
            _w = _w.replace(" ", "-")
            words[i] = _w
            i += 1

        num_infos = []
        temp = ""
        num = 0
        for _w in words:
            if _w == '-':
                if temp:
                    num_infos.append(temp)
                    temp = ""
                    num += 1
                continue
            if num >= 2 and re.match("\d+", _w) is None:
                # ３つ目以上の数字判定中（号の判定）に文字が出たら
                # ビル名やマンション名なので無視する
                num_infos.append(temp)
                temp = ""
                break
            if num == 0 and re.match("\d+", _w) and temp:
                # いきなり字が入る場合の対応
                num_infos.append(temp)
                temp = ""
            temp = temp + _w
        if temp and re.match("\d+", temp):
            num_infos.append(temp)

        # 末尾が空白/Noneのものは削除
        while not num_infos[-1]:
            del num_infos[-1]

        # 漢数字を変換
        i = 0
        while i < len(num_infos):
            _info = num_infos[i]
            if re.match("[一二三四五六七八九十]+", _info):
                num_infos[i] = str(kanji2int(_info))
            i += 1
        return num_infos
