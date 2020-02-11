#from flask import Flask
from flask import request
from flask_api import FlaskAPI, exceptions
import pandas as pd
import os
import nagisa
import mojimoji

api = FlaskAPI(__name__)
api.debug = True

info = pd.read_csv(os.path.join("scrape", "infos.txt"), dtype=str, header=None, sep=',')
prefs = list(info.groupby(2).groups.keys())


@api.route('/', methods=['GET'])
def find():
    addr = request.args['address']
    token = nagisa.tagging(addr)

    (_pref, _city, num) = get_pref_city(token.words)
    rest_words = token.words[num + 1:]

    pref_city = info[(info[2] == _pref) & (info[3] == _city)]
    if len(pref_city) != 1:
        # 該当の都道府県-市区がない場合
        raise exceptions.NotFound("指定の都道府県・市区が見つかりません")

    cnt = len(rest_words)
    if cnt == 0:
        # パラメータ情報が少なすぎ
        raise exceptions.APIException("パラメータ情報が少なすぎです")
        
    pref_id = pref_city[0].values[0]
    city_id = pref_city[1].values[0]
    pc_path = os.path.join("scrape", "output", "{}_{}.csv.gz".format(pref_id, city_id))
    if not os.path.exists(pc_path):
        raise exception.NotFound()

    # データ読込＆解析＆抽出
    # pref, city, town, town_num, words, num1, num2 ,lati, lngi
    dat = pd.read_csv(pc_path, compression='gzip', header=None)
    dat['latitude'] = dat[8].astype(float)
    dat['longitude'] = dat[7].astype(float)

    # 町名の判定        
    num = 0
    _w = rest_words[num]
    while num < cnt and len(dat[dat[2].str.contains(_w)]) > 0:
        if num >= (cnt - 1) or len(dat[dat[2].str.contains(_w + rest_words[num + 1])]) == 0:
            break
        _w = _w + rest_words[num + 1]
        num += 1

    target = dat[dat[2] == _w]
    if len(rest_words[num+1:]) > 0:
        # 町番号・番地・号の判定
        num_infos = "".join(rest_words[num+1:])
        num_infos = mojimoji.zen_to_han(num_infos)
        num_infos = num_infos.replace("丁目", "-")
        num_infos = num_infos.replace("番地", "-")
        num_infos = num_infos.replace("号", "")
        num_infos = num_infos.split('-')
        while not num_infos[-1]:
            del num_infos[-1]
        num_infos = [int(num) for num in num_infos]

        if len(num_infos) == 3:
            target = target[(target[3] == num_infos[0]) & (target[5] == num_infos[1]) & (target[6] == num_infos[2])]
        elif len(num_infos) == 2:
            _target = target[(target[3] == num_infos[0]) & (target[5] == num_infos[1])]
            if len(_target) == 0:
                _target = target[(target[5] == num_infos[0]) & (target[6] == num_infos[1])]
            target = _target
        elif len(num_infos) == 1:
            _target = target[(target[3] == num_infos[0])]
            if len(_target) == 0:
                _target = target[(target[5] == num_infos[0])]
            if len(_target) == 0:
                _target = target[(target[6] == num_infos[0])]
            target = _target                        
    else:
        # 番地・号がない場合
        num_infos = None

    if len(target) == 1:
        latitude = target['latitude'].values[0]
        longitude = target['longitude'].values[0]
    else:
        # 複数の値がある場合
        # 緯度・経度の中間点
        latitude = target['latitude'].mean()
        longitude = target['longitude'].mean()
        
    return {
        "pref": _pref,
        "city": _city,
        "town": _w,
        "infos": num_infos,
        "latitude": latitude,
        "longitude": longitude,
    }


def get_pref_city(words):
    cnt = len(words)
    num = 0
    _pref = None
    _city = None
    while num < cnt:
        w = words[num]
        # 都道府県判定
        if _pref is None:
            if len(info[info[2] == w]) > 0:
                _pref = w
            elif len(info[info[2].str.contains(w)]) > 0:
                if num < cnt - 1:
                    _w = words[num + 1]
                    if len(info[info[2] == w + _w]) > 0:
                        _pref = w + _w
                        num += 1
            if _pref is None:
                # ここの処理に入っても何も解決しなければおかしい
                raise exceptions.APIException("都道府県の指定が不正です")
            continue

        # 市区判定
        elif len(info[info[3] == w]) > 0:
            _city = w
        elif len(info[info[3].str.contains(w)]) > 0:
            _w = w
            while num < (cnt - 1) and len(info[info[3].str.contains(_w + words[num + 1])]) > 0:
                num += 1
                _w = _w + words[num]
            _city = _w

        if _city is not None:
            break;

        num += 1
    return (_pref, _city, num)
    

if __name__ == "__main__":
    api.run()
