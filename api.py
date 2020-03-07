#from flask import Flask
from flask import request
from flask_api import FlaskAPI, exceptions
import pandas as pd
import os
import nagisa
import mojimoji
import re
import difflib
from kanjize import kanji2int

api = FlaskAPI(__name__)
api.debug = True

# pref_cd,town_cd,pref_name,town_name,file_name
info = pd.read_csv(os.path.join("scrape", "infos.txt"), dtype=str, header=None, sep=',')
# pref_cd,pref_name,town_name
info2 = pd.read_csv(os.path.join("tools", "infos.txt"), dtype=str, header=None, sep=',')
prefs = list(info.groupby(2).groups.keys())


@api.route('/', methods=['GET'])
def find():

    # 入力文字列の解析
    addr = request.args['address']
    token = nagisa.tagging(addr)
    (_pref, _city) = get_pref_city(token.words)

    ## 誤解析を防ぐために都道府県・市町村をぬいてから再度トークンを分割する
    ptn = re.compile(r"^{}\s*{}(.*)$".format(_pref, _city))
    m = ptn.search(addr)
    if m is None:
        raise exceptions.APIException("文言の解析エラー1")
    _rest_words = m.groups()[0]
    rest_words = nagisa.tagging(_rest_words).words
    cnt = len(rest_words)
    if cnt == 0:
        # パラメータ情報が少なすぎ
        raise exceptions.APIException("パラメータ情報が少なすぎです")

    # データ読込＆解析＆抽出
    ## tools以下の情報を使う
    pref_city = info[info[2] == _pref]
    pref_id = pref_city[0].values[0]
    pc_path = os.path.join("tools", "output", "{:02}_2018.csv.gz".format(int(pref_id)))
    if not os.path.exists(pc_path):
        raise exceptions.NotFound()
    ## pref, city, town_infos,lat, lng
    ##  * town_infos: "town town_s town_num num1 num2"
#    print(pc_path)
    dat = pd.read_csv(pc_path, compression='gzip', header=None, dtype=str)
    if len(info[(info[2] == _pref) & (info[3] == _city)]) == 1:
        # 測量情報からも取得
        pref_city = info[(info[2] == _pref) & (info[3] == _city)]
        pref_id = pref_city[0].values[0]
        city_id = pref_city[1].values[0]
        pc_path2 = os.path.join("scrape", "output", "{}_{}.csv.gz".format(pref_id, city_id))
        if not os.path.exists(pc_path2):
            raise exceptions.NotFound()
        dat2 = pd.read_csv(pc_path2, compression='gzip', header=None, dtype=str)
        print(pc_path, pc_path2, dat.shape, dat2.shape)
        dat = pd.concat([dat, dat2])

    dat.columns = [
        'pref',
        'city',
        'town_infos',
        'lat',
        'lng',
    ]
    ## データのクレンジング
    dat.fillna('', inplace=True)
    dat = dat[(dat['pref']==_pref) & (dat['city']==_city)]
    dat['latitude'] = dat['lat'].astype(float)
    dat['longitude'] = dat['lng'].astype(float)

    # 町名の判定
    num = 0
    _w = rest_words[num]
    if _w:
        while num < cnt and len(dat[dat['town_infos'].str.contains(_w)]) > 0:
            if num >= (cnt - 1) or len(dat[dat['town_infos'].str.contains(_w + rest_words[num + 1])]) == 0:
                break
            _w = _w + rest_words[num + 1]
            num += 1
        _town = _w
    else:
        _town = ''
    num += 1

    target = dat[dat['town_infos'].str.contains(_town)].copy()  # 町名から抽出
    if len(target) == 0:
        raise exceptions.NotFound('町名が見つかりません')

    if len(rest_words[num:]) > 0:
        # 町番号・番地・号の判定
        ## 半角数字にして分割、リスト化する
        num_infos = parse_num_infos(rest_words[num:])

        # もう少し絞り込む
        _hit = _town
        for _info in num_infos:
            if len(target[target['town_infos'].str.contains(_hit + " " + _info)]) == 0:
                break
            _hit += " " + _info
        target = target[target['town_infos'].str.contains(_hit)].copy()

        # 最後は合致率を計算して最大値のものを抽出
        _infos = "{} {}".format(_town, " ".join(num_infos))
        target['ratio'] = target.apply(lambda row:difflib.SequenceMatcher(None, row['town_infos'], _infos).ratio(), axis=1)
        ratio = target['ratio'].max()
        target = target[(target['ratio']==ratio)]
        hit_infos = list(target['town_infos'].values)
    else:
        # 番地・号がない場合
        ratio = None
        num_infos = None
        hit_infos = None

    latitude = target['latitude'].mean()
    longitude = target['longitude'].mean()

    return {
        "pref": _pref,
        "city": _city,
        "town": _town,
        "infos": num_infos,
        "latitude": latitude,
        "longitude": longitude,
        "search_info": {
            'hit_words': hit_infos,
            'ratio': ratio,
        }
    }


def get_pref_city(words):
    _pref = None
    _city = None
    cnt = len(words)
    num = 0

    # 都道府県判定
    w = words[num]
    if len(info[info[2] == w]) > 0:
        _pref = w
    elif len(info[info[2].str.contains(w)]) > 0:
        while num < cnt - 1 and len(info[info[2] == w + words[num + 1]]) > 0:
            w = w + words[num + 1]
            num += 1
        _pref = w
    else:
        raise exception.APIException("都道府県の指定が不正です")

    # 市区判定
    num += 1
    w = words[num]
    if len(info[info[3] == w]) > 0:
        _city = w
    elif len(info[info[3].str.contains(w)]) > 0:
        while num < (cnt - 1) and len(info[info[3].str.contains(w + words[num + 1])]) > 0:
            w = w + words[num + 1]
            num += 1
        _city = w
    else:
        # info2から探す
        _info2 = info2[info2[1] == _pref]
        if len(_info2[_info2[2] == w]) > 0:
            _city = w
        elif len(_info2[_info2[2].str.contains(w)]) > 0:
            while num < (cnt - 1) and len(_info2[_info2[2].str.contains(w + words[num + 1])]) > 0:
                w = w + words[num + 1]
                num += 1
            _city = w
        else:
            raise exceptions.APIException("市町村区の指定が不正です")

    return (_pref, _city)


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
        print(words[i], _w)
        words[i] = _w
        i += 1

    num_infos = []
    temp = ""
    num = 0
    for _w in words:
        if _w == '-':
            if temp:
                print(_w, temp)
                num_infos.append(temp)
                temp = ""
                num += 1
            continue
        if num >= 2 and re.match("\d+", _w) is None:
            # ３つ目以上の数字判定中（号の判定）に文字が出たら
            # ビル名やマンション名なので無視する
            num_infos.append(temp)
            break
        temp = temp + _w
    if temp and re.match("\d+", temp):
        num_infos.append(temp)
    print(num_infos)
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
    

if __name__ == "__main__":
    api.run()
