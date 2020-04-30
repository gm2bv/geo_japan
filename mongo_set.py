import os
import json
import re
from pymongo import MongoClient
import pandas as pd


def main(*args, **kwargs):
    dat_dir = os.path.join("home_affairs", "output")
    dat_dir2 = os.path.join("scrape", "output")
    for f in os.listdir(dat_dir):
        m = re.match("(\d+)_\d{4}\.csv\.gz", f)
        if m is None:
            continue
        pref_id = m.groups()[0]
        print(pref_id)
        db = _db()
        db["p{}".format(pref_id)].drop()
        
        _path = os.path.join(dat_dir, f)
        _insert_collection(_path, "p{}".format(pref_id))

        # 観測データも取り込み
        for _f in os.listdir(dat_dir2):
            m = re.match("(\d+)_\d{5}\.csv\.gz", _f)
            if m is None:
                continue
            _pref_id = "{:02}".format(int(m.groups()[0]))
            if pref_id != _pref_id:
                continue
            _path = os.path.join(dat_dir2, _f)
            _insert_collection(_path, "p{}".format(_pref_id))


def _db():
    client = MongoClient('localhost')
    db = client['geo_japan']
    return client['geo_japan']

def _get_town_name(row):
    return row['town_infos'].split(' ')[0]

def _insert_collection(path, collection_name):
    dat = pd.read_csv(path, compression='gzip', dtype=str, header=None)
    dat.columns = [
        'pref',
        'city',
        'town_infos',
        'lng',
        'lat',
    ]
    if len(dat) == 0:
        return
    dat['town_name'] = dat.apply(_get_town_name, axis=1)
#    records = json.loads(dat.T.to_json()).values()

    db = _db()
    collection = db[collection_name]
#    collection.insert_many(records)

    _size = 1000000
    _len = len(dat)
    _temp = 0
    while _temp < _len:
        __size = (_temp + _size) - 1
        _dat = dat.loc[_temp:__size]
        collection.insert_many(_dat.T.to_dict().values())
        _temp += _size
    del db


if __name__ == "__main__":
    main()
