import os
import pandas as pd
from pymongo import MongoClient


class DatInfo():
    def __init__(self):
        _dirs = os.path.split(__file__)
        self._dir = _dirs[0]
        self.infos = []

        path_scrape = os.path.join(self._dir, "scrape", "infos.txt")
        if os.path.exists(path_scrape):
            # pref_cd,town_cd,pref_name,town_name,file_name
            info = pd.read_csv(path_scrape, dtype=str, header=None, sep=',')
            info['pid'] = info.apply(lambda row: "{:02}".format(int(row[0])), axis=1)
            self.infos.append(info)
        else:
            raise Exception("ERROR", "can't find '{}'".format(self._dir))

        # pref_cd,pref_name,town_name
        path_home_affairs = os.path.join(self._dir, "home_affairs", "infos.txt")
        if os.path.exists(path_home_affairs):
            info2 = pd.read_csv(path_home_affairs, dtype=str, header=None, sep=',')
            self.infos.append(info2)

    def run(self):
        pc1 = list(self.infos[0].groupby(['pid', 2, 3]).groups.keys())
        pc2 = list(self.infos[1].groupby([0, 1, 2]).groups.keys())
        vals = [{"pid": row[0], "pref": row[1], "city": row[2]} for row in list(set(pc1 + pc2))]
        db = self._db()
        collection = db["infos"]
        collection.insert_many(vals)

    def _db(self):
        client = MongoClient(host='localhost')
        return client['geo_japan']
        

def main(*args, **kwargs):
    info = DatInfo()
    info.run()


if __name__ == "__main__":
    main()
