# geo_japan
国土交通省の公開データをもとにGEOコーディングを行うAPIを生成するプログラムです

* 国土交通省 国土地理院 https://www.gsi.go.jp/kihonjohochousa/jukyo_jusho.html
* 国土交通省 GISホームページ http://nlftp.mlit.go.jp/isj/index.html


## 環境
* python 3.7.5
* ダウンロードしたデータを保持するのに1.5GBほど要します

``` bash
$ pip install -r requiements.txt

$ cd scrape
$ scrapy runspider scrape/spider/giaj.py
  # データをスクレイピングしてローカルに適切に配置し直すのでしばらく時間がかかります

$ cd ../home_affairs
$ scrapy runspider home_affairs/spider/city_code.py
  # データをダウンロードしてローカルに適切に配置し直すのでしばらく時間がかかります

$ cd ..
$ python api.py  # WEBサーバー起動
```

ブラウザで
> http://localhost:5000?addres=東京都千代田区永田町２丁目3-1

と指定するとGEOコーディングした結果が返ります

```
HTTP 200 OK
Content-Type: application/json

{
    "pref": "東京都",
    "city": "千代田区",
    "town": "永田町",
    "infos": [
        "2",
        "3"
    ],
    "latitude": 139.743113,
    "longitude": 35.672826,
    "search_info": {
        "hit_words": [
            "永田町 2 3"
        ],
        "ratio": 1.0
    }
}
```


## 更新履歴
- 2020/03/23  総務省・国土交通省からデータをスクレイピングする箇所を修正
- 2020/03/07　検索ロジックを高一致率のものを抽出する方式に変更
- 2020/02 初版
