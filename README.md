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

$ cd ../tools
$ python get_isj.py
  # データをダウンロードしてローカルに適切に配置し直すのでしばらく時間がかかります

$ cd ..
$ python api.py  # WEBサーバー起動
```

ブラウザで
> http://localhost:5000?addres=東京都世田谷区玉川一丁目14番1号

と指定するとGEOコーディングした結果が返ります

## 更新履歴
- 2020/03/07　検索ロジックを高一致率のものを抽出する方式に変更
- 2020/02 初版