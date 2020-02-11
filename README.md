# geo_japan
国土交通省の公開データをもとにGEOコーディングを行うAPIを生成するプログラムです

* 国土交通省 国土地理院 https://www.gsi.go.jp/kihonjohochousa/jukyo_jusho.html

## 環境
python 3.7.5
ダウンロードしたデータを保持するのに1.5GBほど要します

```
pip install -r requiements.txt

cd scrape
scrapy runspider scrape/spider/giaj.py
# しばらく時間がかかります

cd ..
python api.py  # WEBサーバー起動
```
ブラウザで"http://localhost:5000?addres=東京都世田谷区玉川一丁目14番1号"などと指定するとGEOコーディングした結果が帰ります

