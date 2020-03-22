#from flask import Flask
from flask import request
from flask_api import FlaskAPI, exceptions
from geo.engine import GeoEngine

api = FlaskAPI(__name__)
api.debug = True

geo_engine = GeoEngine()


@api.route('/', methods=['GET'])
def find():

    # 入力文字列の解析
    addr = request.args['address']
    return geo_engine.coding(addr)


if __name__ == "__main__":
    api.run()
