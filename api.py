import sys
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
    try:
        ret = geo_engine.coding(addr)
    except Exception as e:
        print(str(e))
        raise exceptions.APIException(str(e))

    return ret

if __name__ == "__main__":
    api.run()
