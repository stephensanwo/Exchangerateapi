from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
import os
import pandas as pd
import json
from datetime import datetime
import dateutil.parser

app = Flask(__name__)

app.config["MONGO_DBNAME"] = "Nigeria_fx_data"

# Todo change the password of the atlas cluster to the general password before commit

app.config["MONGO_URI"] = "mongodb+srv://stephensanwo:stephensanwo@stephencluster-ifq4j.azure.mongodb.net/Nigeria_fx_data"

mongo = PyMongo(app)

environment = "dev"


# @route   GET /aboki_fx rates
# @desc    Get rates for all AbokiFx
# @access  Public

@app.route('/abokifx_rates', methods=['GET'])
def get_all_abokifx_rates():
    result = []
    for q in mongo.db.abokifx_rates.find():
        result.append({
            "date": q['Date'],
            "usd_buy": q['USD_Buy'],
            "usd_sell": q['USD_Sell'],
            "gbp_buy": q['GBP_Buy'],
            "gbp_sell": q['GBP_Sell'],
            "eur_buy": q['EUR_Buy'],
            "eur_sell": q['EUR_Sell']
        })
    return jsonify({'result': result})


# @route   GET /fx_mallam rates
# @desc    Get rates for all Fx Mallam
# @access  Public

@app.route('/fxmallam_rates', methods=['GET'])
def get_all_fxmallam_rates():
    result = []
    for q in mongo.db.fxmallam_rates.find():
        result.append({
            "date": q['Date'],
            "usd_buy": q['USD_Buy'],
            "usd_sell": q['USD_Sell'],
            "gbp_buy": q['GBP_Buy'],
            "gbp_sell": q['GBP_Sell'],
            "eur_buy": q['EUR_Buy'],
            "eur_sell": q['EUR_Sell']
        })
    return jsonify({'result': result})


# @route   GET /cbn rates
# @desc    Get rates for CBN
# @access  Public

@app.route('/cbn_rates', methods=['GET'])
def get_all_cbn_rates():
    result = []
    for q in mongo.db.cbn_rates.find():
        result.append({
            "date": q['Rate Date'].split("T")[0],
            "currency": q['Currency'],
            "buying_rate": q['Buying Rate'],
            "central_rate": q['Central Rate'],
            "selling_rate": q['Selling Rate']

        })
    return jsonify({'result': result})


if environment == "dev":
    if __name__ == '__main__':
        app.run(debug=True)
else:
    if __name__ == '__main__':
        # Bind to PORT if defined, otherwise default to 5000.
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port)
