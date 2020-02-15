from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
import os
import pandas as pd
import json

app = Flask(__name__)

app.config["MONGO_DBNAME"] = "Nigeria_fx_data"

# Todo change the password of the atlas cluster to the general password before commit

app.config["MONGO_URI"] = "mongodb+srv://stephensanwo:stephensanwo@stephencluster-ifq4j.azure.mongodb.net/Nigeria_fx_data"

mongo = PyMongo(app)

environment = "prod"


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
            "date": q['Rate Date'],
            "currency": q['Currency'],
            "buying_rate": q['Buying Rate'],
            "central_rate": q['Central Rate'],
            "selling_rate": q['Selling Rate']
        })
    return jsonify({'result': result})


# @route   GET /all_usd_bdc_rates
# @desc    Get usd rates for all the available BDCs
# @access  Public

@app.route('/all_usd_bdc_rates', methods=['GET'])
def get_all_usd_bdc_rates():
    abokifx_usd = []
    for q in mongo.db.abokifx_rates.find():
        abokifx_usd.append({
            "date": q['Date'],
            "abokifx_usd_buy": q['USD_Buy'],
            "abokifx_usd_sell": q['USD_Sell']
        })

    fxmallam_usd = []
    for q in mongo.db.fxmallam_rates.find():
        fxmallam_usd.append({
            "date": q['Date'],
            "fxmallam_usd_buy": q['USD_Buy'],
            "fxmallam_usd_sell": q['USD_Sell']
        })

# Merge the mongo results together using the CBN data as base.
# Todo: refactor without using pandas

    df1 = pd.DataFrame(abokifx_usd)
    df1['compDate'] = pd.to_datetime(df1['date'])

    df2 = pd.DataFrame(fxmallam_usd)
    df2['compDate'] = pd.to_datetime(df2['date'])

    df_merged = df1.merge(df2, how="outer", on='compDate')

    df_merged.drop_duplicates('compDate', keep='first', inplace=True)

    result = df_merged[['compDate', "abokifx_usd_buy",
                        "abokifx_usd_sell", "fxmallam_usd_buy", "fxmallam_usd_sell"]].to_dict('records')

    return jsonify({'result': result})


# @route   GET /all_gbp_bdc_rates
# @desc    Get gbp rates for all the available BDCs
# @access  Public

@app.route('/all_gbp_bdc_rates', methods=['GET'])
def get_all_gbp_bdc_rates():
    abokifx_gbp = []
    for q in mongo.db.abokifx_rates.find():
        abokifx_gbp.append({
            "date": q['Date'],
            "abokifx_gbp_buy": q['GBP_Buy'],
            "abokifx_gbp_sell": q['GBP_Sell']
        })

    fxmallam_gbp = []
    for q in mongo.db.fxmallam_rates.find():
        fxmallam_gbp.append({
            "date": q['Date'],
            "fxmallam_gbp_buy": q['GBP_Buy'],
            "fxmallam_gbp_sell": q['GBP_Sell']
        })

# Merge the mongo results together using the CBN data as base.
# Todo: refactor without using pandas

    df1 = pd.DataFrame(abokifx_gbp)
    df1['compDate'] = pd.to_datetime(df1['date'])

    df2 = pd.DataFrame(fxmallam_gbp)
    df2['compDate'] = pd.to_datetime(df2['date'])

    df_merged = df1.merge(df2, how="outer", on='compDate')

    df_merged.drop_duplicates('compDate', keep='first', inplace=True)

    result = df_merged[['compDate', "abokifx_gbp_buy",
                        "abokifx_gbp_sell", "fxmallam_gbp_buy", "fxmallam_gbp_sell"]].to_dict('records')

    return jsonify({'result': result})


# @route   GET /all_eur_bdc_rates
# @desc    Get eur rates for all the available BDCs
# @access  Public

@app.route('/all_eur_bdc_rates', methods=['GET'])
def get_all_eur_rates():
    abokifx_eur = []
    for q in mongo.db.abokifx_rates.find():
        abokifx_eur.append({
            "date": q['Date'],
            "abokifx_eur_buy": q['EUR_Buy'],
            "abokifx_eur_sell": q['EUR_Sell']
        })

    fxmallam_eur = []
    for q in mongo.db.fxmallam_rates.find():
        fxmallam_eur.append({
            "date": q['Date'],
            "fxmallam_eur_buy": q['EUR_Buy'],
            "fxmallam_eur_sell": q['EUR_Sell']
        })

# Merge the mongo results together using the CBN data as base.
# Todo: refactor without using pandas

    df1 = pd.DataFrame(abokifx_eur)
    df1['compDate'] = pd.to_datetime(df1['date'])

    df2 = pd.DataFrame(fxmallam_eur)
    df2['compDate'] = pd.to_datetime(df2['date'])

    df_merged = df1.merge(df2, how="outer", on='compDate')

    df_merged.drop_duplicates('compDate', keep='first', inplace=True)

    result = df_merged[['compDate', "abokifx_eur_buy",
                        "abokifx_eur_sell", "fxmallam_eur_buy", "fxmallam_eur_sell"]].to_dict('records')

    return jsonify({'result': result})


# @route   GET /cbn_30days_pred
# @desc    Get predicted rates 30 days from a select interval
# @access  Public

@app.route('/cbn_30days_pred')
def index():
    documents = []
    for doc in mongo.db.cbn_predict_30_days.find():
        if doc not in documents:
            documents.append(doc)

    data = pd.DataFrame(documents)
    data.iloc[:, 1:]
    data1 = data.transpose()[1:]
    data1.reset_index(inplace=True)
    data1.columns = data1.iloc[0]
    data2 = data1[1:]
    data2['index'] = pd.to_datetime(
        data2['index'], format="%Y-%m-%dT%H:%M:%SZ")

    result = json.loads(data2.to_json(orient = 'records', date_unit = 's', date_format = 'iso'))

    return jsonify({'result': result})

# Check environment and run app

if environment == "dev":
    if __name__ == '__main__':
        app.run(debug=True)
else:
    if __name__ == '__main__':
        # Bind to PORT if defined, otherwise default to 5000.
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port)
