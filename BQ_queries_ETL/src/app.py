from __future__ import division, print_function
# coding=utf-8
from bq_views import *
from flask import Flask,abort
from mailer import *
import pytz
from datetime import datetime


# Define a flask app
app = Flask(__name__)
@app.route('/run', methods=['GET'])
def etl_bq_queries():
    try:
        get_bq_queries()
        ist = pytz.timezone('Asia/Kolkata')
        subject = 'Success - Daily BQ Export ETL {}'.format(datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S %Z'))
        msg = """"Hi Team, BQ Export ETL ran successfully. 
        Thank and Regards, 
        DS Team"""
        mail(subject,msg)
        return "Successfully updated data on Gsheet Youtube_Stats"
    except SystemExit as e:
        ist = pytz.timezone('Asia/Kolkata')
        subject = 'Failed - Daily BQ Export ETL {}'.format(datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S %Z'))
        msg = """"Hi Team, BQ Export ETL failed today. 
        Thank and Regards, 
        DS Team"""
        mail(subject,msg)
        abort(400)

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=8080)