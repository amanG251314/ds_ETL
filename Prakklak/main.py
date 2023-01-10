from __future__ import division, print_function
# coding=utf-8
import numpy as np
from ETL_Prakklak import *
import functions_framework

@functions_framework.http
def etl_prakklak(request):
    if request.method == 'GET':
        ETL()
