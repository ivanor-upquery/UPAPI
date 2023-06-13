import pyodbc
import fdb
import psycopg2
import pymysql
import pymssql
import json
import requests
import sys
import logging
import pandas as pd
import sqlalchemy as sa
import cx_Oracle
import time
import base64
import math
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from requests.structures import CaseInsensitiveDict
import threading
import datetime
from datetime import datetime, timedelta
from datetime import date
from time import gmtime, strftime
import configparser as ConfigParser
import os
import csv

dados = pd.read_csv('/opt/oracle/upapi/testes/conpagar.txt', sep='\t')

object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
dtyp = {c:sa.types.VARCHAR(dados[c].str.len().max()) for c in object_columns}

# --- Exclui registros -----------------------------------------------------------
if exec_clear is not None:
    with engine.connect() as con0:
            r_del = con0.execute(exec_clear)

# --- Monta as colunas da tabela a serem inseridas 
dados.columns = dados.columns.str.strip().str.lower().str.replace(".","_")
for index in range(len(tab_colunas)):
    if  tab_colunas[index] not in dados.columns:
        dados[tab_colunas[index]]=''
dados = dados[tab_colunas]

