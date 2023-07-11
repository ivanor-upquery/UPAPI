import pyodbc
import fdb
import psycopg2
import redshift_connector
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
import fnmatch

print('a0')

dsn_origem = cx_Oracle.makedsn('capital-rds.conexos.cloud', '1521', 'CONEXOS')
con = cx_Oracle.connect(user='CNXBI_CAPITAL',password='CZCBABSDUYSADVCAVG', dsn=dsn_origem)

exec_comando = "SELECT PRI_ESP_REFCLIENTE, FIL_COD, PRD_COD, TEC_ESP_COD, TEC_ESP_COD_SEQ, PRD_DES_NOME, INI_PCT_II, INI_PCT_IPI, INI_PCT_COFINS, INI_PCT_PIS, INI_PCT_ICMS,PRI_COD FROM VB_INVOICE_ITENS"
exec_clear   = "DELETE FROM TESTE_ETL_INVOICE_ITENS"
colunas      = ["PRI_ESP_REFCLIENTE","FIL_COD","PRD_COD","TEC_ESP_COD","TEC_ESP_COD_SEQ","PRD_DES_NOME","INI_PCT_II","INI_PCT_IPI","INI_PCT_COFINS","INI_PCT_PIS","INI_PCT_ICMS","PRI_COD"]

print('buscando - sql: ' +  + str(datetime.today()) )
dados = pd.read_sql(exec_comando, con)
print('formatando: ' +  + str(datetime.today()) )
dados = pd.DataFrame(dados)
dados.columns = colunas
dados.columns = dados.columns.str.strip().str.lower()
object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}
engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn))
print('inserindo : ' +  + str(datetime.today()) )
with engine.connect() as con0:
	if exec_clear is not None:
		r_del = con0.execute(exec_clear)
	dados.to_sql(name='TESTE_ETL_INVOICE_ITENS',con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
	
print('fim : ' +  + str(datetime.today()) )


