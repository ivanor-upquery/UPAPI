import fdb
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
import etl_conces   # importa arquivo concessionárias (COPEL, CELESC) 

#with open("/opt/oracle/upapi/error.txt", "w") as text_file:
#    text_file.write(str(data))

def check_service(section):
    servico = config[section].get('name','')
    

# >>>>
# >>>> Execução Principal <<<<
# >>>>

config = ConfigParser.ConfigParser()
config.readfp(open(r'/opt/oracle/upapi/upquery_mon.ini'))
log_file = config['DEFAULT'].get('logfile','/opt/oracle/upapi/upquery_mon.log')
config.readfp(open(r'/opt/oracle/upapi/upquery_mon.ini'))

logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger()

chk = datetime.today()
h_inicio = chk.strftime('%d/%m/%Y %H:%M:%S')

logger.info('Serviço Iniciado [UPQUERY_MON]')

ws_parado=0
while True:
      count_exec = 0
      for section in dict(config):
          time.sleep(3)
          if  section in ['DB']:
              fonte_host = config[section].get('host','localhost')
              fonte_port = config[section].get('port','1521')
              fonte_serv = config[section].get('servicename','')
              fonte_user = config[section].get('username','')
              fonte_pass = config[section].get('password','')

          if  section in ['SERVICE']:
              check_service(section); 
              

            ##   dsn = cx_Oracle.makedsn(fonte_host,port=fonte_port,service_name=fonte_serv)
            ##   engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn))

            ##   try:
            ##       with engine.connect() as con0:
            ##            data_cnt=pd.read_sql_query("select count(*) as cnt from ETL_FILA where status='A'",con=con0)
            ##            count_exec=data_cnt['cnt'].values[0]
            ##       con0.close
            ##       if  count_exec != 0 and len(threading.enumerate()) < 11 and section not in threading.enumerate():
            ##           exec_thr = threading.Thread(target=exec_client, name=section, args=(section,))
            ##           exec_thr.setDaemon(True)
            ##           exec_thr.start()
            ##   except SQLAlchemyError as err:
            ##       error = str(err.__cause__)
            ##       logger.error('Conn: '+str(section)+ '- '+error)

      ws_ter = 0
      ws_running = 'Rodando: '
      main_thread = threading.current_thread()

      for t in threading.enumerate():
          if t is main_thread:
             continue
          else:
             ws_ter += 1
             ws_running = ws_running + '['+t.name+'] '
      if  ws_ter > 0:
          ws_parado = 1
          logger.info('Running: '+ws_running)
      else:
          if  ws_parado == 1:
              logger.info('>>>>> Waiting <<<<<')
              ws_parado=0

