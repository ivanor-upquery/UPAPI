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

## id_client  = ""
## fonte_host = ""
## fonte_user = ""
## fonte_pass = ""
## fonte_serv = ""
## fonte_port = ""
## fonte_ftp  = ""
## dsn        = ""
  

# -------------------------------------------------------------------------------------------
#  função que importa arquivo enviado por FTP
# -------------------------------------------------------------------------------------------
def ftp_client(step_id, file):
    
    try: 
        con0 = cx_Oracle.connect(user=fonte_user,password=fonte_pass,dsn=dsn,encoding="UTF-8")
        cur0 = con0.cursor()
        # cur0.execute ('begin etf.ftp_etl_fila_new(:1,:2); end;', [step_id, file])
        cur0.close()
        con0.close()
        logger.info('FTP - Criado fila arquivo: ' + fonte_ftp+'/'+file)

    except Exception as e:
        erros='FTP - Erro cliente ['+id_client+']: '+str(e)[0:3500]
        logger.error(erros)

# -------------------------------------------------------------------------------------------
#  função que importa arquivo enviado por FTP
# -------------------------------------------------------------------------------------------
def ftp_client(cfg_cliente):

    try: 
        con0 = cx_Oracle.connect(user=fonte_user,password=fonte_pass,dsn=dsn,encoding="UTF-8")
        cur0 = con0.cursor()
        cur0.execute("select step.step_id, con.conteudo, step.comando from etl_conexoes con, etl_step step where step.id_conexao = con.id_conexao and con.cd_parametro = 'DB' and step.tipo_comando = 'INTEGRADOR_FTP'") 
        steps = cur0.fetchall()

        for step in steps:
            try: 
                ws_step_id  = step[0] 
                ws_arquivo  = str(step[2]).split('|')[0]   # Nome do arquivo 
                for file in fnmatch.filter(os.listdir(fonte_ftp),ws_arquivo):
                    cnt=pd.read_sql_query("select count(*) as CNT from ETL_FILA where status='A' and run_id=:1 ",con=con0,params=[ws_step_id])
                    if cnt['CNT'].values[0] == 0:
                        #cur1.callproc('etf.ftp_etl_fila_new',[ws_step_id, file])
                        logger.info('FTP - Criado fila arquivo: ' + fonte_ftp+'/'+file)

            except Exception as e:
                erros='FTP - Erro cliente/acao ['+id_client+'/'+ws_step_id+']: '+str(e)[0:3500]
                logger.error(erros)

        cur0.close()
        con0.close()

    except Exception as e:
        erros='FTP - Erro cliente ['+id_client+']: '+str(e)[0:3500]
        logger.error(erros)


# -------------------------------------------------------------------------------------------
#  função Operação 
# -------------------------------------------------------------------------------------------
def exec_client(cfg_cliente):
    try: 
        with engine.connect() as con0:
            data_cnt=pd.read_sql_query("select count(*) as cnt from ETL_FILA where status='A'",con=con0)
            count_exec=data_cnt['cnt'].values[0]
            con0.close
        if  count_exec == 0:
            return
    except SQLAlchemyError as err:
        error = str(err.__cause__)
        logger.error('Conn: '+str(section)+ '- '+error)
        return


# >>>>
# >>>> Execução Principal <<<<
# >>>>

config = ConfigParser.ConfigParser()
config.readfp(open(r'/opt/oracle/upapi/upquery_ftp.ini'))
log_file = config['DEFAULT'].get('logfile','/opt/oracle/upapi/upquery_ftp.log')

logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger()
chk = datetime.today()
h_inicio = chk.strftime('%d/%m/%Y %H:%M:%S')

logger.info('Serviço Iniciado [UPQUERY_FTP]')

ws_parado=0
while True:

    count_exec = 0
    for section in dict(config):
        time.sleep(3)
        if  section not in ['DEFAULT']:
              
            id_client = section
            fonte_host = config[section].get('host','localhost')
            fonte_user = config[section].get('username','')
            fonte_pass = config[section].get('password','')
            fonte_serv = config[section].get('servicename','')
            fonte_port = config[section].get('port','1521')
            fonte_ftp  = config[section].get('ftp_path','N/A')
            dsn        = cx_Oracle.makedsn(fonte_host,port=fonte_port,service_name=fonte_serv)
            engine     = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn))


            try:
                with engine.connect() as con0:
                     data_cnt=pd.read_sql_query("select count(*) as cnt from ETL_FILA where status='A'",con=con0)
                     count_exec=data_cnt['cnt'].values[0]
                con0.close
                if  count_exec != 0 and len(threading.enumerate()) < 11 and section not in threading.enumerate():
                    exec_thr = threading.Thread(target=exec_client, name=section, args=(section,))
                    exec_thr.setDaemon(True)
                    exec_thr.start()
            except SQLAlchemyError as err:
                error = str(err.__cause__)
                logger.error('Conn: '+str(section)+ '- '+error)

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

