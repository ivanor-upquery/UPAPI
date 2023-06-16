import datetime
import datetime as dt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
import re
import shutil
import os
import time
import cx_Oracle
import time
import logging
import sys

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from time import gmtime, strftime
import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import csv
import pandas as pd

def f_download(cd_local, x_mes_ref, x_data_i, x_data_f, driver,colunas,engine):
 try:
    arquivo_csv = "/opt/oracle/upapi/relatorio_dados_leitura.csv"
    if os.path.isfile(arquivo_csv):
       os.remove(arquivo_csv)

#    pressione = driver.find_element_by_name("mesReferencia")
#    driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, x_mes_ref)
    print('++++++++')

    time.sleep(5) 
    pressione = driver.find_element_by_name("dataInicio")
    driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, x_data_i)
    pressione = driver.find_element_by_name("dataFim")
    driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, x_data_f)

    driver.find_element_by_name("searchLocalidadeId").send_keys(cd_local)

    pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr[3]/td/form/table[3]/tbody/tr/td[2]/button')
    driver.execute_script("arguments[0].click();", pressione)
    print('...........................')
    try:
      alert = driver.switch_to_alert()
      alert.accept()
    except:
      saida=0

    time.sleep(20)

    driver.execute_script("arguments[0].click();", pressione)

    time.sleep(800)

    print('......>>>')
    try:
      alert = driver.switch_to_alert()
      alert.accept()
    except:
      saida=0
    print('----------------')
#    except:
#    while not os.path.isfile(arquivo_csv):
#          time.sleep(1)
    print('OK Pass 01.')
    if  os.path.isfile(arquivo_csv):
        dados = pd.read_csv(arquivo_csv, sep=";", names=colunas, encoding = "ISO-8859-1", low_memory=False, skiprows=1)
        dados.drop(dados.columns[58], axis=1)

        dados.columns = colunas
        dados.columns = dados.columns.str.strip().str.lower()
        object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
        dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}

        print('Linhas...: '+str(len(dados)))
        with engine.connect() as con0:
             dados.to_sql(name='vm_etl_transfer_teste', con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)

#    driver.get_screenshot_as_file('/tmp/teste2.png')
 except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    erros='DOWNLOAD Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
    logger.error(erros)

def run_mes(p_ano_mes):
    try:

       fonte_host = 'cloud.upquery.com'
       fonte_user = 'dwu'
       fonte_pass = 'S0R0BYbS2Fmd'
       fonte_serv = 'fernandes'
       fonte_port = 1521

       dsn = cx_Oracle.makedsn(fonte_host,port=fonte_port,service_name=fonte_serv)
       engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn))

       with engine.connect() as con0:
            temp_col = pd.read_sql_query('select column_name from all_tab_columns where table_name='+chr(39)+'VM_ETL_MEDICAO'+chr(39)+' order by column_id',con=con0)
            temp_col['column_name'] = temp_col['column_name'].astype(str)
            colunas=[]
            for col in temp_col['column_name'].values.tolist():
                colunas.append(col.lower())

       option = Options()
       option.add_argument('--headless')
       option.add_argument("--no-sandbox");
       option.add_argument('--disable-dev-shm-usage') 
       option.add_argument('--single-process')
       option.add_argument('--remote-debugging-port=9222')
       option.add_argument('--start-maximized')
       option.add_argument("--disable-setuid-sandbox")
       option.add_argument("--extension-load-timeout=60000")
       option.binary_location = r"/opt/google/chrome/google-chrome"

       prefs={'download.default_directory': "/opt/oracle/upapi"}

       option.add_experimental_option('prefs', {
           "download.default_directory": r"/opt/oracle/upapi",
           "download.prompt_for_download": False,
           "download.directory_upgrade": True,
           "plugins.always_open_pdf_externally": True
       })
       option.headless = True #Esconder o processo False/True
       driver = webdriver.Chrome("/usr/bin/chromedriver", options = option)
       driver.set_page_load_timeout(1500)
       time.sleep(5)
       driver.get("http://emobile.celesc.com.br:8080/MobileExpert/Login.do?lang=pt")
       time.sleep(25)
       usuario = "edemar.r"
       senha = "161005"
       time.sleep(2)
       print('OK....Senha')
       driver.find_element_by_class_name("lbl_campo_maiusculo").send_keys(usuario)
       driver.find_element_by_class_name("lbl_campo").send_keys(senha)
       time.sleep(2)
       pressione = driver.find_element_by_class_name("lgn_btn")
       driver.execute_script("arguments[0].click();", pressione)
       print('Wait......' )

       time.sleep(5)
       pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[1]/table/tbody/tr[16]/td/table/tbody/tr[10]/td/a')
       driver.execute_script("arguments[0].click();", pressione)

       time.sleep(5)

       driver.find_element_by_name("searchConcessionariaId").send_keys(Keys.ARROW_DOWN+Keys.SPACE)
       driver.find_element_by_name("searchEmpreiteiraId").send_keys(Keys.ARROW_DOWN+Keys.SPACE)

       select_box = driver.find_element_by_name("searchLocalidadeId")
       options = [x for x in select_box.find_elements_by_tag_name("option")]

       lista=['Todas']

       with engine.connect() as con0:
            temp_col = pd.read_sql_query('select column_name from all_tab_columns where table_name='+chr(39)+'VM_ETL_MEDICAO'+chr(39)+' order by column_id',con=con0)
            temp_col['column_name'] = temp_col['column_name'].astype(str)
            colunas=[]
            for col in temp_col['column_name'].values.tolist():
                colunas.append(col.lower())

       ano = p_ano_mes[0:4]
       mes = p_ano_mes[-2:]
       print(ano)
       print(mes)
       date = dt.date(int(ano), int(mes), 1)
       p_mes_ref = date.strftime('%m/%Y')
       p_data_i  = date.replace(day = 1).strftime('%d/%m/%Y')
       p_data_f  = date.replace(day = calendar.monthrange(date.year, date.month)[1]).strftime('%d/%m/%Y')

       print('Atenção: '+p_mes_ref+' - '+p_data_i+' - '+p_data_f)

       for locais in lista:
           chk = datetime.today()
           h_inicio = chk.strftime('%d/%m/%Y %H:%M:%S')
           if  locais != '0':
               logger.info('1-Região: ['+locais+'] - ['+h_inicio+']')
               f_download(locais, p_mes_ref, p_data_i, p_data_f,driver,colunas,engine)
               chk = datetime.today()
               h_inicio = chk.strftime('%d/%m/%Y %H:%M:%S')
       driver.quit()

    except Exception as e:
       exc_type, exc_obj, exc_tb = sys.exc_info()
       erros='CGI Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
       logger.error(erros)
#      erros='Erro: '+str(e)[0:3500]
#      logger.error(erros)
       driver.quit()

try:

    log_file = '/opt/oracle/upapi/logs/celesc.log'

    logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
    logger = logging.getLogger()

    fonte_host = 'cloud.upquery.com'
    fonte_user = 'dwu'
    fonte_pass = 'S0R0BYbS2Fmd'
    fonte_serv = 'fernandes'
    fonte_port = 1521

    logger.info('Iniciando processo: [CELESC]')

    dsn_mes = cx_Oracle.makedsn(fonte_host,port=fonte_port,service_name=fonte_serv)
    engine_mes = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn_mes))

    with engine_mes.connect() as con_mes:
         temp_meses = pd.read_sql_query("select id_mes, anomes_run from VM_LOCK where status='WAITING' order by anomes_run",con=con_mes)

    for index, row in temp_meses.iterrows():
        print('id_mes:', row['id_mes'], 'anomes_run:', row['anomes_run'])
        with engine_mes.connect() as con_status:
          r_back = con_status.execute("update VM_LOCK set status='RUNNING' where id_mes=:1",row['id_mes'])

        run_mes(row['anomes_run'])

        with engine_mes.connect() as con_status:
          r_back = con_status.execute("update VM_LOCK set status='OK' where id_mes=:1",row['id_mes'])

except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    erros='Geral Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
    logger.error(erros)
