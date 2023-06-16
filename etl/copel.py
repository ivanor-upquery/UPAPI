import datetime
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

def f_download(cd_local, x_data_i, x_data_f):
    arquivo_csv = "/opt/oracle/upapi/relatorio_dados_controle_empreiteiras.csv"
    if os.path.isfile(arquivo_csv):
       os.remove(arquivo_csv)

#    pressione = driver.find_element_by_name("mesReferencia")
#    driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, x_mes_ref)

    pressione = driver.find_element_by_name("dataInicio")
    driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, x_data_i)
    pressione = driver.find_element_by_name("dataFim")
    driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, x_data_f)
    driver.find_element_by_name("searchLocalidadeId").send_keys(cd_local)
    pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr[3]/td/form/table[3]/tbody/tr/td[2]/button')
    driver.execute_script("arguments[0].click();", pressione)
    time.sleep(24)

    try:
       alert = driver.switch_to_alert()
       alert.accept()
    except:
       while not os.path.isfile(arquivo_csv):
             print(arquivo_csv+' ......')
             time.sleep(1)

    logger.info("Download........ [OK] ")

    print('Importando...')
    if  os.path.isfile(arquivo_csv):
        colunas=['ds_concessionaria','ds_empreiteira','ds_equipe','ds_usuario','mesano_referente_livro','dt_leitura','hr_leitura','cd_uc','cd_cidade','ds_cidade','tp_local','cd_etapa','cd_livro','status_releitura','cd_equipamento','ds_especificacao','ds_mensagem','ds_mensagem_aux','ds_obs','ds_foto','cd_fat_campo','cd_impressao_comunicado','ds_entrega_fatura']

        try:
            dados = pd.read_csv(arquivo_csv, sep=";", encoding = "ISO-8859-1", low_memory=False, skiprows=2, error_bad_lines=False)
            dados.drop(dados.columns[23], axis=1, inplace=True)
            dados.columns = colunas
            dados.columns = dados.columns.str.strip().str.lower()
            object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
            dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}
            with engine.connect() as con0:
                 dados.to_sql(name='vm_etl_transfer_teste', con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            erros='Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
            print(erros)
            logger.error(erros)

#    driver.get_screenshot_as_file('/tmp/teste2.png')

try:

    p_data_i = sys.argv[1]
    p_data_f = sys.argv[2]

    log_file = '/opt/oracle/upapi/logs/copel.log'

    logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
    logger = logging.getLogger()

    fonte_host = 'cloud.upquery.com'
    fonte_user = 'dwu'
    fonte_pass = 'S0R0BYbS2Fmd'
    fonte_serv = 'fernandes'
    fonte_port = 1521

    logger.info('Serviço Iniciado [UPQUERY_ETL]')

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
    option.add_argument("--enable-features=NetworkServiceInProcess")
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
    time.sleep(5)
    driver.get("https://www.copel.com/lis/")
    time.sleep(10)
    usuario = "T314836"
    senha = "314836"
    driver.find_element_by_class_name("lbl_campo_maiusculo").send_keys(usuario)
    driver.find_element_by_class_name("lbl_campo").send_keys(senha)
    time.sleep(2)
    pressione = driver.find_element_by_class_name("lgn_btn")
    driver.execute_script("arguments[0].click();", pressione)

    time.sleep(5)
    #pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[1]/table/tbody/tr[12]/td/table/tbody/tr[4]/td/a')
    pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[1]/table/tbody/tr[11]/td/table/tbody/tr[4]/td/a')
    driver.execute_script("arguments[0].click();", pressione)

    time.sleep(5)

    driver.find_element_by_name("searchConcessionariaId").send_keys(Keys.ARROW_DOWN+Keys.SPACE)
    driver.find_element_by_name("searchEmpreiteiraId").send_keys(Keys.ARROW_DOWN+Keys.SPACE)

    lista=['Todas']

    with engine.connect() as con0:
         temp_col = pd.read_sql_query('select column_name from all_tab_columns where table_name='+chr(39)+'VM_ETL_MEDICAO'+chr(39)+' order by column_id',con=con0)
         temp_col['column_name'] = temp_col['column_name'].astype(str)
         colunas=[]
         for col in temp_col['column_name'].values.tolist():
             colunas.append(col.lower())

    print('Atenção: '+p_data_i+' - '+p_data_f)

    for locais in lista:
        if  locais != '0':
            logger.info('Região: ['+locais+']')
            f_download(locais, p_data_i, p_data_f)

    print('OK COPEL')

except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    erros='Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
    print(erros)
    logger.error(erros)

driver.quit()
