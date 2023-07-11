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

# >>>>
# >>>> Formato Parametro <<<<
# >>>>


def get_par(dados,parametro,defval):
    try:
        retorno = dados.loc[dados['cd_parametro'] == parametro]['conteudo'].values[0]
    except:
        if defval is None:
           retorno = ''
        else:
           retorno = defval
    return retorno

# >>>>
# >>>> função Operação <<<<
# >>>>

def exec_client(cfg_cliente):
  try:
     id_client = cfg_cliente
     fonte_host = config[section].get('host','localhost')
     fonte_user = config[section].get('username','')
     fonte_pass = config[section].get('password','')
     fonte_serv = config[section].get('servicename','')
     fonte_port = config[section].get('port','1521')
     dsn = cx_Oracle.makedsn(fonte_host,port=fonte_port,service_name=fonte_serv)
     engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn))

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

     logger.info('Início Execução ['+id_client+']')

     with engine.connect() as con0:
          data_exec    = pd.read_sql_query("select * from (select ID_UNIQ, TBL_DESTINO, COMANDO, COMANDO_LIMPAR, ID_CONEXAO from ETL_FILA where status='A' order by dt_criacao) where rownum=1",con=con0)
          if  len(data_exec) < 1:
              return
          exec_comando = data_exec['comando'].values[0]
          exec_clear   = data_exec['comando_limpar'].values[0]
          id_uniq      = data_exec['id_uniq'].values[0]
          tbl_destino  = data_exec['tbl_destino'].values[0].lower()
          parbuf       = []
          parbuf.append(data_exec['id_conexao'].values[0])
          id_conexao   = data_exec['id_conexao'].values[0]

     chk = datetime.today()
     h_inicio = chk.strftime('%d/%m/%Y %H:%M:%S')

     logger.info('Inicio: '+id_uniq)

     with engine.connect() as con0:
          data_con      = pd.read_sql_query('select * from ETL_CONEXOES where id_conexao = :1',con=con0,params=parbuf)
          cnx_driver    = get_par(data_con,'DRIVER','')
          cnx_host      = get_par(data_con,'HOST','')
          cnx_port      = int(get_par(data_con,'PORTA',0))
          cnx_dbase     = get_par(data_con,'DATABASE','')
          cnx_headers   = get_par(data_con,'HEADERS','')
          cnx_user      = get_par(data_con,'USUARIO','')
          cnx_pass      = get_par(data_con,'SENHA','')
          cnx_charset   = get_par(data_con,'CHARSET','')
          cnx_url       = get_par(data_con,'URL','')
          cnx_content   = get_par(data_con,'CONTENT','')
          cnx_db        = get_par(data_con,'DB','')
          cnx_service   = get_par(data_con,'SERVICE','')
          cnx_api_key   = get_par(data_con,'API_KEY','')
          cnx_secret    = get_par(data_con,'SECRET','')
          cnx_loc_file  = get_par(data_con,'LOC_FILE','')

     if  cnx_db not in drivers:
         logger.error('Driver nao configurado: '+cnx_db)
         return

     with engine.connect() as con0:
          r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='R' where id_uniq=:1",id_uniq)

     logger.info('Inicio: '+id_uniq+' Conn/Database: '+id_conexao+'/'+cnx_dbase)

     with engine.connect() as con0:
          temp_col = pd.read_sql_query('select column_name from all_tab_columns where table_name='+chr(39)+tbl_destino.upper()+chr(39)+' order by column_id',con=con0)
          temp_col['column_name'] = temp_col['column_name'].astype(str)
          colunas=[]
          tab_colunas=[]
          for col in temp_col['column_name'].values.tolist():
              colunas.append(col)
              tab_colunas.append(col.lower())

     engine.fast_executemany = True

     try:

        get_type = 'db_con'

        if  cnx_db == "ODBC":
            con = pyodbc.connect('DRIVER={'+cnx_driver+'};HOST='+cnx_host+';PORT='+str(cnx_port)+';DB='+cnx_dbase+';UID='+cnx_user+';PWD='+cnx_pass)
        if  cnx_db == "FIREBIRD":
            con = fdb.connect(host=cnx_host, port=cnx_port, database=cnx_dbase, user=cnx_user, password=cnx_pass, charset=cnx_charset)
        if  cnx_db == "POSTGRESQL":
            con = psycopg2.connect(host=cnx_host, port=cnx_port, dbname=cnx_dbase, user=cnx_user, password=cnx_pass)
        if  cnx_db == "MYSQL":
            con = pymysql.connect(host=cnx_host, port=cnx_port, user=cnx_user, passwd=cnx_pass, db=cnx_dbase)
        if  cnx_db == "MSSQL":
            con = pymssql.connect(host=cnx_host, port=cnx_port, user=cnx_user, password=cnx_pass, database=cnx_dbase)
        if  cnx_db == "ORACLE":
            dsn_origem = cx_Oracle.makedsn(cnx_host, cnx_port, cnx_service)
            con = cx_Oracle.connect(user=cnx_user,password=cnx_pass, dsn=dsn_origem)
        if  cnx_db in ('API_CGI','API_OMIE','HCM_SENIOR','API_LINHA','API_NEXTI','EXCEL','TXT','API_TRELLO'):
            get_type = cnx_db.lower()

        if  get_type == "db_con": 
            dados = pd.read_sql(exec_comando, con)
            dados = pd.DataFrame(dados)
            dados.columns = colunas
            dados.columns = dados.columns.str.strip().str.lower()
            object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
            dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}
            engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn))
     
            with engine.connect() as con0:
                 if exec_clear is not None:
                    r_del = con0.execute(exec_clear)
                 dados.to_sql(name=tbl_destino,con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
                 r_back = con0.execute("update ETL_FILA set dt_final=sysdate, status='F' where id_uniq=:1",id_uniq)

        if  get_type == "excel" or get_type == "txt":

            # --- Pega os parametros do comando -----------------------------------------------------------
            if exec_comando is None:
                raise Exception('Comando esta em Nulo, verifique o comando e as variaveis do comando.')
            params = exec_comando.split('|')
            try:
                ws_arquivo   = (params[0]).strip()
                ws_separador = (params[1] if get_type == "txt" else "")
            except Exception as e:
                raise Exception('Numero incorreto de parâmetros no comando ['+ str(e)[0:3500] + ']')

            if ws_arquivo.find("'") != -1:
                raise Exception('Nome do arquivo nao pode conter ASPAS.')

            try:
                # --- Lê arquivo -----------------------------------------------------------
                engine.fast_executemany = True

                if get_type == "excel":
                    if ws_arquivo[-3:].upper() == 'XLS':
                        dados = pd.read_excel(cnx_loc_file+ws_arquivo, engine='xlrd')
                    else:     
                        dados = pd.read_excel(cnx_loc_file+ws_arquivo, engine='openpyxl')
                else:
                    dados = pd.read_csv(cnx_loc_file+ws_arquivo, sep=ws_separador)

                object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
                dtyp = {c:sa.types.VARCHAR(dados[c].str.len().max()) for c in object_columns}

                # --- Exclui registros -----------------------------------------------------------
                if exec_clear is not None:
                    with engine.connect() as con0:
                         r_del = con0.execute(exec_clear)

                # --- Insere registros e atualiza status da fila -----------------------------------------------------------
                with engine.connect() as con0:
                     dados.to_sql(name=tbl_destino,con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
                     r_back = con0.execute("update ETL_FILA set dt_final=sysdate, status='F' where id_uniq=:1",id_uniq)
                
            except Exception as e:
                erros='Erro '+ get_type.upper()+ ': '+str(e)[0:3500]
                logger.error(erros)
                with engine.connect() as con0:
                     r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='E', erros=:erros where id_uniq=:id_uniq",id_uniq=id_uniq,erros=erros)

        if  get_type == "api_cgi": 
            try:
                comando = exec_comando.split(',')
                par_content=json.loads(cnx_content)
                par_content['params'][1]['valor']=comando[1]
                par_content['params'][2]['valor']='{\"'+comando[2]+'\": \"'+comando[3]+'\" }'

                with engine.connect() as con0:
                     r_del  = con0.execute(exec_clear)

                     data   = requests.put(url=cnx_url, json=par_content).json()

                     chk = datetime.today()
                     h_file = chk.strftime('%d%m%Y%H%M%S')

                     if  comando[0] == "full_line":
                         dados = pd.DataFrame(data[comando[4]])
                     else:
                          dados = pd.json_normalize(data[comando[4]][comando[5]])
                          dados = pd.DataFrame(data[comando[4]][comando[5]]).explode(comando[6]).reset_index(drop=True)
                          dados = pd.concat([dados,pd.json_normalize(dados[comando[6]].dropna(),errors='ignore').add_prefix("it_")],axis=1)
                          del dados[comando[6]]

                     colunas = list(dados.columns)
                     dados.columns = colunas
                     dados.columns = dados.columns.str.strip().str.lower()

                     object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
                     dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}
                     dados.to_sql(name=tbl_destino,con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
                     r_back = con0.execute("update ETL_FILA set dt_final=sysdate, status='F' where id_uniq=:1",id_uniq)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                erros='CGI Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
                logger.error(erros)
                with engine.connect() as con0:
                     r_del = con0.execute(exec_clear)
                     r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='E', erros=:erros where id_uniq=:id_uniq",id_uniq=id_uniq,erros=erros)

        if  get_type == "api_linha":
            try:
                comando = exec_comando.split('|')[0].split(',')
                headers = json.loads(cnx_headers)
                cnx_url = cnx_url+comando[1]
                ws_pagina = 1
                ws_total  = 100

                if  comando[0] != 'get_content':
                    with engine.connect() as con0:
                         r_del = con0.execute(exec_clear)

                while (ws_pagina<=ws_total):
                     time.sleep(3)
                     data   = requests.get(url=cnx_url+'?page='+str(ws_pagina), headers=headers).json()

                     if  len(comando) > 2:
                         data_check = data[comando[2]]
                     else:
                         data_check = data

                     if  ('current_page' in data and 'last_page' in data):
                         ws_total  = data['last_page']
                         ws_pagina = data['current_page']+1
                     else:
                         ws_pagina +=1
                         ws_total = ws_pagina + 1

                     if  data_check != []:
                         if  comando[0] == 'get_content':
                             par_get_content = []
                             par_get_content.append(str(data)[0:3950])
                             par_get_content.append(id_uniq)
                             with engine.connect() as con0:
                                  r_back = con0.execute("update ETL_FILA set erros = :1 where id_uniq=:2", par_get_content)
                             ws_pagina = 0
                             ws_total  = 0
                         else:
                             if  len(comando) > 2:
                                 data=data[comando[2]]

                             if  comando[0] == 'occur_line':
                                 try:
                                    p_meta = exec_comando.split('|')[1].split(',')
                                 except:
                                    p_meta = []
                                 try:
                                    p_record_path = exec_comando.split('|')[2].split(',')
                                 except:
                                     p_record_path = []
                                 if  p_meta != [] and p_record_path != []:
                                     dados = pd.json_normalize(data, record_path=p_record_path, meta=p_meta)
                                 else:
                                     dados = pd.json_normalize(data,  meta=p_meta)
                                 dados = dados.rename(columns=lambda x: x.replace('.','_'))
                             else:
                                 dados = pd.json_normalize(data)

                             dados.columns = dados.columns.str.strip().str.lower()
                             object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
                             dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}
                             colunas = dados.columns
                             dados = dados.rename(columns=lambda x: x.split('.')[-1])
                             for index in range(len(tab_colunas)):
                                 if  tab_colunas[index] not in dados.columns:
                                     dados[tab_colunas[index]]=''
                             dados = dados[tab_colunas]
                             with engine.connect() as con0:
                                  dados.to_sql(name=tbl_destino,con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)

                         print('OK Pagina: '+str(ws_pagina))
                     else:
                         print('final GET')
                         ws_total = ws_pagina-1
                with engine.connect() as con0:
                     r_back = con0.execute("update ETL_FILA set dt_final=sysdate, status='F' where id_uniq=:1",id_uniq)

            except Exception as e:
                #det_err  = traceback.format_exc()
                erros='Erro API_LINHA: '+str(e)[0:3500]
                logger.error(erros)
                #logger.error(det_err)
                with engine.connect() as con0:
                     r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='E', erros=:erros where id_uniq=:id_uniq",id_uniq=id_uniq,erros=erros)

        if  get_type == "api_omie":
            try:
                comando = exec_comando.split('|')[0].split(',')
                par_content = json.loads(cnx_content)
                par_content['call'] = comando[2]
                par_content['app_key'] = cnx_api_key
                par_content['app_secret'] = cnx_secret
                params = exec_comando.split('|')[1].split(',')
                for i in range(1,len(params),2):
                    par_content['param'][0][params[i-1]]=params[i]

                cnx_url = cnx_url+comando[1]
                ws_pagina = 1
                ws_total = 2

                with engine.connect() as con0:
                     r_del = con0.execute(exec_clear)
                while (ws_pagina<ws_total):
                     data   = requests.post(url=cnx_url, json=par_content).json()
                     #with open('/opt/oracle/upapi/data.json', 'w') as f:
                     #     json.dump(data, f)
                     ws_pagina = data['pagina']
                     ws_total  = data['total_de_paginas']
                     par_content['param'][0]['pagina'] = ws_pagina + 1
                     ws_local = 0
                     if  comando[0] == 'occur_line':
                         subnivel = comando[5].split('/')
                         dados1=pd.json_normalize(data[comando[3]], record_path = [comando[4]], meta=subnivel, errors='ignore')
                         dados2=[]
                         dados2.append(dados1)
                         for it_sub in subnivel:
                             dados2.append(pd.json_normalize(dados1[it_sub], erros='ignore'))
                         dados = pd.concat(dados2,axis=1)
                     else:
                         dados = pd.json_normalize(data[comando[3]])
                     dados.columns = dados.columns.str.strip().str.lower()
                     object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
                     dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}
                     colunas = dados.columns
                     dados = dados.rename(columns=lambda x: x.split('.')[-1])
                     for index in range(len(tab_colunas)):
                         if  tab_colunas[index] not in dados.columns:
                             dados[tab_colunas[index]]=''
                     dados = dados[tab_colunas]

                     with engine.connect() as con0:
                          dados.to_sql(name=tbl_destino,con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
                          r_back = con0.execute("update ETL_FILA set dt_final=sysdate, status='F' where id_uniq=:1",id_uniq) 

            except Exception as e:
                erros='['+str(ws_local)+'] Erro OMIE: '+str(e)[0:3500]
                logger.error(erros)
                with engine.connect() as con0:
                     r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='E', erros=:erros where id_uniq=:id_uniq",id_uniq=id_uniq,erros=erros)

        if  get_type == "hcm_senior":
            try:
                url = cnx_service
                headers = CaseInsensitiveDict()
                headers["client_id"] = cnx_api_key
                headers["tenant"] = cnx_content
                headers["Content-Type"] = "application/json"
                par_data  = '{ "username": "'+cnx_user+'", "password": "'+cnx_pass+'" }'
                resp = requests.post(url, headers=headers, data=par_data)

                w_token = json.loads(resp.text)['jsonToken']
                w_token = json.loads(w_token)
                token = w_token['access_token']

                headers = CaseInsensitiveDict()
                headers["client_id"] = cnx_api_key
                headers["Authorization"] = "Bearer "+token
                headers["accept"] = "application/json"

                ws_size = 1000
                ws_pagina = 1
                ws_total = 2

                while (ws_pagina<=ws_total):
                     params = exec_comando.split('|')
                     url = cnx_url+params[2]+'?size='+str(ws_size)+'&offset='+str(ws_pagina-1)+'&'+params[3]
                     data = requests.get(url, headers=headers).json()
                     #with open("/opt/oracle/upapi/error.txt", "w") as text_file:
                     #     text_file.write(str(data))
                     if  ws_pagina == 1:
                         ws_total = data['totalPages']

                     if params[0] == "full_line" or params[0] == "full_content":
                        dados = pd.json_normalize(data['contents'])
                     else:    
                        dados = pd.json_normalize(data['contents'])
                        dados = pd.DataFrame(data['contents']).explode(params[1]).reset_index(drop=True)
                        dados = pd.concat([dados,pd.json_normalize(dados[params[1]].dropna(),errors='ignore').add_prefix("it_")],axis=1)

                     ws_pagina += 1
                     dados.columns = dados.columns.str.strip().str.lower().str.replace(".","_")
                     object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
                     dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}
                     colunas = dados.columns
                     dados = dados.rename(columns=lambda x: x.split('.')[-1])
                     for index in range(len(tab_colunas)):
                         if  tab_colunas[index] not in dados.columns:
                             dados[tab_colunas[index]]=''
                     dados = dados[tab_colunas]

                     con = cx_Oracle.connect(user=fonte_user,password= fonte_pass, dsn=dsn,encoding="UTF-8")
                     cur = con.cursor()
                     id_clear=[]
                     for lista in dados['id']:
                         id_clear.append([lista])
                     logger.info('Delete: '+str(len(id_clear)))
                     cur.executemany(exec_clear, id_clear)
                     con.commit()
                     con.close()
                     with engine.connect() as con0:
                          dados.to_sql(name=tbl_destino,con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
                          r_back = con0.execute("update ETL_FILA set dt_final=sysdate, status='F' where id_uniq=:1",id_uniq)

            except Exception as e:
                erros='Erro HCM_SENIOR: '+str(e)[0:3500]
                logger.error(erros)
                with engine.connect() as con0:
                     r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='E', erros=:erros where id_uniq=:id_uniq",id_uniq=id_uniq,erros=erros)

        if  get_type == "api_nexti":
            try:
                # obtem o token que deve ser utilizado na api de get dos dados 
                url = cnx_service
                headers = CaseInsensitiveDict()
                token = (cnx_api_key+":"+cnx_secret).encode('ascii')
                token = str(base64.b64encode(token)).replace("b'","").replace("'","") 
                headers["Authorization"] = "Basic " + token # "Z3J1cG9zZXR1cDo2MTFlMDNkNDNkYTNjMjNhMTQzMTAyNzQwNWM5OTNlMjJiOTY0M2M0"
                url = url+"?grant_type=client_credentials&client_id="+cnx_api_key+"&client_secret="+cnx_secret
                resp = requests.post(url, headers=headers)
                if resp.status_code != 200:
                    raise Exception('Erro obtendo Token de acesso aos dados')
                else:
                    token = json.loads(resp.text)['access_token']
                
                headers = CaseInsensitiveDict()
                headers["Authorization"] = "bearer " + token
                ws_size = 1000
                ws_pagina = 1
                ws_total = 1

                logger.info('Deletando ['+ tbl_destino + ']...')
                with engine.connect() as con0:
                     r_del = con0.execute(exec_clear)

                while (ws_pagina<=ws_total):
                     params = exec_comando.split('|')
                     if (params[2]).find('?') == -1:
                        url = cnx_url+params[2]+'?size='+str(ws_size)+'&page='+str(ws_pagina-1)
                     else: 
                        url = cnx_url+params[2]+'&size='+str(ws_size)+'&page='+str(ws_pagina-1)

                     resp = requests.get(url, headers=headers)
                     data = resp.json()
                     #with open("/opt/oracle/upapi/error"+str(ws_pagina)+".txt", "w") as text_file:
                     #     text_file.write(str(data))


                     # --------------- Valida conteudo retornado -------------------------
                     if 'status' in data and 'error' in data:
                       raise Exception('Erro retornado pela API '+ '[pagina='+str(ws_pagina)+'/'+str(ws_total)+']: ' + str(data['status']) + str(data['error']))  
                     if 'status_code' in resp:
                        if resp.status_code != 200:
                            raise Exception('Erro retornado pela API '+ '[pagina='+str(ws_pagina)+'/'+str(ws_total)+']: ' + str(data.status_code))
                     if 'message' in data:
                        raise Exception(data['message'])
                     if params[0] == "full_content" and ws_pagina == 1 and 'totalElements' not in data:
                        raise Exception('Registro do tipo CONTENT não retornou o elemento [totalElements]')
                     
                     # ---------------Pega o total de paginas retornadas ---------------------
                     if ws_pagina == 1:
                        ws_total = 1 
                        if 'totalElements' in data:
                             ws_total = math.ceil(data['totalElements'] / ws_size) 

                     # --------------- Pega o conteudo --------------------
                     if params[0] == "full_content": 
                        dados = pd.json_normalize(data['content'])
                     elif params[0] == "full_line":
                        dados = pd.json_normalize(data)
                     else:    
                        dados = pd.json_normalize(data['content'])
                        dados = pd.DataFrame(data['content']).explode(params[1]).reset_index(drop=True)
                        dados = pd.concat([dados,pd.json_normalize(dados[params[1]].dropna(),errors='ignore').add_prefix("it_")],axis=1)

                     # --------------- Pega as colunas e os dados para o Insert -------------------------
                     dados.columns = dados.columns.str.strip().str.lower().str.replace(".","_")
                     object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
                     dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}
                     colunas = dados.columns
                     dados = dados.rename(columns=lambda x: x.split('.')[-1])

                     for index in range(len(tab_colunas)):
                         if  tab_colunas[index] not in dados.columns:
                             dados[tab_colunas[index]]=''
                     dados = dados[tab_colunas]

                     logger.info('Inserindo [' + tbl_destino + ']... (' + str(ws_pagina) + '/' + str(ws_total) + ')')
                     with engine.connect() as con0:
                          dados.to_sql(name=tbl_destino,con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
                          cnt = pd.read_sql_query("select count(*) as cnt from ETL_FILA where status='C' and id_uniq=:1",con=con0,params=[id_uniq])
                          if cnt['cnt'].values[0] > 0:
                              raise Exception('Insert cancelado pelo usuário [STATUS=C] [pagina='+str(ws_pagina)+'/'+str(ws_total)+']')
                    
                     ws_pagina += 1

                # --------------- Atualiza o status da Fila -------------------------
                with engine.connect() as con0:
                    r_back = con0.execute("update ETL_FILA set dt_final=sysdate, status='F', erros=null where id_uniq=:1",id_uniq)

            except Exception as e:
                erros='Erro ' + get_type.upper() + ' : '+str(e)[0:3500]
                logger.error(erros)
                with engine.connect() as con0:
                     r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='E', erros=:erros where id_uniq=:id_uniq",id_uniq=id_uniq,erros=erros)

        if  get_type == "api_trello":    # sem paginação # parametros 
            ws_passo = ''
            try:
                ws_passo = 'PARAMETROS'
                if exec_comando is None:
                    raise Exception('Comando esta em Nulo, verifique o comando e as variaveis do comando.')
                if len(tab_colunas) == 0:
                    raise Exception('Erro pegando colunas da tabela de destino, verifique se o nome da tabela foi informada corretamente ['+tbl_destino.upper()+'].') 

                # --- Pega os parametros do comando -----------------------------------------------------------
                params = exec_comando.split('|')
                try:
                    ws_tp_reg     = (params[0]).strip().lower()
                    ws_occur      = (params[1]).strip()
                    ws_url_api    = (params[2]).strip()
                    ws_url_params = (params[3]).strip().split(';')
                except Exception as e:
                    raise Exception('Numero incorreto de parâmetros no comando ['+ str(e)[0:3500] + ']')

                if ws_url_api.find(':1') != -1 and len(ws_url_params) == 0:
                    raise Exception('Necessario informar os parametros da URL no comando.')
                if ws_tp_reg == 'occur_line' and len(ws_occur) == 0:
                    raise Exception('Necessario informar o registro OCCUR_LINE para esse tipo de comando.')

                if len(ws_url_params) == 0:  # --- se o array de parametros do comando estiver zerado, adiciona 1 elemento para que entre no loop abaixo 
                    ws_url_params[0] = ""

                # --- Deleta registros da tabela
                ws_passo = 'DELETE'
                if exec_clear is not None:
                    logger.info('Deletando ['+ tbl_destino + ']...')
                    with engine.connect() as con0:
                        r_del = con0.execute(exec_clear)

                ws_count = 0
                ws_total = len(ws_url_params)

                ws_passo = 'FOR'
                for url_param in ws_url_params: 
                    ws_count = ws_count + 1
                    # logger.info('Inserindo [' + tbl_destino + ']... (' + str(ws_count) + '/' + str(ws_total) + ')')
                    url = cnx_url + ws_url_api.replace(":1",url_param) +'?key='+cnx_api_key+'&token='+cnx_secret
                    
                    ws_passo = 'REQUEST'
                    resp = requests.get(url)
                    data = resp.json()
                    with open("/opt/oracle/upapi/error.txt", "w") as text_file:
                        text_file.write(str(data))

                    # --------------- Valida conteudo retornado -------------------------
                    if resp.status_code != 200:
                       raise Exception('Erro retornado pela API :' + str(data.status_code) + 'URL_API:'+ url)
 
                    # --------------- Pega as colunas e os dados para o Insert -------------------------
                    ws_passo = 'NORMALIZE'
                    if ws_tp_reg == "full_content": 
                        dados = pd.json_normalize(data['content'])
                    elif ws_tp_reg == "full_line":
                        dados = pd.json_normalize(data)
                    else:    
                        if "content" in data:
                            dados = pd.json_normalize(data['content'])
                            dados = pd.DataFrame(data['content']).explode(ws_occur).reset_index(drop=True)
                        else: 
                            dados = pd.json_normalize(data)
                            dados = pd.DataFrame(data).explode(ws_occur).reset_index(drop=True)
                        
                        dados = pd.concat([dados,pd.json_normalize(dados[ws_occur].dropna(),errors='ignore').add_prefix("it_")],axis=1)

                    dados.columns = dados.columns.str.strip().str.lower().str.replace(".","_")
                    object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
                    dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}
                    dados = dados.rename(columns=lambda x: x.split('.')[-1])

                    for index in range(len(tab_colunas)):
                        if  tab_colunas[index] not in dados.columns:
                            dados[tab_colunas[index]]=''
                    dados = dados[tab_colunas]

                    ws_passo = 'INSERT'
                    with engine.connect() as con0:
                         dados.to_sql(name=tbl_destino,con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
                         cnt = pd.read_sql_query("select count(*) as cnt from ETL_FILA where status='C' and id_uniq=:1",con=con0,params=[id_uniq])
                         if cnt['cnt'].values[0] > 0:
                             raise Exception('Insert cancelado pelo usuário [STATUS=C] [pagina='+str(ws_count)+'/'+str(ws_total)+']')
                   
                # --------------- Atualiza o status da Fila -------------------------
                logger.info(tbl_destino + ' - Registros inseridos [' + str(ws_count) + ']')
                ws_passo = 'STATUS'
                with engine.connect() as con0:
                    r_back = con0.execute("update ETL_FILA set dt_final=sysdate, status='F', erros=null where id_uniq=:1",id_uniq)

            except Exception as e:
                erros='Erro ' + get_type.upper()+'['+ ws_passo +']: ' +str(e)[0:3500]
                logger.error(erros)
                with engine.connect() as con0:
                     r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='E', erros=:erros where id_uniq=:id_uniq",id_uniq=id_uniq,erros=erros)

     except Exception as e:
        erros=str(e)[0:3000]
        logger.error(erros)
        with engine.connect() as con0:
             r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='E', erros=:erros where id_uniq=:id_uniq",id_uniq=id_uniq,erros=erros)
     con0.close
     logger.info('Executado: '+tbl_destino)
  except Exception as e:
     exc_type, exc_obj, exc_tb = sys.exc_info()
     erros='Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
     logger.error(erros)

# >>>>
# >>>> Execução Principal <<<<
# >>>>

config = ConfigParser.ConfigParser()
config.readfp(open(r'/opt/oracle/upapi/upquery_etl.ini'))
log_file = config['DEFAULT'].get('logfile','/opt/oracle/upapi/upquery_etl.log')
config.readfp(open(r'/opt/oracle/upapi/upquery_etl.ini'))

logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger()

chk = datetime.today()
h_inicio = chk.strftime('%d/%m/%Y %H:%M:%S')

drivers = ['ODBC', 'FIREBIRD','POSTGRESQL','MYSQL','MSSQL','ORACLE','API_CGI','API_OMIE','HCM_SENIOR','API_NEXTI','EXCEL','TXT','API_TRELLO']


logger.info('Serviço Iniciado [UPQUERY_ETL]')

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
              dsn = cx_Oracle.makedsn(fonte_host,port=fonte_port,service_name=fonte_serv)
              engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn))
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

