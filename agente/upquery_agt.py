import cx_Oracle
import csv
from io import StringIO
import numpy as np
import pandas as pd
import io
import sqlalchemy as sa
import time
import datetime
from datetime import datetime, timedelta
import warnings
import threading
import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def get_par(dados,parametro,defval):
    try:
        retorno = dados.loc[dados['cd_parametro'] == parametro]['conteudo'].values[0]
    except:
        if defval is None:
           retorno = ''
        else:
           retorno = defval
    return retorno

fonte_host='demo.upquery.com'
fonte_port=1521
fonte_serv='controle'
fonte_user='dwu'
fonte_pass='s82sksw9qskw0'

warnings.filterwarnings("ignore")

dsn = cx_Oracle.makedsn(fonte_host,port=fonte_port,service_name=fonte_serv)
engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn))

log_file = '/opt/oracle/upapi/logs/upquery_agt.log'

logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger()


def exec_etl(p_cd_cliente):
   while True:
         pars = {"b1": p_cd_cliente}
         count_exec = 0
         count_run=0

         try:
             with engine.connect() as con0:
                  data_cnt=pd.read_sql_query("select count(*) as cnt from LOCK_CLIENTE where id_cliente = :b1 and status='A'",con=con0, params=pars)
                  count_run=data_cnt['cnt'].values[0]
                  con0.close
             if  count_run > 0:
                 break
             try:
                 with engine.connect() as con0:
                      data_cnt=pd.read_sql_query("select count(*) as cnt from CTB_ACOES_EXEC where id_cliente = :b1 and status='STANDBY' ",con=con0, params=pars)
                      cnt_ante = data_cnt['cnt'].values[0]
                      con0.close
                 connection = engine.raw_connection()
                 cursor = connection.cursor()
                 cursor.callproc("exec_agendamento", [p_cd_cliente])
                 cursor.close()
                 connection.commit()
                 with engine.connect() as con0:
                      data_cnt=pd.read_sql_query("select count(*) as cnt from CTB_ACOES_EXEC where id_cliente = :b1 and status='STANDBY' ",con=con0, params=pars)
                      cnt_atu = data_cnt['cnt'].values[0]
                      con0.close
                 if  cnt_atu > 0 and cnt_ante == 0:
                     logger.info('Agendado   ['+p_cd_cliente+'] - ['+str(cnt_atu)+']')

                 with engine.connect() as con0:
                      data_cnt=pd.read_sql_query("select count(*) as cnt from VM_SYS_FILA where id_cliente = :b1 and rownum=1",con=con0, params=pars)
                      count_exec=data_cnt['cnt'].values[0]
                      con0.close
                 if  count_exec == 0:
                     break
             except SQLAlchemyError as err:
                 error = str(err.__cause__)
                 logger.error('Conn-'+error)
                 break
         except:
             error = str(e) 
             logger.error('Conn-'+error)
             break

         connection = engine.raw_connection()
         cursor = connection.cursor()
         cursor.callproc("lock_status", [p_cd_cliente,'A'])
         cursor.close()
         connection.commit()

         #con = cx_Oracle.connect(user=fonte_user,password=fonte_pass,dsn=fonte_serv,encoding="UTF-8")
         con = cx_Oracle.connect(user=fonte_user,password=fonte_pass,dsn=dsn,encoding="UTF-8")
         cur = con.cursor()

         parbuf = []
         parbuf.append(p_cd_cliente)
         cur.execute("select BLOB_CONTENT, ID_CLIENTE, CHECK_ID, ID_ACAO, TABELA_CRITERIO, DB_ORIGEM from VM_SYS_FILA where id_cliente = :1 and rownum=1", parbuf)
         row = cur.fetchone()
         blob = row[0].read()
         blob_content = blob.decode('latin-1')
         parbuf = []
         parbuf.append(row[1])
         parbuf.append(row[2])
         parbuf.append(row[3])
         tabela_criterio = row[4]
         db_origem       = row[5]
         id_cliente      = row[1]
         cur.close()
         con.close()

         with engine.connect() as con0:
              r_back = con0.execute("update TMP_DOCS SET status = 'RUNNING', last_updated = sysdate where TMP_DOCS.id_cliente = :1 and TMP_DOCS.check_id = :2 and TMP_DOCS.id_acao = :3",parbuf)

         try:

            with engine.connect() as con0:
                  data_con = pd.read_sql_query('select TABELA_TRANSP , ANO_AGENDAMENTO , MES_AGENDAMENTO, ID_CONEXAO from CTB_ACOES_EXEC EXEC where EXEC.id_cliente = :1 and EXEC.check_id = :2 and EXEC.id_acao = :3',con=con0,params=parbuf)

            parcon = []
            tabela_transp   = data_con['tabela_transp'].values[0]
            ano_agendamento = data_con['ano_agendamento'].values[0]
            mes_agendamento = data_con['mes_agendamento'].values[0]
            parcon.append(p_cd_cliente)

            logger.info('Iniciado   ['+p_cd_cliente+'] - ['+tabela_transp+']')
            with engine.connect() as con0:
                data_con    = pd.read_sql_query('select ID_CLIENTE, CD_PARAMETRO, CONTEUDO from CTB_DESTINO where id_cliente = :1',con=con0,params=parcon)
                cnx_host    = get_par(data_con,'HOST','')
                cnx_port    = int(get_par(data_con,'PORTA',0))
                cnx_user    = get_par(data_con,'USUARIO','')
                cnx_pass    = get_par(data_con,'SENHA','')
                cnx_service = get_par(data_con,'SERVICE_NAME','')

            dsn_destino = cx_Oracle.makedsn(cnx_host,port=cnx_port,service_name=cnx_service)
            destino = create_engine('oracle+cx_oracle://%s:%s@%s' % (cnx_user, cnx_pass, dsn_destino))
            tmp_tabela= tabela_transp.split('@')
            nm_tabela = tmp_tabela[0]

            parcli = []
            parcli.append(id_cliente)
            p_virgula = ''
            p_empresa_fix = 'Sem'
            p_empresas = '('
            with engine.connect() as con0:
                data_con = pd.read_sql_query("select prefixo_tabela, identificador_filial from CTB_CLIENTE_EMPRESA where codigo_cliente= :1 and trim(status)='ATIVO'",con=con0,params=parcli)
                for prefixo in data_con['prefixo_tabela']:
                    if  prefixo:
                        if  p_empresa_fix == 'Sem':
                            p_empresa_fix = prefixo
                        p_empresas = p_empresas+p_virgula+chr(39)+prefixo+chr(39)
                        p_virgula = ','
                p_empresas = p_empresas+')'
            if  not p_empresas:
                p_empresas = ' '

            if  tabela_criterio == 'SCHEDULER':
                cmd_delete = 'delete '+nm_tabela+' where codigo_empresa = '+chr(39)+id_cliente+chr(39)+' and ano_agendamento = '+chr(39)+ano_agendamento+chr(39)+' and mes_agendamento = '+chr(39)+mes_agendamento+chr(39)+' and cd_empresa in '+p_empresas 
            else:
                cmd_delete = 'delete '+nm_tabela+' where cd_empresa in '+p_empresas

            # Busca COLUNAS da tabela de insert dos dados 
            get_colunas = "select column_name from user_tab_columns where table_name = :1 and column_name not in ('CODIGO_EMPRESA','DT_UPQUERY_REGISTRO','ANO_AGENDAMENTO','MES_AGENDAMENTO') order by column_id"
            parcol = []
            colunas = []
            ws_try = 10
            parcol.append(nm_tabela)
            with destino.connect() as con_destino:              
                while(ws_try > 1):
                    try:
                        data_con = pd.read_sql_query(get_colunas,con=con_destino,params=parcol)
                        for c_column in data_con['column_name']:
                            if  c_column:
                                colunas.append(c_column.lower())
                        ws_try = 0
                    except:
                        time.sleep(1)
                        ws_try-=1
                con_destino.close()

            if  db_origem == 'FILE':
                colunas.remove('cd_empresa')

            # Converte CSV do blob em dados do insert 
            if p_cd_cliente == "000000098" or p_cd_cliente == "000000096":
                dados=pd.read_csv(io.StringIO(str(blob_content)),names=colunas, header=None, sep=",", dtype=str)  # teste com str para um cliente Finatto
            else:
                dados=pd.read_csv(io.StringIO(str(blob_content)),names=colunas, header=None, sep=",")

            if  tabela_criterio == 'SCHEDULER':
                dados.insert(loc=0, column='mes_agendamento', value=mes_agendamento)
                dados.insert(loc=0, column='ano_agendamento', value=ano_agendamento)
                if  db_origem == 'FILE':
                    dados.insert(loc=0, column='cd_empresa', value=p_empresa_fix) 
            if  'cd_empresa' not in dados.columns:
                dados.insert(loc=0, column='cd_empresa', value=p_empresa_fix)

            data_local = datetime.today()
            ref_local = data_local.strftime('%Y-%m-%d %H:%M:%S')
            dados.insert(loc=0, column='codigo_empresa',      value=id_cliente)
            dados.insert(loc=0, column='dt_upquery_registro', value=ref_local)

            colunas = list(dados.columns)
            dados.columns = colunas
            dados.columns = dados.columns.str.strip().str.lower()

            dados = dados.astype(object).where(pd.notnull(dados),None)
            all_columns = list(dados)

            object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
            dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}

            dados = dados.astype(object).where(pd.notnull(dados),None)
            all_columns = list(dados)
            dados.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
            dtyp = {c:sa.types.VARCHAR(dados[c].astype('str').str.len().max()) for c in object_columns}

            # Insere os dados e atualiza o status da TMP_DOCS 
            status = 'OK'
            try:
                with destino.connect() as con_destino:
                    con_destino.execute('''alter session set NLS_DATE_FORMAT=\'YYYY-MM-DD HH24:MI:SS\'''')
                    con_destino.execute('''alter session set NLS_NUMERIC_CHARACTERS =\'.,\'''')
                    con_destino.execute(cmd_delete)
                    dados.to_sql(name=nm_tabela,con=con_destino, if_exists='append', index=False, chunksize=50000, dtype=dtyp)
                    with engine.connect() as con0:
                        r_back = con0.execute("update TMP_DOCS SET status = 'END', last_updated = sysdate where TMP_DOCS.id_cliente = :1 and TMP_DOCS.check_id = :2 and TMP_DOCS.id_acao = :3",parbuf)
            except Exception as e:
                erros='Erro atualizando status da TMP_DOCS para END: '+str(e)[0:3000]
                raise Exception(erros) 
                #exc_type, exc_obj, exc_tb = sys.exc_info()
                #status='ERRO'
                #erros='Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
                #logger.error(erros)
                #with engine.connect() as con0:
                #    r_back = con0.execute("update TMP_DOCS SET status = 'ERRO', last_updated = sysdate where TMP_DOCS.id_cliente = :1 and TMP_DOCS.check_id = :2 and TMP_DOCS.id_acao = :3",parbuf)

            #connection = engine.raw_connection()
            #cursor = connection.cursor()
            #cursor.callproc("lock_status", [p_cd_cliente,'I'])
            #cursor.close()
            #connection.commit()
            #logger.info('Finalizado ['+p_cd_cliente+'] - ['+tabela_transp+'] - ['+status+']')

         except Exception as e:
            try:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                nr_linha = exc_tb.tb_lineno
            except:
                nr_linha = 0
            status='ERRO'
            erros='Linha:['+str(nr_linha)+'] '+str(e)[0:3000]
            logger.error(erros)
            with engine.connect() as con0:
                r_back = con0.execute("update TMP_DOCS SET status = 'ERRO', last_updated = sysdate where TMP_DOCS.id_cliente = :1 and TMP_DOCS.check_id = :2 and TMP_DOCS.id_acao = :3",parbuf)

         connection = engine.raw_connection()
         cursor = connection.cursor()
         cursor.callproc("lock_status", [p_cd_cliente,'I'])
         cursor.close()
         connection.commit()
         logger.info('Finalizado ['+p_cd_cliente+'] - ['+tabela_transp+'] - ['+status+']')

logger.info('Serviço Iniciado [UPQUERY_AGT - AGENTE]')

ws_parado=0
while True:

     if  len(threading.enumerate()) < 11:
         with engine.connect() as con0:
              #cliente_status=pd.read_sql_query("select id_cliente, status from LOCK_CLIENTE where status='I' and id_cliente not in ('000000113','000000096') and id_cliente in ('000000128','000000078','000000037') ",con=con0)
              cliente_status=pd.read_sql_query("select id_cliente, status from LOCK_CLIENTE where status='I' and id_cliente not in ('000000113','000000096') ",con=con0)
              con0.close

         for ind in range(0, len(cliente_status)):
             time.sleep(1)
             if  cliente_status.iloc[ind]['id_cliente'] not in threading.enumerate():
                 exec_thr = threading.Thread(target=exec_etl, name=cliente_status.iloc[ind]['id_cliente'], args=(cliente_status.iloc[ind]['id_cliente'],))
                 exec_thr.setDaemon(True)
                 exec_thr.start()

         time.sleep(5)
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

