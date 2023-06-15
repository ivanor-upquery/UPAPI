import sys
import logging
import pandas as pd
import sqlalchemy as sa
import cx_Oracle
import time
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
import fnmatch
import copy
import etl_copel 

# -------------------------------------------------------------------------------------------
# Formato Parametro 
# -------------------------------------------------------------------------------------------
def get_par(dados,parametro,defval):
    try:
        retorno = dados.loc[dados['cd_parametro'] == parametro]['conteudo'].values[0]
    except:
        if defval is None:
           retorno = ''
        else:
           retorno = defval
    return retorno

# -------------------------------------------------------------------------------------------
#  função que importa arquivo enviado por FTP
# -------------------------------------------------------------------------------------------
def ftp_client():
    ws_msg = '['+id_client+'] - FTP - '
    try: 
        con0 = cx_Oracle.connect(user=fonte_user,password=fonte_pass,dsn=dsn,encoding="UTF-8")
        cur0 = con0.cursor()
        cur1 = con0.cursor()
        cur0.execute("select step.step_id, con.conteudo, step.comando from etl_conexoes con, etl_step step where step.id_conexao = con.id_conexao and con.cd_parametro = 'DB' and step.tipo_comando = 'INTEGRADOR_FTP'") 
        steps = cur0.fetchall()
        for step in steps:
            try: 
                ws_step_id  = step[0] 
                ws_arquivo  = str(step[2]).split('|')[0]   # Nome do arquivo 
                for file in fnmatch.filter(os.listdir(fonte_ftp),ws_arquivo):
                    cnt=pd.read_sql_query("select count(*) as CNT from ETL_FILA where status='A' and run_id=:1 ",con=con0,params=[ws_step_id])
                    if cnt['CNT'].values[0] == 0:
                        cur1.callproc('etf.ftp_etl_fila_new',[ws_step_id, file])
                        logger.info(ws_msg + 'Criado fila arquivo: ' + fonte_ftp+'/'+file)
            except Exception as e:
                erros=ws_msg + 'Erro cliente/acao ['+id_client+'/'+ws_step_id+']: '+str(e)[0:3500]
                logger.error(erros)
        cur0.close()
        con0.close()
    except Exception as e:
        erros=ws_msg + 'Erro: '+str(e)[0:3500]
        logger.error(erros)


# -------------------------------------------------------------------------------------------
#  função Operação 
# -------------------------------------------------------------------------------------------
def exec_client(id_uniq, session, engine, dsn):

    # id_uniq = copy.copy(id_uniq)  
    # session = copy.deepcopy(session)
    # engine  = copy.deepcopy(engine)
    # dsn     = copy.deepcopy(dsn)

    id_client = session
    fonte_user = config[session].get('username','')
    fonte_pass = config[session].get('password','')

    logger.info('a1')

    # Busca dados da fila e da conexão 
    with engine.connect() as con0:
        data_exec    = pd.read_sql_query("select id_uniq, tbl_destino, comando, comando_limpar, id_conexao from etl_fila where id_uniq=:1 and status='A'",con=con0, params=[id_uniq])
        if len(data_exec) < 1:
            return
        id_conexao   = data_exec['id_conexao'].values[0]
        exec_comando = data_exec['comando'].values[0]
        data_con      = pd.read_sql_query('select * from ETL_CONEXOES where id_conexao = :1',con=con0,params=[id_conexao])
        con0.close() 

    cnx_db        = get_par(data_con,'DB','')
    cnx_loc_file  = get_par(data_con,'LOC_FILE','')
    cnx_usuario   = get_par(data_con,'USUARIO','')
    cnx_senha     = get_par(data_con,'SENHA','')
    
    logger.info('a2')
    logger.info(data_con)
    tipo_integracao = cnx_db.lower()

    con0 = cx_Oracle.connect(user=fonte_user,password=fonte_pass,dsn=dsn,encoding="UTF-8")
    cur0 = con0.cursor()
    ws_parametros = cur0.var(str)
    ws_conteudos  = cur0.var(str)
    cur0.execute ('begin etf.ftp_get_comando_param(:1,:2,:3,:4); end;', [cnx_db, exec_comando, ws_parametros, ws_conteudos])
    con0.close()
    par_comando = pd.Series(ws_conteudos.getvalue().split('|'), index=ws_parametros.getvalue().split('|')) 
    

    try:

        logger.info('INICIO - Cliente ['+id_uniq+'] Conexao ['+cnx_db+']')
        with engine.connect() as con0:
            r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='R' where id_uniq=:1",id_uniq)

        if tipo_integracao == 'copel':
            arquivo = par_comando['FILE_NAME']
            dt_i    = par_comando['DT_INICIAL']
            dt_f    = par_comando['DT_FINAL']

            logger.info('a1')
            logger.info(arquivo)
            logger.info(dt_i)
            logger.info(dt_f)

            etl_copel.f_copel(cnx_usuario, cnx_senha, cnx_loc_file, arquivo, dt_i, dt_f)
            if os.path.isfile(cnx_loc_file+'/'+arquivo):
                logger.info('Download OK - Cliente ['+id_uniq+'] Conexao ['+cnx_db+']')
            else: 
                logger.info('Download ERRO - Cliente ['+id_uniq+'] Conexao ['+cnx_db+'] - Arquivo não gerado')

        logger.info('FIM    - Cliente ['+id_uniq+'] Conexao ['+cnx_db+']')

    except Exception as e:
        erros=str(e)[0:3000]
        logger.error(erros)
        with engine.connect() as con0:
            r_back = con0.execute("update ETL_FILA set dt_inicio=sysdate, status='E', erros=:erros where id_uniq=:id_uniq",id_uniq=id_uniq,erros=erros)




# -------------------------------------------------------------------------------------------
# >>>> Execução Principal <<<<
# -------------------------------------------------------------------------------------------
drivers = ['COPEL', 'CELESC']

config = ConfigParser.ConfigParser()
config.readfp(open(r'/opt/oracle/upapi/upquery_ftp.ini'))
log_file = config['DEFAULT'].get('logfile','/opt/oracle/upapi/upquery_ftp.log')
logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger   = logging.getLogger()
chk      = datetime.today()
h_inicio = chk.strftime('%d/%m/%Y %H:%M:%S')
logger.info('Serviço Iniciado [UPQUERY_FTP]')

ws_parado=0
while True:
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
            
            # Não é necessário criar tread para esse processo, é rápido e precisa ser validados a todo momento para verificar se tem arquivo na pasta de FTP 
            if fonte_ftp != "N/A":
                ftp_client()

            try:
                count_exec = 0
                cnx_db     = ""
                id_uniq    = ""
                
                with engine.connect() as con0:
                    data_exec=pd.read_sql_query("select* from (select f.id_uniq, c.conteudo from etl_conexoes c, etl_fila f where c.id_conexao = f.id_conexao and c.cd_parametro = 'DB' and f.status='A' order by f.dt_criacao) where rownum=1",con=con0)
                    if  len(data_exec) >= 1:
                        count_exec = 1
                        id_uniq = data_exec['id_uniq'].values[0]
                        cnx_db  = data_exec['conteudo'].values[0]
                    con0.close

                if  cnx_db not in drivers:    # Cancela se o tipo de driver não faz parte desse processo de FTP 
                    count_exec = 0

                if  count_exec != 0 and len(threading.enumerate()) < 11 and section not in threading.enumerate():
                    exec_thr = threading.Thread(target=exec_client, name=section, args=(id_uniq, section, engine, dsn))
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
