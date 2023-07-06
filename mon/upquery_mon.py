import logging
import cx_Oracle
from sqlalchemy import create_engine
import configparser as ConfigParser
import subprocess




# ----------------------------------------------------------------------------------------------------------------------------------------------------
# >>>> Atualiza tabela MON_SISTEMA <<<<
# ----------------------------------------------------------------------------------------------------------------------------------------------------
def atualiza_monit(p_engine, p_status, p_erro, p_tp, p_nm):
    with p_engine.connect() as con0:
        print('xxx1')

        r_back = con0.execute("begin update MON_SISTEMA set cd_status=:status, ds_erro=:erro, dh_monit=sysdate where tp_monit=:tp and nm_monit=:nm; " + 
                              "   if sql%notfound then " +
                              "      insert into MON_SISTEMA (tp_monit, nm_monit, dh_monit, cd_status, ds_erro) values (:tp, :nm, sysdate, :status, :erro); " +
                              "    end if; " + 
                              "    commit;" +
                              "end;",tp=p_tp,nm=p_nm,status=p_status,erro=p_erro)
        print('xxx2')        

        con0.close




# ----------------------------------------------------------------------------------------------------------------------------------------------------
# >>>> Check situação de serviços Linux <<<<
# ----------------------------------------------------------------------------------------------------------------------------------------------------
def check_service(p_section, p_engine):
    tp = 'SERVICE'
    for prg in config[p_section]:
        nm = str(config[p_section].get(prg)) 
        logger.info('Service: '+nm)

        try: 
            cmd = 'ps ax |grep -v grep |grep '+ nm
            returned = subprocess.call(cmd, shell=True)  # returns the exit code in unix
            if returned == 0: 
                status = 'OK'
                erro   = ''
            else:
                status = 'ERRO'
                erro   = 'Servico nao esta executando'

            atualiza_monit(p_engine, status, erro, tp, nm)

        except Exception as e:
            status = 'ERRO'
            erros=str(e)[0:3000]
            logger.error('Service: '+nm+' ERRO:' + erros)
            atualiza_monit(engine, status, erro, tp, nm)




# ----------------------------------------------------------------------------------------------------------------------------------------------------
# >>>> Execução Principal <<<<
# ----------------------------------------------------------------------------------------------------------------------------------------------------
config = ConfigParser.ConfigParser()
config.readfp(open(r'/opt/oracle/upapi/upquery_mon.ini'))

log_file = '/opt/oracle/upapi/logs/upquery_mon.log'
logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger()

logger.info('Inicio')
try:
    for section in dict(config):
        if  section in ['db']:
            fonte_host = config[section].get('host','localhost')
            fonte_port = config[section].get('port','1521')
            fonte_serv = config[section].get('servicename','')
            fonte_user = config[section].get('username','')
            fonte_pass = config[section].get('password','')
            dsn = cx_Oracle.makedsn(fonte_host,port=fonte_port,service_name=fonte_serv)
            engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (fonte_user, fonte_pass, dsn))
        if  section in ['service']:
            check_service(section, engine); 
except Exception as e:
    erros=str(e)[0:3000]
    print(erros)
    raise Exception(erros)   

logger.info('Fim')

