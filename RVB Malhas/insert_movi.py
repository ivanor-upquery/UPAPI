import json
import cx_Oracle
import requests

import urllib.request 
import urllib.parse
from urllib.request import urlopen

import pandas as pd
from pandas.io.json import json_normalize
import cx_Oracle
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.dialects.oracle import VARCHAR2, VARCHAR, CHAR, NUMBER, DATE
from dateutil import tz
import datetime
from datetime import datetime, timedelta
from datetime import date


def insert_log(p_inicio,p_resposta,p_texto, p_tabela):
    with engine.connect() as con1:
         chk = datetime.today()
         h_fim = chk.strftime('%d/%m/%Y %H:%M:%S')
         execucao = []
         execucao.append(p_inicio)
         execucao.append(h_fim)
         execucao.append(p_tabela)
         execucao.append(p_resposta)
         execucao.append(p_texto)
         result = con1.execute("insert into VM_DETALHES_AGENT values (to_date(:1,'dd/mm/yyyy hh24:mi:ss'),to_date(:2,'dd/mm/yyyy hh24:mi:ss'),'00000096',:3,:4,'',:5)",execucao)

def remove_timezone(dt):
    try:
       to_zone=tz.tzlocal()
       local=dt.replace(tzinfo=tz.tzutc())
       return local.astimezone(to_zone)
    except ValueError:
        return dt

def strurl_ticket(pular,data):
    # url_flex = 'https://api.movidesk.com/public/v1/tickets?token=be637dc3-62f5-496a-81c4-887dbe3ab291&$select=id,type,subject,category,urgency,status,origin,createddate,lifetimeWorkingTime,resolvedIn,lastActionDate,slaSolutionDate,slaResponseDate,slaRealResponseDate,serviceFirstLevel,serviceFirstLevelId,&$expand=owner($select=businessName,email,id)&$filter=lastUpdate%20gt%20'+data+'%20or%20createdDate%20gt%20'+data+'&$orderby=createdDate&$top=1000&$skip='+str(pular)
    url_flex = 'https://api.movidesk.com/public/v1/tickets?token=be637dc3-62f5-496a-81c4-887dbe3ab291&$select=id,type,subject,category,urgency,status,origin,createddate,lifetimeWorkingTime,resolvedIn,lastActionDate,slaSolutionDate,slaResponseDate,slaRealResponseDate,serviceFirstLevel,serviceFirstLevelId,&$expand=owner($select=businessName,email,id),&$expand=clients($select=id,businessName,email)&$filter=lastUpdate%20gt%20'+data+'%20or%20createdDate%20gt%20'+data+'&$orderby=createdDate&$top=1000&$skip='+str(pular)
    print(url_flex)
    return url_flex

def importa_ticket():

    formato=['serviceFirstLevelId','serviceFirstLevel','slaRealResponseDate','slaResponseDate','slaSolutionDate','lastActionDate','resolvedIn','lifeTimeWorkingTime','createdDate','origin','status','urgency','category','subject','type','id','owner.email','owner.businessName','owner.id','clients.id','clients.email','clients.businessName',]
    colunas=['serviceFirstLevelId','serviceFirstLevel','slaRealResponseDate','slaResponseDate','slaSolutionDate','lastActionDate','resolvedIn','lifeTimeWorkingTime','createdDate','origin','status','urgency','category','subject','type','id','email','businessName','ownerid','clientsid','clientsemail','clientsbusinessName']

    pular = 1

    try:
        with engine.connect() as con0:
            if  not con0.dialect.has_table(con0, 'ora$tmp_ticket'):
                result = con0.execute("create global temporary table ora$tmp_ticket on commit preserve rows as select * from VM_TICKET_API where 1=2")
            while (pular != 0):
                if  pular == 1:
                    pular = 0
                url_api = strurl_ticket(pular, referencia)
                leitura = urllib.request.urlopen(url_api).read()
                data = json.loads(leitura)

                pular = pular + 1000
                if  data != []:
                    dados = pd.json_normalize(data)
                    dados = pd.DataFrame(dados,columns=formato)
                    dados['createdDate']         = pd.to_datetime(dados['createdDate']).apply(remove_timezone)
                    dados['resolvedIn']          = pd.to_datetime(dados['resolvedIn']).apply(remove_timezone)
                    dados['slaRealResponseDate'] = pd.to_datetime(dados['slaRealResponseDate']).apply(remove_timezone)
                    dados['slaResponseDate']     = pd.to_datetime(dados['slaResponseDate']).apply(remove_timezone)
                    dados['slaSolutionDate']     = pd.to_datetime(dados['slaSolutionDate']).apply(remove_timezone)
                    dados['lastActionDate']      = pd.to_datetime(dados['lastActionDate']).apply(remove_timezone)
                    print(dados.info())
                    print(dados)
                    dados.columns = colunas
                    dados.columns = dados.columns.str.strip().str.lower()
                    object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
                    dtyp = {c:sa.types.VARCHAR(dados[c].str.len().max()) for c in object_columns}
                    dados.to_sql(name='ora$tmp_ticket',con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
                else:
                    pular = 0

            result = con0.execute("delete from VM_TICKET_API where id in (select id from ora$tmp_ticket)")
            del_linhas = result.rowcount
            result = con0.execute("insert into VM_TICKET_API select * from ora$tmp_ticket")
            insert_linhas = result.rowcount
            #result = con0.execute("truncate table ora$tmp_ticket")
            insert_log(h_inicio,'EXECUTADO OK','Deletadas: '+str(del_linhas)+' Inseridas: '+str(insert_linhas), 'VM_TICKET')
    except Exception as e:
        insert_log(h_inicio,'Erro',e,'VM_TICKET')


def importa_person():

    formato   =['id','businessName','bossName','role']
    colunas   =['id','businessName','bossName','role']
    tabela    ='VM_PERSON_API'
    tabela_tmp='ora$tmp_person'
    tipo_log  = 'VM_PERSON'

    try:

        url_api = 'https://api.movidesk.com/public/v1/persons?token=be637dc3-62f5-496a-81c4-887dbe3ab291&$select=id,businessName,bossName,role'
        leitura = urllib.request.urlopen(url_api).read()
        data = json.loads(leitura)
        dados = pd.json_normalize(data)
        dados = pd.DataFrame(dados,columns=formato)

        print(dados.info())
        print(dados)

        dados.columns = colunas
        dados.columns = dados.columns.str.strip().str.lower()
        object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
        dtyp = {c:sa.types.VARCHAR(dados[c].str.len().max()) for c in object_columns}


        with engine.connect() as con0:
            if  not con0.dialect.has_table(con0, tabela_tmp):
                result = con0.execute("create global temporary table "+tabela_tmp+" on commit preserve rows as select * from "+tabela+" where 1=2")
            
            result = con0.execute("delete from " + tabela_tmp)
            dados.to_sql(name=tabela_tmp,con=con0, if_exists='append', index=False, chunksize=50000,  dtype=dtyp)
            
            result = con0.execute("delete from " + tabela)
            del_linhas = result.rowcount
            result = con0.execute("insert into " + tabela + " select * from " + tabela_tmp)
            insert_linhas = result.rowcount
            #result = con0.execute("truncate table ora$tmp_ticket")
            insert_log(h_inicio,'EXECUTADO OK','Deletadas: '+str(del_linhas)+' Inseridas: '+str(insert_linhas), tipo_log)
    except Exception as e:
        insert_log(h_inicio,'Erro',e, tipo_log)



#---------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Bloco Principal
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------

host        = 'bmw'
servicename = 'dbbi'
usuario     = 'dwu'
senha       = 'ITwen2020'
## host        = '172.1.1.230'
## servicename = 'DESENV'
## usuario     = 'dwup2'
## senha       = 'dwup2'

atual      = date.today()+timedelta(days=-2)
referencia = atual.strftime("%Y-%m-%d")+'T00:00:00.00z'
dsn        = cx_Oracle.makedsn(host,port=1521,service_name=servicename)
engine     = create_engine('oracle+cx_oracle://%s:%s@%s' % (usuario, senha, dsn))
chk        = datetime.today()
h_inicio   = chk.strftime('%d/%m/%Y %H:%M:%S')

importa_ticket()
importa_person()



