import pandas as pd
import sqlalchemy as sa
import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from requests.structures import CaseInsensitiveDict
from time import gmtime, strftime
import configparser as ConfigParser
import os
import csv
import glob
import fnmatch
from datetime import datetime

import glob
import fnmatch

exec_comando = 'aaaa,bbbbb,ccccc'
exec_comando = exec_comando.replace(",","|")
print(exec_comando)
comando = exec_comando.split(',')
print(comando)


#sourceFile = open('/opt/oracle/upapi/logs/teste2.log', 'a')
#print('Hello, Python!', file = sourceFile)
#print('2Hello, Python!', file = sourceFile)
#print('3Hello, Python!', file = sourceFile)
#print('4Hello, Python!', file = sourceFile)
#print(datetime.now().strftime("%m/%d/%Y %H:%M:%S"), file = sourceFile)
#sourceFile.close()

#with open(logfile, "w") as log:
#    log.write(('teste 1'))
#    log.write(('teste 2'))
#    log.write(('teste 3'))



#log_file_2 = '/opt/oracle/upapi/logs/teste2.log'
#log_file = '/opt/oracle/upapi/logs/teste1.log'
#logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
#logger = logging.getLogger('root')

#log_file_2 = '/opt/oracle/upapi/logs/teste2.log'
# logging.basicConfig(filename=log_file_2, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

#log = logging.basicConfig(filename=log_file_2, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

#print(log)
#log2 = log.getLogger('log2')


#formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
#handler = logging.FileHandler(log_file_2)
#handler.setFormatter(formatter)
#
#specified_logger = logging.getLogger('log2')
#specified_logger.setLevel(logging.DEBUG)
#specified_logger.addHandler(handler)
#
#specified_logger.debug('teste 2')

#log2.addHandler(fileHandler)

#log2.info('teste2')
#log2.info('teste2 - novamente')

# print(str((datetime.now()) - datetime.now()))


# p_ano_mes = '202306'
# ano = p_ano_mes[0:4]
# mes = p_ano_mes[-2:]
# print(ano)
# print(mes)
# date = dt.date(int(ano), int(mes), 1)
# p_mes_ref = date.strftime('%m/%Y')
# p_data_i  = date.replace(day = 1).strftime('%d/%m/%Y')
# p_data_f  = date.replace(day = calendar.monthrange(date.year, date.month)[1]).strftime('%d/%m/%Y')

# print('Atenção: '+p_mes_ref+' - '+p_data_i+' - '+p_data_f)

#a2 = "aa|bbb|111"
#a3 = "X1|X2|X3"
#dados = pd.Series(a2.split('|'), index=a3.split('|')) 
#print(dados)
#print(dados['X1']) 

#files=fnmatch.filter(os.listdir('/opt/oracle/upapi/testes'), '')
#print(files)

#nm_arquivo   = '/opt/oracle/upapi/testes/relatorio_dados_controle_empreiteiras.csv'
#dados = pd.read_csv(nm_arquivo, sep=";", header=None, low_memory=False, error_bad_lines=False)

#dados = pd.read_excel(ws_arquivo, engine='openpyxl', header=None)
#dados.columns = dados.loc[2].str.strip().str.lower().str.replace(".","_")
#exec_delete = 'select aaa, bbbb from xxx where aaa = :cod1 and bbb = :COD2' 
#param_arq = pd.Series(dados.loc[3])
#for index in range(len(dados.columns)):
#    print(dados.columns[index]) 
#    print(param_arq[dados.columns[index]])
#    exec_delete = exec_delete.upper().replace(":"+str(dados.columns[index].upper()), "'" + str(param_arq[dados.columns[index]])+ "'" )

#exec_delete.replace('aaa', 'CCcccC')

#print(exec_delete.replace('aaa', 'CCcccC'))


##   ws_separador = ';'
##   ws_row_ini   = 2
##   ws_row_cab   = 1
##   ws_row_par   = 2
##   
##   dados = pd.read_csv(ws_arquivo, sep=ws_separador, header=None) 
##   # dados = pd.read_csv(ws_arquivo, sep=ws_separador) 
##   
##   # Monta o cabeçalho pegando do próprio arquivo 
##   if ws_row_cab > 0:
##       ws_cab = dados.loc[ws_row_cab-1]
##   
##   if ws_row_par > 0:
##       ws_par = dados.loc[ws_row_par-1]
##   
##   # Exclui as linhas a serem ignoradas 
##   if ws_row_ini > 1:
##       for i in range(0, ws_row_ini-1):
##           dados.drop(i, axis='rows', inplace = True)
##   
##   dados.columns = ws_cab
##   
##   #print(dados)
##   
##   #print(dados.dtypes.value_counts())
##   
##   #for c in dados.loc[2]: # [dados.dtypes == 'object']:
##   #    print(dados.dtypes)
##       #print(c)
##   
##   #for c in dados.columns[dados.dtypes == 'object']:
##   #    print(c)
##   
##   object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
##   dtyp = {c:sa.types.VARCHAR(dados[c].str.len().max()) for c in object_columns}
##   
##   a2 = "aa|bbb|111"
##   a1 = a2.split('|')
##   #print(a1)
##   #print(a1[0])
##   
##   dados1 = pd.DataFrame(a2.split('|') , index=['a', 'b', 'c']) 
##   #dados1 = pd.DataFrame(a2.split('|')) 
##   #print(dados1)
##   #print(dados1.index.name) 
##   
##   #dados1.index=dados1.index.rename(['Index','Courses_Name','Courses_Duration'])
##   #print(dados1)



