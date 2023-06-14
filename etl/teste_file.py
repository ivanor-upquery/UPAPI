import pandas as pd
import sqlalchemy as sa
import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from requests.structures import CaseInsensitiveDict
from datetime import datetime, timedelta
from datetime import date
from time import gmtime, strftime
import configparser as ConfigParser
import os
import csv


import glob
import fnmatch

import glob
import fnmatch

a2 = "aa|bbb|111"
a3 = "X1|X2|X3"
dados = pd.Series(a2.split('|'), index=a3.split('|')) 
print(dados)

print(dados['X1']) 
      

          


#files=fnmatch.filter(os.listdir('/opt/oracle/upapi/testes'), '')
#print(files)


##   ws_arquivo   = '/opt/oracle/upapi/testes/arquivo_teste.csv'
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



