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

dados = pd.read_csv('/opt/oracle/upapi/testes/arquivo_teste.csv', sep=';', header=None)


#print(dados.dtypes) 

# print(dados.loc[0])


print(dados)


#print(dados)

#for c in dados.columns: # [dados.dtypes == 'object']:
#    print(dados.dtypes)
#    print(c)

object_columns = [c for c in dados.columns[dados.dtypes == 'object'].tolist()]
dtyp = {c:sa.types.VARCHAR(dados[c].str.len().max()) for c in object_columns}

