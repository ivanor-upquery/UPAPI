import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import img2pdf
import email, smtplib, ssl
import lxml.html
import sys
import os
import pdfkit
import io
import base64

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import logging
import pandas as pd
import sqlalchemy as sa
import cx_Oracle

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

import datetime
from datetime import datetime, timedelta
from datetime import date
import configparser as ConfigParser

def get_par(dados,parametro,defval):
    try:
        retorno = dados.loc[dados['variavel'] == parametro]['conteudo'].values[0]
    except:
        if defval is None:
           retorno = ''
        else:
           retorno = defval
    return retorno

def up_sender(p_subject, p_body, p_sender_email, p_receiver_email, p_smtp_server, p_password, p_port, p_url, p_largura, p_altura, p_id, p_tp_conteudo, p_blob_content = b' '):
     message = MIMEMultipart()
     message["From"] = p_sender_email
     message["To"] = p_receiver_email
     message["Subject"] = p_subject
     logger.info('URL: ['+str(p_url)+']')
     if  lxml.html.fromstring(p_body).find('.//*') is not None:
         conteudo = MIMEText(p_body, "html")
     else:
         conteudo = MIMEText(p_body, "plain")

     if  p_tp_conteudo == 'URL':
         option = Options()
         option.add_argument('--headless')
         option.add_argument("--no-sandbox");
         option.add_argument('--disable-dev-shm-usage')
         option.add_argument('--single-process')
         option.add_argument('--remote-debugging-port=9222')
         option.add_argument('--start-maximized')
         option.add_argument("--disable-setuid-sandbox")
         option.add_argument('--window-size='+p_largura+'x'+p_altura)
         option.binary_location = r"/opt/google/chrome/google-chrome"
         option.headless = True
         driver = webdriver.Chrome("/usr/bin/chromedriver", options = option)
         print(p_url)
         if  p_url[:5] != 'http:':
             ws_url = 'http://'+p_url
         else:
             ws_url = p_url
         driver.get(str(ws_url))
         time.sleep(30)
         ws_filename = '/tmp/'+p_id+'.png'
         driver.get_screenshot_as_file(ws_filename)
         driver.quit()
         ImgFile = open(ws_filename, "rb")
         PdfContent = img2pdf.convert(ImgFile)
         ImgFile.close()
         os.remove(ws_filename)
         message.attach(conteudo)
         filename = "dashboard.pdf"
         part = MIMEBase("application", "octet-stream")
         part.set_payload(PdfContent)
         encoders.encode_base64(part)
         part.add_header("Content-Disposition", f"attachment; filename= {filename}",)
     if  p_tp_conteudo == 'HTML':
         PdfContent = pdfkit.from_string(p_blob_content, options={'page-size':'A4', 'dpi':400})
         message.attach(conteudo)
         filename = "report.pdf"
         part = MIMEBase("application", "octet-stream")
         part.set_payload(PdfContent)
         encoders.encode_base64(part)

     if  p_tp_conteudo == 'EXCEL':
         message.attach(conteudo)
         filename = "report.xls"
         part = MIMEBase("application", "octet-stream")
         part.set_payload(bytes(p_blob_content))
         encoders.encode_base64(part)

     part.add_header("Content-Disposition", f"attachment; filename= {filename}",)
     message.attach(part)
     text = message.as_string()
     context = ssl.create_default_context()

     try:
         server = smtplib.SMTP(p_smtp_server,p_port)
         server.ehlo()
         try:
            server.starttls(context=context) # Secure the connection
         except Exception as e:
            server.starttls() # Secure the connection
            print('Segunda tentativa STARTTLS')   

         print('Continua')   
         server.ehlo()
         server.login(p_sender_email, p_password)
     except Exception as e:
         print(e)

     logger.info('Enviando...')
     server.sendmail(p_sender_email, p_receiver_email, text)
     logger.info('E-mail Enviado.')
     server.quit()

config = ConfigParser.ConfigParser()

config.readfp(open(r'/opt/oracle/upapi/upquery_msg.ini'))
log_file = config['DEFAULT'].get('logfile','/opt/oracle/upapi/logs/upquery_msg.log')

logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger()

chk = datetime.today()
h_inicio = chk.strftime('%d/%m/%Y %H:%M:%S')

logger.info('Serviço Iniciado [UPQUERY_MSG]')
id_uniq = ''

while True:
  try:
     try:
        count_exec = 0
        while(count_exec==0):
              time.sleep(3)
              config.readfp(open(r'/opt/oracle/upapi/upquery_msg.ini'))
              for section in dict(config):
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
                               data_cnt=pd.read_sql_query("select count(*) as cnt from BI_REPORT_FILA where status='A'",con=con0)
                               count_exec=data_cnt['cnt'].values[0]
                          con0.close
                          if  count_exec != 0:
                              break
                      except SQLAlchemyError as err:
                          error = str(err.__cause__)
                          logger.error('Conn: '+str(section)+ '- '+error)

        logger.info('Início Execução ['+id_client+']')

        with engine.connect() as con0:
             data_exec    = pd.read_sql_query("select * from (select iu_report_fila, destinatario, assunto, mensagem, largura_folha, altura_folha, tp_conteudo, nm_conteudo from BI_REPORT_FILA where status='A' order by dt_criacao) where rownum=1",con=con0)
             receiver_email = data_exec['destinatario'].values[0]
             subject        = data_exec['assunto'].values[0]
             id_uniq        = data_exec['iu_report_fila'].values[0]
             nm_conteudo    = data_exec['nm_conteudo'].values[0]
             tp_conteudo    = data_exec['tp_conteudo'].values[0]
             body           = data_exec['mensagem'].values[0]
             parbuf         = []
             parbuf.append(data_exec['iu_report_fila'].values[0])
             altura         = str(data_exec['altura_folha'].values[0])
             largura        = str(data_exec['largura_folha'].values[0])

        chk = datetime.today()
        h_inicio = chk.strftime('%d/%m/%Y %H:%M:%S')

        logger.info('Inicio: ['+id_uniq+']')

        with engine.connect() as con0:
             r_back = con0.execute("update BI_REPORT_FILA set dt_inicio=sysdate, status='R' where iu_report_fila=:1",id_uniq)

        with engine.connect() as con0:
             data_con    = pd.read_sql_query("select variavel, conteudo from var_conteudo where usuario='DWU' and variavel in ('COM_SERVICO','COM_PORTA','COM_USUARIO','COM_SENHA')",con=con0)
             smtp_server = get_par(data_con,'COM_SERVICO','')
             port        = int(get_par(data_con,'COM_PORTA',0))
             sender      = get_par(data_con,'COM_USUARIO','')
             password    = get_par(data_con,'COM_SENHA','')

        if  tp_conteudo in ('HTML','EXCEL'):
            con = cx_Oracle.connect(user=fonte_user,password=fonte_pass,dsn=dsn,encoding="UTF-8")
            cur = con.cursor()
            parbuf = []
            parbuf.append(nm_conteudo)
            cur.execute("select BLOB_CONTENT from tab_documentos where name = :b1", parbuf)
            row = cur.fetchone()

            if  tp_conteudo == 'EXCEL':
                blob = row[0].read()
            else:
                blob = row[0].read().decode('latin-1')

            up_sender(subject, body, sender, receiver_email, smtp_server, password, port, nm_conteudo, largura, altura, str(id_uniq), tp_conteudo, blob)
        else:
            up_sender(subject, body, sender, receiver_email, smtp_server, password, port, nm_conteudo, largura, altura, str(id_uniq), tp_conteudo, b' ')


        with engine.connect() as con0:
             r_back = con0.execute("update BI_REPORT_FILA set dt_final=sysdate, status='F' where iu_report_fila=:1",id_uniq)

     except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        erros='Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
        logger.error(erros)
        with engine.connect() as con0:
             r_back = con0.execute("update BI_REPORT_FILA set dt_inicio=sysdate, status='E', erros=:erros where iu_report_fila=:id_uniq",id_uniq=id_uniq,erros=erros)
  except Exception as e:
     exc_type, exc_obj, exc_tb = sys.exc_info()
     erros='Linha:['+str(exc_tb.tb_lineno)+'] '+str(e)[0:3000]
     logger.error(erros)

