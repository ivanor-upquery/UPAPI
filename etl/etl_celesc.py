import datetime
import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar
import os
import time
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def f_celesc(p_cnx, p_usuario, p_senha, pasta, arquivo, ano_mes):
    
    log_file = '/opt/oracle/upapi/logs/upquery_etl.log'
    logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
    logger = logging.getLogger()

    arquivo_download = pasta + arquivo

    option = Options()
    option.add_argument('--headless')
    option.add_argument("--no-sandbox")
    option.add_argument('--disable-dev-shm-usage') 
    option.add_argument('--single-process')
    option.add_argument('--remote-debugging-port=9222')
    option.add_argument('--start-maximized')
    option.add_argument("--disable-setuid-sandbox")
    option.add_argument("--extension-load-timeout=60000")
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
    driver.set_page_load_timeout(1500)
    time.sleep(5)
    
    driver.get("http://emobile.celesc.com.br:8080/MobileExpert/Login.do?lang=pt")
    time.sleep(25)

    driver.find_element_by_class_name("lbl_campo_maiusculo").send_keys(p_usuario)
    driver.find_element_by_class_name("lbl_campo").send_keys(p_senha)
    time.sleep(2)

    pressione = driver.find_element_by_class_name("lgn_btn")
    driver.execute_script("arguments[0].click();", pressione)
    time.sleep(5)
    
    pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[1]/table/tbody/tr[16]/td/table/tbody/tr[10]/td/a')
    driver.execute_script("arguments[0].click();", pressione)
    time.sleep(5)

    driver.find_element_by_name("searchConcessionariaId").send_keys(Keys.ARROW_DOWN+Keys.SPACE)
    driver.find_element_by_name("searchEmpreiteiraId").send_keys(Keys.ARROW_DOWN+Keys.SPACE)

    #select_box = driver.find_element_by_name("searchLocalidadeId")
    #options = [x for x in select_box.find_elements_by_tag_name("option")]

    ano = ano_mes[0:4]
    mes = ano_mes[-2:]
    print(ano)
    print(mes)
    date = dt.date(int(ano), int(mes), 1)
    p_mes_ref = date.strftime('%m/%Y')
    p_data_i  = date.replace(day = 1).strftime('%d/%m/%Y')
    p_data_f  = date.replace(day = calendar.monthrange(date.year, date.month)[1]).strftime('%d/%m/%Y')

    print('Atenção: '+p_mes_ref+' - '+p_data_i+' - '+p_data_f)

    lista=['Todas']

    for locais in lista:
        if  locais != '0':
            logger.info(p_cnx+' - Download........  Local:'+ locais + '   Periodo: '+p_data_i+' - '+p_data_f)

            if os.path.isfile(arquivo_download):
                os.remove(arquivo_download)
            time.sleep(5) 

            pressione = driver.find_element_by_name("dataInicio")
            driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, p_data_i)
            pressione = driver.find_element_by_name("dataFim")
            driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, p_data_f)
            driver.find_element_by_name("searchLocalidadeId").send_keys(locais)

            pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr[3]/td/form/table[3]/tbody/tr/td[2]/button')
            driver.execute_script("arguments[0].click();", pressione)

            try:
                alert = driver.switch_to_alert()
                alert.accept()
            except:
                saida=0
            time.sleep(20)

            driver.execute_script("arguments[0].click();", pressione)
            time.sleep(800)

            try:
                alert = driver.switch_to_alert()
                alert.accept()
            except:
                saida=0
            
            logger.info(p_cnx+' - Download........  Local:'+ locais + '   Periodo: '+p_data_i+' - '+p_data_f + '............ OK')

    logger.info(p_cnx+' - Download........ OK')
    driver.quit()
