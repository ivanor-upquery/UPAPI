import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def f_copel(p_usuario, p_senha, pasta, arquivo, p_data_i, p_data_f):

    log_file = '/opt/oracle/upapi/logs/copel.log'
    logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
    logger = logging.getLogger()

    arquivo_download = pasta + '/' + arquivo
    option = Options()
    option.add_argument('--headless')
    option.add_argument("--no-sandbox")
    option.add_argument('--disable-dev-shm-usage') 
    option.add_argument('--single-process')
    option.add_argument('--remote-debugging-port=9222')
    option.add_argument('--start-maximized')
    option.add_argument("--disable-setuid-sandbox")
    option.add_argument("--enable-features=NetworkServiceInProcess")
    option.binary_location = r"/opt/google/chrome/google-chrome"

    prefs={'download.default_directory': pasta}

    option.add_experimental_option('prefs', {
        "download.default_directory": r""+pasta,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
    })
    option.headless = True #Esconder o processo False/True
    driver = webdriver.Chrome("/usr/bin/chromedriver", options = option)
    time.sleep(5)
    
    driver.get("https://www.copel.com/lis/")
    time.sleep(10)
    
    driver.find_element_by_class_name("lbl_campo_maiusculo").send_keys(p_usuario)
    driver.find_element_by_class_name("lbl_campo").send_keys(p_senha)
    time.sleep(2)
    
    pressione = driver.find_element_by_class_name("lgn_btn")
    driver.execute_script("arguments[0].click();", pressione)
    time.sleep(5)

    # pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[1]/table/tbody/tr[12]/td/table/tbody/tr[4]/td/a')
    pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[1]/table/tbody/tr[11]/td/table/tbody/tr[4]/td/a')
    driver.execute_script("arguments[0].click();", pressione)
    time.sleep(5)

    driver.find_element_by_name("searchConcessionariaId").send_keys(Keys.ARROW_DOWN+Keys.SPACE)
    driver.find_element_by_name("searchEmpreiteiraId").send_keys(Keys.ARROW_DOWN+Keys.SPACE)

    lista=['Todas']

    for locais in lista:
        if  locais != '0':
            logger.info('Local: '+ locais + ' Periodo: '+p_data_i+' - '+p_data_f)

            if os.path.isfile(arquivo_download):
                os.remove(arquivo_download)

            pressione = driver.find_element_by_name("dataInicio")
            driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, p_data_i)
            pressione = driver.find_element_by_name("dataFim")
            driver.execute_script("arguments[0].setAttribute('value',arguments[1])",pressione, p_data_f)
            driver.find_element_by_name("searchLocalidadeId").send_keys(locais)
            pressione = driver.find_element(By.XPATH,'//*[@id="Table_01"]/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr[3]/td/form/table[3]/tbody/tr/td[2]/button')
            driver.execute_script("arguments[0].click();", pressione)
            time.sleep(24)

            try:
                alert = driver.switch_to_alert()
                alert.accept()
            except:
                while not os.path.isfile(arquivo_download):
                    print(arquivo_download+' ......')
                    time.sleep(1)

            logger.info("Download........ [OK] ")


    logger.info('COPEL OK')
