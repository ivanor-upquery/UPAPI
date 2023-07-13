from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import calendar
import re
import shutil
import os
import time
from time import gmtime, strftime
import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

#--

option = Options()
option.add_argument("--no-sandbox");
option.add_argument('--headless')
option.add_argument('--single-process')
option.binary_location = r"/opt/google/chrome/google-chrome"
option.add_experimental_option('prefs', {
    "download.default_directory": r"/home/multiagent/arquivos/baixados", #Change default directory for downloads
"download.prompt_for_download": False, #To auto download the file
"download.directory_upgrade": True,
"plugins.always_open_pdf_externally": True #It will not show PDF directly in chrome
})

option.headless = True #Esconder o processo False/True
driver = webdriver.Chrome("/usr/bin/chromedriver", options = option)

def touch(cliente):
    touch = 'touch /home/multiagent/wait/' + cliente + '.wait'
    os.system(touch)

def nowait(cliente):
    touch = 'rm -Rf /home/multiagent/wait/' + cliente + '.wait'
    os.system(touch)

def waiting(cliente):
    while os.path.exists('/home/multiagent/wait/' + cliente + '.processing'):
          time.sleep(1)

def logar_ssw(vDominio, vCPF, vUsuario, vSenha):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0422")
    time.sleep(2)
    
    driver.find_element_by_id("1").send_keys(Keys.BACKSPACE)
    driver.find_element_by_id("1").send_keys(Keys.BACKSPACE)
    driver.find_element_by_id("1").send_keys(Keys.BACKSPACE)
    driver.find_element_by_id("1").send_keys(vDominio)
    driver.find_element_by_id("2").send_keys(vCPF)
    driver.find_element_by_id("3").send_keys(vUsuario)
    driver.find_element_by_id("4").send_keys(vSenha)
    driver.find_element_by_id("5").click()

def limpar_diretorio():
    main_folder = r'/home/multiagent/arquivos/baixados'
    for root, dirs, files in os.walk(main_folder):
        for file in files:        
            try:
                os.remove(f"{main_folder}\{file}")
            except OSError as e:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f"Error:{ e.strerror}")
            print(fdatetime.now().strftime('%Y-%m-%d %H:%M:%S'), file)

def baixar_arquivo (vRelatorio, vUsuario, vCompetencia, vDominio, vCliente, vComplementar):
    driver.get("https://sistema.ssw.inf.br/bin/ssw1440")
    time.sleep(2)
    
    status = 'Aguardando'

    while status != "Concluído" :

        try:
            driver.find_element_by_id("1").click()
            time.sleep(5)
        except:
            pass

        vPosicao = buscar_posicao_relatorio(vRelatorio, vUsuario)
        status = driver.find_element_by_xpath('//*[@id="tblsr"]/tbody/tr[' + str(vPosicao) + ']/td[7]/div').text

        if status == "Abortado" :
            break

    if status != "Abortado" :
        driver.find_element_by_xpath('//*[@id="tblsr"]/tbody/tr[' + str(vPosicao) + ']/td[9]/div/a').click()
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Arquivo baixado")
        time.sleep(10)

        renomear_arquivo(vRelatorio, vCompetencia, vDominio, vCliente, vComplementar)

def buscar_posicao_relatorio(vRelatorio, vUsuario):

    for vPosicao in range(2,20):
        sequencial = driver.find_element_by_xpath('//*[@id="tblsr"]/tbody/tr[' + str(vPosicao) + ']/td[1]/div').text
        relatorio  = driver.find_element_by_xpath('//*[@id="tblsr"]/tbody/tr[' + str(vPosicao) + ']/td[2]/div').text[0:3]
        usuario    = driver.find_element_by_xpath('//*[@id="tblsr"]/tbody/tr[' + str(vPosicao) + ']/td[4]/div').text

        if relatorio == vRelatorio and usuario == vUsuario :
            return(vPosicao)

def renomear_arquivo(vRelatorio, vCompetencia, vDominio, vCliente, vComplementar):
    main_folder = r'/home/multiagent/arquivos/baixados'

    for root, dirs, files in os.walk(main_folder):
        for file in files:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), file)
            if not re.search(r'\.py$',file):
                new_file_name = f'REL{vRelatorio}{vComplementar}_{vCompetencia}_{vDominio}.csv'
                old_file_full_name = os.path.join(root, file)
                new_file_full_name = f'/home/multiagent/arquivos/{vCliente}/' + new_file_name
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), old_file_full_name)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), new_file_full_name)

                #Verificar se o arquivo existe e exlucir ele na pasta destino
                try:
                    os.remove(new_file_full_name)
                except:
                    pass #se nao existir so passa

                #Renomear (mover) o arquivo para a pasta /home/dwu/Extracao_Dominio/Agent com o nome apropriado para o Agent do BI importa-lo

                try:
                   shutil.move(old_file_full_name, new_file_full_name)
                   break
                except OSError as e:
                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),f"Cliente: {vDominio}, Arquivo: {vRelatorio}, Error:{e.strerror}")

def relatorio_455(vDtInicio, vDtFinal, vInfComp):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0230")
    time.sleep(2)
    
    driver.find_element_by_id("2").clear()
    driver.find_element_by_id("3").clear()
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("5").clear()
    driver.find_element_by_id("7").clear()
    driver.find_element_by_id("8").clear()
    driver.find_element_by_id("9").clear()
    driver.find_element_by_id("10").clear()
    driver.find_element_by_id("11").clear()
    driver.find_element_by_id("12").clear()
    driver.find_element_by_id("11").send_keys(vDtInicio)
    driver.find_element_by_id("12").send_keys(vDtFinal)
    driver.find_element_by_id("13").clear()
    driver.find_element_by_id("14").clear()
    driver.find_element_by_id("15").clear()

    driver.find_element_by_id("18").send_keys("T")
    driver.find_element_by_id("19").send_keys("T")
    driver.find_element_by_id("20").send_keys("S")
    driver.find_element_by_id("21").clear()
    driver.find_element_by_id("21").send_keys("T")


    
    driver.find_element_by_id("22").send_keys("T")
    driver.find_element_by_id("23").send_keys("A")
    driver.find_element_by_id("25").send_keys("T")
    driver.find_element_by_id("26").send_keys("A")
    driver.find_element_by_id("27").send_keys("A")
    driver.find_element_by_id("28").send_keys("T")
    driver.find_element_by_id("29").send_keys("A")
    driver.find_element_by_id("30").send_keys("A")

    driver.find_element_by_id("32").clear()
    driver.find_element_by_id("34").clear()
    driver.find_element_by_id("35").clear()
    driver.find_element_by_id("35").send_keys("E")
    driver.find_element_by_id("37").send_keys("H")
    driver.find_element_by_id("38").send_keys("A")
    driver.find_element_by_id("39").send_keys(vInfComp)
    driver.find_element_by_id("40").click()


def relatorio_455_comp(vDtInicio, vDtFinal, vInfComp):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0230")
    time.sleep(2)

    driver.find_element_by_id("2").clear()
    driver.find_element_by_id("3").clear()
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("5").clear()
    driver.find_element_by_id("7").clear()
    driver.find_element_by_id("8").clear()
    driver.find_element_by_id("9").clear()
    driver.find_element_by_id("10").clear()
    driver.find_element_by_id("11").clear()
    driver.find_element_by_id("11").send_keys(vDtInicio)
    driver.find_element_by_id("12").clear()
    driver.find_element_by_id("12").send_keys(vDtFinal)
    driver.find_element_by_id("13").clear()
    driver.find_element_by_id("14").clear()
    driver.find_element_by_id("15").clear()

    driver.find_element_by_id("18").send_keys("T")
    driver.find_element_by_id("19").send_keys("T")
    driver.find_element_by_id("20").send_keys("S")
    driver.find_element_by_id("21").send_keys("T")

    driver.find_element_by_id("22").send_keys("T")
    driver.find_element_by_id("23").send_keys("A")
    driver.find_element_by_id("25").send_keys("T")
    driver.find_element_by_id("26").send_keys("A")
    driver.find_element_by_id("27").send_keys("A")
    driver.find_element_by_id("28").send_keys("T")
    driver.find_element_by_id("29").send_keys("A")
    driver.find_element_by_id("30").send_keys("A")

    driver.find_element_by_id("32").clear()
    driver.find_element_by_id("34").clear()
    driver.find_element_by_id("35").clear()
    driver.find_element_by_id("35").send_keys("E")
    driver.find_element_by_id("37").send_keys("D")
    driver.save_screenshot("rel455comp.png")
    driver.find_element_by_id("40").click()


def relatorio_200(vDtInicio, vDtFinal, vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0644")
    time.sleep(5)
    
    driver.find_element_by_id("1").send_keys(vDtInicio)
    driver.find_element_by_id("2").send_keys(vDtFinal)
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("11").clear()
    driver.find_element_by_id("11").send_keys("E")

    time.sleep(10)
    renomear_arquivo("200",vCompetencia, vDominio, vCliente,'')

def relatorio_435(vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0103")
    time.sleep(5)

    driver.find_element_by_id("t_data_ref_fim").clear()
    driver.find_element_by_id("t_sigla_1").clear()
    driver.find_element_by_id("t_sigla_2").clear()
    driver.find_element_by_id("t_sigla_3").clear()
    driver.find_element_by_id("t_sigla_4").clear()
    driver.find_element_by_id("t_sigla_5").clear()
    driver.find_element_by_id("t_sigla_6").clear()
    driver.find_element_by_id("t_sigla_7").clear()
    driver.find_element_by_id("t_sigla_8").clear()
    driver.find_element_by_id("t_sigla_9").clear()
    driver.find_element_by_id("t_sigla_10").clear()
    driver.find_element_by_id("t_tp_fil").send_keys("C")
    driver.find_element_by_id("t_tp_cli_fat").send_keys("T")
    driver.find_element_by_id("t_banco").clear()
    driver.find_element_by_id("t_vlr_min_ctrc").clear()
    driver.find_element_by_id("t_vlr_min_fat").clear()
    driver.find_element_by_id("t_situacao_ctrc").send_keys("I")
    driver.find_element_by_id("t_periodicidade").send_keys("T")
    driver.find_element_by_id("t_rel_lista").send_keys("T")
    driver.find_element_by_id("t_cons_bloqueados").send_keys("N")
    driver.find_element_by_id("t_cons_a_vista").send_keys("N")
    driver.find_element_by_id("t_tp_classificacao").send_keys("F")
    driver.find_element_by_id("t_excel").send_keys("S")
    driver.find_element_by_id("t_ler_morto").send_keys("N")
    driver.find_element_by_id("btn_env").click()
    
    time.sleep(10)
    renomear_arquivo("435",vCompetencia, vDominio, vCliente,'')

def relatorio_441(vDtInicio, vDtFinal, vCompetencia):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0049")
    time.sleep(2)
    
    driver.find_element_by_id("tp_arquivo").clear()
    driver.find_element_by_id("tp_arquivo").send_keys("F")
    driver.find_element_by_id("rel_ana_fg_data").clear()
    driver.find_element_by_id("rel_ana_fg_data").send_keys("E")
    driver.find_element_by_id("rel_ana_per_pesq_ini").clear()
    driver.find_element_by_id("rel_ana_per_pesq_fin").clear()
    driver.find_element_by_id("rel_ana_per_pesq_ini").send_keys(vDtInicio)
    driver.find_element_by_id("rel_ana_per_pesq_fin").send_keys(vDtFinal)
    driver.find_element_by_id("rel_ana_empresa").clear()
    driver.find_element_by_id("rel_ana_fil_1").clear()
    driver.find_element_by_id("rel_ana_fil_2").clear()
    driver.find_element_by_id("rel_ana_fil_3").clear()
    driver.find_element_by_id("rel_ana_fil_4").clear()
    driver.find_element_by_id("rel_ana_fil_5").clear()
    driver.find_element_by_id("rel_ana_cgc").clear()
    driver.find_element_by_id("rel_ana_cgc_grupo").clear()
    driver.find_element_by_id("rel_ana_class_fil").clear()
    driver.find_element_by_id("rel_ana_class_cgc").clear()
    driver.find_element_by_id("rel_ana_class_data").clear()
    driver.find_element_by_id("rel_ana_class_nro_fat").clear()
    driver.find_element_by_id("rel_ana_class_nro_fat").send_keys("X")
    driver.find_element_by_id("rel_ana_sit_fat").clear()
    driver.find_element_by_id("rel_ana_sit_fat").send_keys("T")
    driver.find_element_by_id("rel_ana_crit_adc").clear()
    driver.find_element_by_id("rel_ana_crit_adc").send_keys("T")
    driver.find_element_by_id("rel_ana_fg_fat_desc").clear()
    driver.find_element_by_id("rel_ana_fg_fat_desc").send_keys("I")
    driver.find_element_by_id("rel_ana_fg_lista_ocor").clear()
    driver.find_element_by_id("rel_ana_fg_lista_ocor").send_keys("N")
    driver.find_element_by_id("rel_ana_fg_ctrc_fob_dir").clear()
    driver.find_element_by_id("rel_ana_fg_ctrc_fob_dir").send_keys("N")
    driver.find_element_by_id("tp_cobranca").clear()
    driver.find_element_by_id("tp_cobranca").send_keys("A")
    driver.find_element_by_id("nro_banco_fat").clear()
    driver.find_element_by_id("nro_agen_fat").clear()
    driver.find_element_by_id("dig_agen_fat").clear()
    driver.find_element_by_id("nro_cc_fat").clear()
    driver.find_element_by_id("dig_cc_fat").clear()
    driver.find_element_by_id("carteira_fat").clear()
    driver.find_element_by_id("rel_ana_vlr_min_fat").clear()
    driver.find_element_by_id("rel_ana_vlr_max_fat").send_keys("9999999,99")
    driver.find_element_by_id("rel_ana_cod_vend").clear()
    driver.find_element_by_id("rel_ana_cod_ent").clear()
    driver.find_element_by_id("rel_ana_periodicidade").clear()
    driver.find_element_by_id("rel_ana_periodicidade").send_keys("T")
    driver.find_element_by_id("rel_ana_arq_excel").clear()
    driver.find_element_by_id("rel_ana_arq_excel").send_keys("S")
    driver.find_element_by_id("btn_env_rel_ana").click()

def relatorio_477(vDtInicio, vDtFinal):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0099")
    time.sleep(2)
    
    driver.find_element_by_id("nro_lancto").clear()
    driver.find_element_by_id("nro_nf").clear()
    driver.find_element_by_id("chave_cte_nfe").clear()
    driver.find_element_by_id("cod_barras_boleto").clear()
    driver.find_element_by_id("cgc_fornecedor").clear()
    driver.find_element_by_id("nro_lancto_rel").clear()
    driver.find_element_by_id("nro_nf_rel").clear()
    driver.find_element_by_id("vlr_nf").clear()
    driver.find_element_by_id("unid_pgto").clear()
    driver.find_element_by_id("gru_evento").clear()
    driver.find_element_by_id("cod_evento").clear()
    driver.find_element_by_id("mod_doc_fis").clear()
    driver.find_element_by_id("cfop_ent").clear()
    driver.find_element_by_id("bco").clear()
    driver.find_element_by_id("nro_banco_desp").clear()
    driver.find_element_by_id("nro_agen_desp").clear()
    driver.find_element_by_id("dig_agen_desp").clear()
    driver.find_element_by_id("nro_cc_desp").clear()
    driver.find_element_by_id("dig_cc_desp").clear()
    driver.find_element_by_id("nro_cheque").clear()
    driver.find_element_by_id("placa").clear()
    driver.find_element_by_id("data_ini_emissao_nota_fiscal").clear()
    driver.find_element_by_id("data_fin_emissao_nota_fiscal").clear()
    driver.find_element_by_id("data_ini_entrada_doc_fiscal").clear()
    driver.find_element_by_id("data_fin_entrada_doc_fiscal").clear()
    driver.find_element_by_id("data_ini_inclusao_despesa").clear()
    driver.find_element_by_id("data_fin_inclusao_despesa").clear()
    driver.find_element_by_id("data_ini_inclusao_despesa").send_keys(vDtInicio)
    driver.find_element_by_id("data_fin_inclusao_despesa").send_keys(vDtFinal)
    driver.find_element_by_id("data_ini_pagamento_parcela").clear()
    driver.find_element_by_id("data_fin_pagamento_parcela").clear()
    driver.find_element_by_id("data_ini_vencimento_parcela").clear()
    driver.find_element_by_id("data_fin_vencimento_parcela").clear()
    driver.find_element_by_id("data_ini_caixa").clear()
    driver.find_element_by_id("data_fin_caixa").clear()
    driver.find_element_by_id("mes_comp").clear()
    driver.find_element_by_id("sit_desp").clear()
    driver.find_element_by_id("sit_desp").send_keys("T")
    driver.find_element_by_id("sit_arq").clear()
    driver.find_element_by_id("sit_arq").send_keys("T")
    driver.find_element_by_id("link_excel").click()

def relatorio_076(vDtInicio, vDtFinal, vUnidade):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0216")
    time.sleep(2)
    
    driver.find_element_by_id("2").send_keys(vUnidade)
    driver.find_element_by_id("3").clear()
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("3").send_keys(vDtInicio)
    driver.find_element_by_id("4").send_keys(vDtFinal)
    driver.find_element_by_id("6").clear()
    driver.find_element_by_id("7").clear()
    driver.find_element_by_id("7").send_keys("S")
    
    #driver.find_element_by_id("8").click()

def relatorio_046(vCompetencia, vDominio, vCliente) :
    driver.get("https://sistema.ssw.inf.br/bin/ssw0062")
    time.sleep(2)

    driver.find_element_by_id("1").send_keys("A")
    driver.find_element_by_id("2").clear()
    driver.find_element_by_id("3").clear()
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("5").clear()
    driver.find_element_by_id("6").clear()
    driver.find_element_by_id("6").send_keys("S")
    #driver.find_element_by_id("7").click()

    time.sleep(30)
    renomear_arquivo("046",vCompetencia, vDominio, vCliente,'')

def relatorio_073(vDtInicio, vDtFinal, vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0332")
    time.sleep(2)

    driver.find_element_by_id("ser_ctrb_os").clear()
    driver.find_element_by_id("nro_ctrb_os").clear()
    driver.find_element_by_id("per_ini_inc").clear()
    driver.find_element_by_id("per_fin_inc").clear()
    driver.find_element_by_id("per_ini_inc").send_keys(vDtInicio)
    driver.find_element_by_id("per_fin_inc").send_keys(vDtFinal)
    driver.find_element_by_id("tp_propriedade").clear()
    driver.find_element_by_id("tp_propriedade").send_keys("T")
    driver.find_element_by_id("tp_ctrb_os").clear()
    driver.find_element_by_id("tp_ctrb_os").send_keys("C")
    driver.find_element_by_id("fg_ctrb_os").clear()
    driver.find_element_by_id("fg_ctrb_os").send_keys("A")
    driver.find_element_by_id("fg_cancelados").clear()
    driver.find_element_by_id("fg_cancelados").send_keys("T")
    driver.find_element_by_id("unid_orig").clear()
    driver.find_element_by_id("placa_cav").clear()
    driver.find_element_by_id("cgc_prop").clear()
    driver.find_element_by_id("cpf_mot").clear()
    driver.find_element_by_id("ciot").clear()
    driver.find_element_by_id("link_gera_excel").click()

    time.sleep(5)
    renomear_arquivo("073",vCompetencia,vDominio, vCliente,'')

def relatorio_916(vDtInicio, vDtFinal, vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0286")
    time.sleep(2)

    driver.find_element_by_id("1").send_keys(vDtInicio)
    driver.find_element_by_id("2").send_keys(vDtFinal)
    driver.find_element_by_id("8").click()

    time.sleep(5)
    renomear_arquivo("916",vCompetencia,vDominio, vCliente,'')

def relatorio_548(vDtInicio, vDtFinal, vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0739")
    time.sleep(2)

    driver.find_element_by_id("3").send_keys(vDtInicio)
    driver.find_element_by_id("4").send_keys(vDtFinal)
    driver.find_element_by_id("6").send_keys("5")
    driver.find_element_by_id("21").send_keys("N")
    driver.find_element_by_id("total_dia").send_keys("N")
    driver.find_element_by_id("22").send_keys("N")
    driver.find_element_by_id("23").send_keys("N")
    driver.find_element_by_id("24").send_keys("N")
    driver.find_element_by_id("25").send_keys("S")
    driver.find_element_by_id("26").send_keys("1")
    driver.find_element_by_id("27").send_keys("1")
    driver.find_element_by_id("28").click()

    time.sleep(5)
    renomear_arquivo("548",vCompetencia,vDominio, vCliente,'')

def relatorio_411(vDtInicio, vDtFinal, vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0309")
    time.sleep(2)

    driver.find_element_by_id("4").send_keys(vDtInicio)
    driver.find_element_by_id("5").send_keys(vDtFinal)
    driver.find_element_by_id("nro_fatura").clear()
    driver.find_element_by_id("2").clear()
    driver.find_element_by_id("3").clear()
    driver.find_element_by_id("7").clear()
    driver.find_element_by_id("9").clear()
    driver.find_element_by_id("10").clear()
    driver.find_element_by_id("10").send_keys("T")
    driver.find_element_by_id("11").clear()
    driver.find_element_by_id("11").send_keys("T")
    driver.find_element_by_id("14").click()

    time.sleep(20)
    renomear_arquivo("411",vCompetencia,vDominio, vCliente,'')

def relatorio_036(vDtInicial, vDtFinal, vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0146")
    time.sleep(2)

    driver.find_element_by_id("5").click()
    driver.find_element_by_id("5").clear()
    driver.find_element_by_id("5").click()
    driver.find_element_by_id("5").send_keys(vDtInicial)
    driver.find_element_by_id("6").send_keys(vDtFinal)
    driver.find_element_by_id("11").click()
    driver.find_element_by_id("11").clear()
    driver.find_element_by_id("11").send_keys("S")

    time.sleep(45)
    renomear_arquivo("036",vCompetencia,vDominio, vCliente,'')

def relatorio_455_manif(vDtInicial, vDtFinal, vInfComp):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0230")
    time.sleep(2)

    driver.find_element_by_id("2").clear()
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("5").clear()
    driver.find_element_by_id("7").clear()
    driver.find_element_by_id("9").clear()
    driver.find_element_by_id("10").clear()
    driver.find_element_by_id("11").clear()
    driver.find_element_by_id("12").clear()
    driver.find_element_by_id("11").send_keys(vDtInicial)
    driver.find_element_by_id("12").send_keys(vDtFinal)
    driver.find_element_by_id("35").clear()
    driver.find_element_by_id("35").send_keys("E")
    driver.find_element_by_id("37").send_keys("A")
    driver.find_element_by_id("40").click()

def relatorio_455_result(vDtInicio, vDtFinal, vInfComp):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0230")
    time.sleep(2)

    driver.find_element_by_id("2").clear()
    driver.find_element_by_id("3").clear()
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("5").clear()
    driver.find_element_by_id("7").clear()
    driver.find_element_by_id("8").clear()
    driver.find_element_by_id("9").clear()
    driver.find_element_by_id("10").clear()
    driver.find_element_by_id("11").clear()
    driver.find_element_by_id("12").clear()
    driver.find_element_by_id("11").send_keys(vDtInicio)
    driver.find_element_by_id("12").send_keys(vDtFinal)
    driver.find_element_by_id("13").clear()
    driver.find_element_by_id("14").clear()
    driver.find_element_by_id("15").clear()
    driver.find_element_by_id("18").send_keys("T")
    driver.find_element_by_id("19").send_keys("T")
    driver.find_element_by_id("20").send_keys("S")
    driver.find_element_by_id("21").clear()
    driver.find_element_by_id("21").send_keys("T")
    driver.find_element_by_id("22").send_keys("T")
    driver.find_element_by_id("23").send_keys("A")
    driver.find_element_by_id("25").send_keys("T")
    driver.find_element_by_id("26").send_keys("A")
    driver.find_element_by_id("27").send_keys("A")
    driver.find_element_by_id("28").send_keys("T")
    driver.find_element_by_id("29").send_keys("A")
    driver.find_element_by_id("30").send_keys("A")
    driver.find_element_by_id("32").clear()
    driver.find_element_by_id("34").clear()
    driver.find_element_by_id("35").clear()
    driver.find_element_by_id("35").send_keys("E")

    driver.find_element_by_id("37").send_keys("G")
    driver.find_element_by_id("40").click()

def relatorio_455_bdk(vDtInicio, vDtFinal, vInfComp):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0230")
    time.sleep(2)

    driver.find_element_by_id("2").clear()
    driver.find_element_by_id("3").clear()
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("5").clear()
    driver.find_element_by_id("7").clear()
    driver.find_element_by_id("8").clear()
    driver.find_element_by_id("9").clear()
    driver.find_element_by_id("10").clear()
    driver.find_element_by_id("11").clear()
    driver.find_element_by_id("12").clear()
    driver.find_element_by_id("11").send_keys(vDtInicio)
    driver.find_element_by_id("12").send_keys(vDtFinal)
    driver.find_element_by_id("13").clear()
    driver.find_element_by_id("14").clear()
    driver.find_element_by_id("15").clear()
    driver.find_element_by_id("18").send_keys("T")
    driver.find_element_by_id("19").send_keys("T")
    driver.find_element_by_id("20").send_keys("S")
    driver.find_element_by_id("21").clear()
    driver.find_element_by_id("21").send_keys("T")
    driver.find_element_by_id("22").send_keys("T")
    driver.find_element_by_id("23").send_keys("A")
    driver.find_element_by_id("25").send_keys("T")
    driver.find_element_by_id("26").send_keys("A")
    driver.find_element_by_id("27").send_keys("A")
    driver.find_element_by_id("28").send_keys("T")
    driver.find_element_by_id("29").send_keys("A")
    driver.find_element_by_id("30").send_keys("A")
    driver.find_element_by_id("32").clear()
    driver.find_element_by_id("34").clear()
    driver.find_element_by_id("35").clear()
    driver.find_element_by_id("35").send_keys("E")

    driver.find_element_by_id("37").send_keys("B")
    driver.find_element_by_id("38").send_keys("D")
    driver.find_element_by_id("39").send_keys("K")
    driver.find_element_by_id("40").click()

def relatorio_455_c(vDtInicio, vDtFinal, vInfComp):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0230")
    time.sleep(2)

    driver.find_element_by_id("2").clear()
    driver.find_element_by_id("3").clear()
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("5").clear()
    driver.find_element_by_id("7").clear()
    driver.find_element_by_id("8").clear()
    driver.find_element_by_id("9").clear()
    driver.find_element_by_id("10").clear()
    driver.find_element_by_id("11").clear()
    driver.find_element_by_id("12").clear()
    driver.find_element_by_id("11").send_keys(vDtInicio)
    driver.find_element_by_id("12").send_keys(vDtFinal)
    driver.find_element_by_id("13").clear()
    driver.find_element_by_id("14").clear()
    driver.find_element_by_id("15").clear()
    driver.find_element_by_id("18").send_keys("T")
    driver.find_element_by_id("19").send_keys("T")
    driver.find_element_by_id("20").send_keys("S")
    driver.find_element_by_id("21").clear()
    driver.find_element_by_id("21").send_keys("T")
    driver.find_element_by_id("22").send_keys("T")
    driver.find_element_by_id("23").send_keys("A")
    driver.find_element_by_id("25").send_keys("T")
    driver.find_element_by_id("26").send_keys("A")
    driver.find_element_by_id("27").send_keys("A")
    driver.find_element_by_id("28").send_keys("T")
    driver.find_element_by_id("29").send_keys("A")
    driver.find_element_by_id("30").send_keys("A")
    driver.find_element_by_id("32").clear()
    driver.find_element_by_id("34").clear()
    driver.find_element_by_id("35").clear()
    driver.find_element_by_id("35").send_keys("E")

    driver.find_element_by_id("37").send_keys("C")
    driver.find_element_by_id("40").click()


def relatorio_056(vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0082")
    time.sleep(2)
    driver.find_element_by_id("11").click()
    time.sleep(10)
    renomear_arquivo("056",vCompetencia,vDominio, vCliente,'')

def relatorio_583(vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0662")
    time.sleep(2)
    driver.find_element_by_xpath('//*[@id="frm"]/a[@id="6"]').click()
    time.sleep(10)
    renomear_arquivo("583",vCompetencia,vDominio, vCliente,'')

def relatorio_524(vDtInicial, vDtFinal, vInfComp):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0585")
    time.sleep(2)
    driver.find_element_by_id("t_unidade").clear()
    driver.find_element_by_id("t_tp_uni").send_keys("E")
    driver.find_element_by_id("t_data_ini").send_keys(vDtInicial)
    driver.find_element_by_id("t_data_fin").send_keys(vDtFinal)
    driver.find_element_by_id("t_tp_arquivo").send_keys("N")
    driver.find_element_by_id("t_excel").send_keys("S")
    driver.find_element_by_id("2").click()

def relatorio_150(vDtInicial, vDtFinal, vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0861")
    time.sleep(2)
    driver.find_element_by_id("1").send_keys(vDtInicial)
    driver.find_element_by_id("2").send_keys(vDtFinal)
    driver.find_element_by_id("4").clear()
    driver.find_element_by_id("6").clear()
    driver.find_element_by_id("7").send_keys("R")
    driver.find_element_by_id("8").send_keys("S")
    driver.find_element_by_id("9").send_keys("S")
    # driver.find_element_by_id("10").click()
    time.sleep(10)
    renomear_arquivo("150",vCompetencia,vDominio, vCliente,'')

def relatorio_103(vDtInicial, vDtFinal, vCompetencia, vDominio, vCliente):
    driver.get("https://sistema.ssw.inf.br/bin/ssw0166")
    time.sleep(2)
    driver.find_element_by_id("14").send_keys(vDtInicial)
    driver.find_element_by_id("15").send_keys(vDtFinal)
    driver.find_element_by_id("16").send_keys("I")
    driver.find_element_by_id("17").send_keys("E")
    driver.find_element_by_id("20").click()
    time.sleep(10)
    renomear_arquivo("103",vCompetencia,vDominio, vCliente,'')



def main_function(vDominio, vCpf, vUsuario, vSenha, vUnidade, vCompetencia, vDtInicial, vDtFinal, vDtRel073, vDtRel076, vDtRel916, vDtRel441, vTipo, vCliente, vInfComp):

    logar_ssw(vDominio,vCpf,vUsuario, vSenha)
    time.sleep(5)

    if vTipo in ("both","scheduler") :

        try:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 455 {vCliente}')
            relatorio_455(vDtInicial,vDtFinal, vInfComp) #precisa acessar a tela 156 para baixar o arquivo
            time.sleep(5)
            baixar_arquivo("455",vUsuario,vCompetencia,vDominio, vCliente, '')
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 455 {vCliente}')
        except:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 455 {vCliente}')

        try:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 411 {vCliente}')
            relatorio_411(vDtInicial,vDtRel073,vCompetencia,vDominio, vCliente) #não precisa acessar a tela 156 para baixar o arquivo
            time.sleep(5)
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 411 {vCliente}')
        except:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 411 {vCliente}')


        try:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 073 {vCliente}')
            relatorio_073(vDtInicial,vDtRel073,vCompetencia,vDominio, vCliente) #baixa automaticamente na propria tela 073
            time.sleep(5)
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 073 {vCliente}')
        except:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 073 {vCliente}')

        if vDominio in ("EFX") :
            
            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 076 EFXPOA {vCliente}')
                relatorio_076(vDtInicial,vDtRel076, "POA") #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("076",vUsuario,vCompetencia,"EFXPOA", vCliente, '')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 076 EFXPOA {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 076 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 076 EFXGRU {vCliente}')
                relatorio_076(vDtInicial,vDtRel076, "SAO") #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("076",vUsuario,vCompetencia,"EFXGRU", vCliente,'')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 076 EFXGRU {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 076 {vCliente}')


            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 200 {vCliente}')
                relatorio_200(vDtInicial,vDtFinal,vCompetencia, vDominio, vCliente) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 200 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 200 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 916 {vCliente}')
                relatorio_916(vDtInicial, vDtRel916, vCompetencia, vDominio, vCliente) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("916",vUsuario,vCompetencia,vDominio, vCliente,'')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 916 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 916 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 548 EFXPOA {vCliente}')
                relatorio_548(vDtInicial, vDtFinal, vCompetencia, vDominio, vCliente) #baixa automaticamente na propria tela 548
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 548 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 548 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 435 EFXPOA {vCliente}')
                relatorio_435(vCompetencia, vDominio, vCliente) #baixa automaticamente na propria tela 435
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 435 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 435 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 455 complementar {vCliente}')
                relatorio_455_comp(vDtInicial,vDtFinal, vInfComp) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("455",vUsuario,vCompetencia,vDominio, vCliente,'comp_1')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 455 complementar {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 455 complementar {vCliente}')

        elif vDominio in ("MOL") :

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 076 GRU {vCliente}')
                relatorio_076(vDtInicial,vDtRel076, "GRU") #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("076",vUsuario,vCompetencia,"GRU", vCliente, '')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 076 GRU {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 076 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 076 BRU {vCliente}')
                relatorio_076(vDtInicial,vDtRel076, "BRU") #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("076",vUsuario,vCompetencia,"BRU", vCliente, '')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 076 BRU {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 076 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 076 SRP {vCliente}')
                relatorio_076(vDtInicial,vDtRel076, "SRP") #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("076",vUsuario,vCompetencia,"SRP", vCliente, '')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 076 SRP {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 076 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 076 SJC {vCliente}')
                relatorio_076(vDtInicial,vDtRel076, "SJC") #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("076",vUsuario,vCompetencia,"SJC", vCliente, '')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 076 SJC {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 076 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 076 RPO {vCliente}')
                relatorio_076(vDtInicial,vDtRel076, "RPO") #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("076",vUsuario,vCompetencia,"RPO", vCliente, '')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 076 RPO {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 076 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 076 CPS {vCliente}')
                relatorio_076(vDtInicial,vDtRel076, "CPS") #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("076",vUsuario,vCompetencia,"CPS", vCliente, '')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 076 CPS {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 076 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 200 {vCliente}')
                relatorio_200(vDtInicial,vDtFinal,vCompetencia, vDominio, vCliente) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 200 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 200 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 916 {vCliente}')
                relatorio_916(vDtInicial, vDtRel916, vCompetencia, vDominio, vCliente) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("916",vUsuario,vCompetencia,vDominio, vCliente,'')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 916 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 916 {vCliente}')
            
            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 548 {vCliente}')
                relatorio_548(vDtInicial, vDtFinal, vCompetencia, vDominio, vCliente) #baixa automaticamente na propria tela 548
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 548 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 548 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 435 {vCliente}')
                relatorio_435(vCompetencia, vDominio, vCliente) #baixa automaticamente na propria tela 435
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 435 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 435 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 455 complementar {vCliente}')
                relatorio_455_comp(vDtInicial,vDtFinal, vInfComp) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("455",vUsuario,vCompetencia,vDominio, vCliente,'comp_1')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 455 complementar {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 455 complementar {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 036 {vCliente}')
                relatorio_036(vDtInicial,vDtRel073, vCompetencia, vDominio, vCliente) #baixa automaticamente na propria tela 036
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 036 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 036 {vCliente}')
           
            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 455 manif {vCliente}')
                relatorio_455_manif(vDtInicial, vDtFinal, vInfComp) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("455",vUsuario,vCompetencia,vDominio, vCliente,'manif')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 455 manif {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 455 manif {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 056 {vCliente}')
                relatorio_056(vCompetencia, vDominio, vCliente) #baixa automaticamente na propria tela 036
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 056 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 056 {vCliente}')

        elif vDominio in ("DMN"):
            
            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 076 {vCliente}')
                relatorio_076(vDtInicial,vDtRel076, vUnidade) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("076",vUsuario,vCompetencia,vDominio, vCliente,'')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 076 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 076 {vCliente}')

        elif vDominio in ("EMI"):

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 455 G-Resultados {vCliente}')
                relatorio_455_result(vDtInicial,vDtFinal, vInfComp) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("455",vUsuario,vCompetencia,vDominio, vCliente, 'result')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 455 G-Resultados {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 455 G-Resultados {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 455 B+D+K {vCliente}')
                relatorio_455_bdk(vDtInicial,vDtFinal, vInfComp) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("455",vUsuario,vCompetencia,vDominio, vCliente, 'bdk')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 455 B+D+K {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 455 B+D+K {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 455 C (Compr.Entrega)  {vCliente}')
                relatorio_455_c(vDtInicial,vDtFinal, vInfComp) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("455",vUsuario,vCompetencia,vDominio, vCliente, 'c')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 455 C (Compr.Entrega) {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 455 C (Compr.Entrega) {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 200 {vCliente}')
                relatorio_200(vDtInicial,vDtFinal,vCompetencia, vDominio, vCliente) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 200 {vCliente}')
            except:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 200 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 524 Resultados {vCliente}')
                relatorio_524(vDtInicial,vDtFinal, vInfComp) #precisa acessar a tela 156 para baixar o arquivo
                time.sleep(5)
                baixar_arquivo("524",vUsuario,vCompetencia,vDominio, vCliente, '')
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 524 Resultados {vCliente}')
            except Exception as e:
                print(str(e)[0:3000])
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 524 Resultados {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 150 {vCliente}')
                relatorio_150(vDtInicial,vDtFinal, vCompetencia, vDominio, vCliente) #baixa automaticamente na propria tela
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 150 {vCliente}')
            except Exception as e:
                print(str(e)[0:3000])
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 150 {vCliente}')

            try:
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 103 {vCliente}')
                relatorio_103(vDtInicial,vDtFinal, vCompetencia, vDominio, vCliente) #baixa automaticamente na propria tela
                time.sleep(5)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 103 {vCliente}')
            except Exception as e:
                print(str(e)[0:3000])
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 103 {vCliente}')


    if vTipo in ("both","full") :
        
        try:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 441 {vCliente}')
            relatorio_441(vDtRel441,vDtFinal,vCompetencia) #baixa automaticamente na propria tela 441
            time.sleep(15)
            baixar_arquivo("441",vUsuario,vCompetencia,vDominio, vCliente,'')
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 441 {vCliente}')
        except:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 441 {vCliente}')

        try:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 477 {vCliente}')
            relatorio_477("010121",vDtFinal) #precisa acessar a tela 156 para baixar o arquivo
            time.sleep(15)
            baixar_arquivo("477",vUsuario,vCompetencia,vDominio, vCliente,'')
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 477 {vCliente}')
        except:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 477 {vCliente}')

        try:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 046 {vCliente}')
            relatorio_046(vCompetencia,vDominio, vCliente) #baixa automaticamente na propria tela 046
            time.sleep(15)
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 046 {vCliente}')
        except:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 046 {vCliente}')

        try:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Inicio Rel 583 {vCliente}')
            relatorio_583(vCompetencia,vDominio, vCliente) #baixa automaticamente na propria tela 046
            time.sleep(15)
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Fim Rel 583 {vCliente}')
        except Exception as e:
            print(str(e)[0:3000])
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f'Erro Rel 583 {vCliente}')
           


    
print("....")
#Preparar as datas iniciais e finais e o parametro competencia (Mes Atual)
prmCompeAtu = date.today().strftime('%Y%m')
prmDtIniAtu = '01' + date.today().strftime('%m%y')
prmDtFinAtu = (date.today().replace(day=calendar.monthrange(date.today().year,date.today().month)[1])).strftime('%d%m%y')
prmDtIniAtuRel441 = (date.today() - timedelta(days=330)).strftime('%d%m%y')
prmDtFinAtuRel076 = (date.today() - timedelta(days=3)).strftime('%d%m%y')
prmDtFinAtuRel916 = (date.today() - timedelta(days=1)).strftime('%d%m%y')
prmDtFinAtuRel073 = date.today().strftime('%d%m%y')
#________________________________________________________________________________________________#

#Preparar as datas iniciais e finais e o parametro competencia (Mes Anterior)
#calendar.monthrange retorna o ultimo dia do anomes e requer os parametros (year,month)
#relativedelta(months=-1) retorno o mes anterior
prmCompeAnt = (date.today() - relativedelta(months=1)).strftime('%Y%m')
prmDtIniAnt = '01' + (date.today() - relativedelta(months=1)).strftime('%m%y')
prmDtFinAnt = (date.today() - relativedelta(months=1)).replace(day=calendar.monthrange((date.today() - relativedelta(months=1)).year, (date.today() - relativedelta(months=1)).month)[1]).strftime('%d%m%y')
prmDtFinAntRel076 = (date.today() - relativedelta(months=1)).replace(day=calendar.monthrange((date.today() - relativedelta(months=1)).year, (date.today() - relativedelta(months=1)).month)[1]).strftime('%d%m%y')
prmDtFinAntRel073 = (date.today() - relativedelta(months=1)).replace(day=calendar.monthrange((date.today() - relativedelta(months=1)).year, (date.today() - relativedelta(months=1)).month)[1]).strftime('%d%m%y')
#________________________________________________________________________________________________#

# Inciar extracoes
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'começou')

#waiting('000000080')
#touch('000000080')
#main_function('DMN', '09319645981', 'upquery', '052222', 'PIN', prmCompeAtu, prmDtIniAtu, prmDtFinAtu, prmDtFinAtuRel073, prmDtFinAtuRel076, prmDtFinAtuRel916, prmDtIniAtuRel441,"both","000000080","")
#main_function('DMN', '09319645981', 'upquery', '052222', 'PIN', prmCompeAnt, prmDtIniAnt, prmDtFinAnt, prmDtFinAntRel073, prmDtFinAntRel076, prmDtFinAtuRel916, prmDtIniAtuRel441,"scheduler","000000080","")
#nowait('000000080')

waiting('000000110')
touch('000000110')
main_function('EMI', '09591830947', 'upquery', '123456', 'PIN', prmCompeAtu, prmDtIniAtu, prmDtFinAtu, prmDtFinAtuRel073, prmDtFinAtuRel076, prmDtFinAtuRel916, prmDtIniAtuRel441,"both","000000110","")
main_function('EMI', '09591830947', 'upquery', '123456', 'PIN', prmCompeAnt, prmDtIniAnt, prmDtFinAnt, prmDtFinAntRel073, prmDtFinAntRel076, prmDtFinAtuRel916, prmDtIniAtuRel441,"scheduler","000000110","")
nowait('000000110')


#waiting('000000084')
#touch('000000084')
#main_function('EFX', '95392572049', 'upquery', 'upeffex', 'POA', prmCompeAtu, prmDtIniAtu, prmDtFinAtu, prmDtFinAtuRel073, prmDtFinAtuRel076, prmDtFinAtuRel916, prmDtIniAtuRel441,"both","000000084","B")
#main_function('EFX', '95392572049', 'upquery', 'upeffex', 'POA', prmCompeAnt, prmDtIniAnt, prmDtFinAnt, prmDtFinAntRel073, prmDtFinAntRel076, prmDtFinAnt, prmDtIniAtuRel441,"scheduler","000000084","B")
#nowait('000000084')

#waiting('000000090')
#touch('000000090')
#main_function('MOL', '18816058846', 'robobi', 'mosca21', 'POA', prmCompeAtu, prmDtIniAtu, prmDtFinAtu, prmDtFinAtuRel073, prmDtFinAtuRel076, prmDtFinAtuRel916, prmDtIniAtuRel441,"both","000000090","B")
#main_function('MOL', '18816058846', 'robobi', 'mosca21', 'POA', prmCompeAnt, prmDtIniAnt, prmDtFinAnt, prmDtFinAntRel073, prmDtFinAntRel076, prmDtFinAnt, prmDtIniAtuRel441,"scheduler","000000090","B")
#nowait('000000090')
#______________________________________________________________________________________________#
driver.quit()
