• SERVICO PYTHON 
    - iniciar servico - https://www.opentechguides.com/how-to/article/centos/169/systemd-custom-service.html
    - ETL 
        - py        - /opt/oracle/upapi/insert_data.py 
        - sh        - /usr/bin/insert_data.sh
        - servico   - etl_upquery.service

    - MSG 
        - py        - /opt/oracle/upapi/upquery_msg.py 
        - sh        - /usr/bin/msg_upquery.sh
        - servico   - msg_upquery.service

    - Logs - /opt/oracle/upapi/logs
           

     tail -f upquery_msg.log

• Senior API
    https://api.xplatform.com.br/api-portal/pt-br/tutoriais/entidades/uso-das-apis-de-entidade-com-filtros-odata
    https://dev.senior.com.br/api_privada/hcm_vacancymanagement/

• USUÁRIOS 
    -- Conta Windows - Office
        desenv01@upquery.onmicrosoft.com / Up@u7523
        ivanor@upquery.com / Ijvivi1927 
        

    -- EMAILs - UPQUERY - https://webmail.upquery.com/
       dwu@upquery.com/f62939@93UPQsx  - senha de 19/01/2023 
       ivanor@upquery.com/padrao
       desenv02@upquery.com / Up@202304

    -- Skypes    
       ivanor@upquery.com/padrao

    -- Githut – controle de versões
        login: ivanor-upquery / J@vi1927


    -- Trello 
        - Usuário master da desenv: desenv.trello/@picnic1978    - dev.trello@upquery.com/@picnic1978 
        - Apikey : 725b22fff37061095d0fd2b2fae50457
        - Token  : 9173e7ef079e26906bc0e2fe8faba68f20687bd38189ed191cdd3d94da9d37e6

        - Ivanor@upquery.com  
        - apikey : bb0c6cbaaa5aab0224999d596d0b6881 
        - Token  : 079d09bcfc67c7cec9aba8c68b071e78f90ed791b9b2b2f583308e366b5e0af5

        - desenv02@upquery.com / dev020423
        
    Lucas 
    - Email : desenv02@upquery.com / Up@202304    -  usauario: desenv02-upquery
    - Trello: desenv02@upquery.com / Up@202304
    - github: desenv02@upquery.com / Up@202304
    - BI desenv:  http://172.1.1.230:7777/desenv/DWUP3.upquery.main  lucas/lucas1234 

    SA ELETRONICOS
    fabiano@upquery.com
    cybernoid
• Python 
    -- Instalar o pacote request no python
    - Entrar na pasta do python  C:\Users\André Pereira\AppData\Local\Programs\Python\Python310>
    - Executar o comando abaixo 
    pip install requests
    pip install os_sys     
    pip install --upgrade os_sys   


• GitHub -----------------------------------------------------------------------------------------------------------------------

    Clonar Github
        git clone https://github.com/antoni-mattei/DOC.git

    Baixar alteração do Github
        git fetch
        git pull

    Enviar alterações para o GitHub
        git add .
        git commit -m “observa”
        git commit -a "observa"    -- Não precisa executar o git add antes
        git push

    Branchs
        git branch                   -- Mostra as branch existentes
        git checkout nomeBranch     -- entra na branch


    Outros 
        git status             -- mostra os arquivos alterados e ainda não subidos para o Git
        git status -s          -- Mostra o status resumido 
        git diff               -- mostra as linhas do arquivo que foram alteradas 
        git config --global user.name "Fulano de Tal"         -- Define o nome do usuário 
        git config --global user.email "you@example.com"      -- Define o email do usuário 
        git config --list       -- MOstra as configurações do Git 
        git config user.name    -- MOstra o usuário atual  
        git log --stat                -- lista os históricos de commit 
        git config --global alias.last 'log -1 HEAD'   -- Cria um comando alias para um commando, nesta caso: git log -1 HEAD , basta digitar: git last
   
    Arquivos a ignorar pelo git, parametrizar no arquivo exclude do git, exemplos:  
        *.a             # ignorar arquivos com extensão .a
        !lib.a          # mas rastrear o arquivo lib.a, mesmo que você esteja ignorando os arquivos .a acima
        /TODO           # ignorar o arquivo TODO apenas no diretório atual, mas não em subdir/TODO 
        build/          # ignorar todos os arquivos no diretório build/
        doc/*.txt       # ignorar doc/notes.txt, mas não doc/server/arch.txt
        doc/**/*.pdf    # ignorar todos os arquivos .pdf no diretório doc/


• NODE e NPM - Node Package Manager (gerenciador de pacotes padrão para Node.js) 
    NODE
      https://nodejs.org/en/download/

    NPM
      $ npm install [-g] shelljs


• VPN 
    Máquina remota Welliton 
    172.1.1.167
    user / 0Sky@2021

• Packages necessárias para o BI 
    •	Utl_Raw
    •	Utl_Encode
    •	utl_smtp – Para envio dos e-mails e chamadas http 


• Links Uteis  -----------------------------------------------------------------------------------------------------------------

    Echarts
      https://echarts.apache.org/examples/en/index.html
      https://echarts.apache.org/examples/en/editor.html?c=multiple-y-axis





• Sistemas ----------------------------------------------------------------------------------------

    BI	
    Chamado	
        http://update.upquery.com/adm/dwu.CHAMADO.main - Adm
        http://update.upquery.com/pls/dwu.CHAMADO.main  - Cliente

    PDI	
        http://172.1.1.230:7777/desenv/dwu.pdi.main?code=3EF14A5647A34D3B193420B8
        pdi_usuarios(TIPO)
	
    Rampinelle	
        http://upquery.rampinelli.com.br:7777/pls/dwu.upquery.main

    Smalt	
        http://upquery.smalticeram.com.br:7777/pls/dwu.upquery.main

    DOOR	
        http://172.1.1.230:7777/desenv/dwu.door.main
        http://172.1.1.230:7777/adm/dwu.door.main
        AUX	http://172.1.1.230:7777/desenv/dwu.aux.main
        http://172.1.1.230:7777/adm/dwu.auxi.main

    ERROR	
        http://172.1.1.230:7777/adm/dwu.error.main

    DOC	
        http://172.1.1.230:7777/desenv/dwu.doc.main

    PAINEL	
        http://cloud.upquery.com/infoshow/dwu.painel.main
        pkg PAINEL




• Oracle - Tabelas e comandos 

    USUÁRIOS / SESSÃO / JOBS 
        alter user dwu account unlock; - desbloquear usuário
        alter system kill session '988,14859';
        select 'execute dbms_job.remove('''||job||''');' from all_jobs;
        Select username,'alter system kill session '||Chr(39)||Sid||','||Serial#||Chr(39)||' IMMEDIATE;'
        select * from from v$session where status='ACTIVE' order by username asc;
        -- Identificar lock de sessões 
        select DISTINCT 'ALTER system DISCONNECT SESSION '''||s1.sid||', '||s1.serial#||''' IMMEDIATE' AS instrucao,
               S1.MACHINE, S1.PROGRAM, S1.OSUSER, S1.USERNAME, s1.sid, S1.SQL_EXEC_START, l1.*
          from v$lock l1, v$session s1
         where s1.sid    = l1.sid
           and l1.BLOCK >= 1 ;  –- Sessões que estão lockando outras
        ------
        Select dbms_metadata.get_grant_ddl(‘grant_system’,’usuário’) from dual
        SELECT dbms_metadata.get_granted_ddl( 'SYSTEM_GRANT', 'JOAO' ) FROM dual
        SELECT dbms_metadata.get_granted_ddl( 'OBJECT_GRANT', 'JOAO' ) FROM dual;
        SELECT dbms_metadata.get_granted_ddl( 'ROLE_GRANT',   'JOAO' ) FROM dual;
        --------- Lock de objeto
        select sess.*, lo.*
          from v$locked_object lo,
              dba_objects ao,
              v$session sess
        where ao.object_id  = lo.object_id
          and lo.session_id = sess.sid;
        ---Outros 
        select * from SYS.nls_database_parameters

    PDBs 
        SELECT NAME FROM v$pdbs;
        SHOW CON_NAME – mostra o container corrente
        ALTER SESSION SET CONTAINER=pdb1;
        /opt/oracle/product/19c/dbhome_1/Middleware/Oracle_WT1/instances/instance1/config/OHS/ohs1/mod_plsql/dads.conf


• WEB Server - Web Tier - Servidor Cloud  (será migrado futuramente para o ORDS)  -----------------------

    Configuração dos serviços dos clientes em cloud 
    - Arquivo dads.conf  
	    cd /u01
	      PRD    - vi ./app/oracle/product/Oracle_WT1/instances/instance1/config/OHS/ohs1/mod_plsql/dads.conf
	      DESENV - vi ./opt/oracle/product/19c/dbhome_1/Middleware/Oracle_WT1/instances/instance1/config/OHS/ohs1/mod_plsql/dads.conf

	    PRD - exemplo 
        <Location /update>
        PlsqlDatabaseConnectString     localhost:1521:upquery.localdomain ServiceNameFormat
        PlsqlNLSLanguage               AMERICAN_AMERICA.AL32UTF8
        PlsqlAuthenticationMode        Basic
        SetHandler                     pls_handler
        PlsqlDatabaseUsername          zapnews
        PlsqlDefaultPage               dwu.check_ac
        PlsqlDatabasePassword          dj3is929sksndj8338s
        Allow from all
        </Location>

    Reiniciando os serviços  
        - Entrar na pasta /opt e executar 
         ./oracle/product/19c/dbhome_1/Middleware/Oracle_WT1/instances/instance1/bin/opmnctl status
         ./oracle/product/19c/dbhome_1/Middleware/Oracle_WT1/instances/instance1/bin/opmnctl stopall
         ./oracle/product/19c/dbhome_1/Middleware/Oracle_WT1/instances/instance1/bin/opmnctl startall


    
• CHAMADO 
  ---------------------------------------------------------------------------------------------------------------
  -- Criar usuário Upquery no sistema de CHAMADO 
  ---------------------------------------------------------------------------------------------------------------
  1. Cadastrar usuário no BI com grupo SUPORTE_QUALIDADE;
  2. Cadastrar no BI (adm) em Cadastros -> Analistas, o nome reduzido deve ser o mesmo do nome do usuário e o Atedimento deve ser ANALISTA;
  3. Entrar na base adm da Upquery com DWU e:
    . Criar usuário no Oracle com mesmo nome de usuário e senha utilizados no BI 
      CREATE USER xxxxx IDENTIFIED BY xxxx;
    . Grants para acesso ao banco e a package  
      GRANT CONNECT TO xxxx;
      GRANT insert,delete,update on tab_documentos TO xxxxx;
      GRANT EXECUTE ON CHAMADO TO xxxx;
      GRANT EXECUTE ON FCL     TO xxxx;
      GRANT EXECUTE ON UPLOAD  TO xxxx;

• TvConex 
    Aparelho Raspberry para desenv e teste = 172.1.1.72    pi/picnic1978
    /home/pi/show_screens.py 
    O mesmo programa fica rodando duas vezes
      * show_screens com parametro render - fica renderizando as telas e gerando arquivos jpg
      * show_screens sem parametro - Fica pegando as imagens renderizadas/geradas e mostrando


• Linux Servidor - Comandos 
    - top 
    - top -u user                         -- monitora processos de um único usuário 
    - ps aux | grep -i oracle | more      -- MOstra processos do oracle 
    - pstree                              -- MOstra processos em forma de arvore 
    - kill -9 PID  (PID do processo)      -- Mata o processo (-9 força a morte)


Servidores:
    - Cloud 

• Serviços Python rodando nos servidores:

    Envio de email
        Instalado na cloud :  
        - arquivo INI     : /opt/oracle/upapi/upquery_msg.ini    
        - verificar status: systemctl status msg_upquery.service
        - iniciar serviço : systemctl start msg_upquery.service
        - parar serviço   : systemctl stop msg_upquery.service
    