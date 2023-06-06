import cx_Oracle
import csv

con = cx_Oracle.connect(user="dwu",password="jOn6630K85pb", dsn="siviero",encoding="UTF-8")
cur = con.cursor()

import os
for file in os.listdir("/home/siviero"):
    if  file.endswith(".txt"):
        nome = str(file)
        file = open("/home/siviero/"+file, "r") 
        csv_reader = csv.reader(file, delimiter=';')
        mes_clear='N'
        for lines in csv_reader:
#            print(lines[0]+' '+lines[1]+' '+lines[2]+' '+lines[3]+' '+lines[4])
            if  mes_clear == 'N':
                databuf=[]
                logbuf=[]
                databuf.append(lines[0][:2:1]+lines[0][3:7:1])
                databuf.append(lines[2])
                logbuf.append(nome)
                logbuf.append(lines[2])
                logbuf.append(lines[0][:2:1]+lines[0][3:7:1])
                mes_clear='Y'
                cur.execute("delete TMP_ETL_CALC_EMPREGADO where mesano = :1 and empresa = :2", databuf)

            cur.execute("insert into TMP_ETL_CALC_EMPREGADO values (substr(:1,1,2)||substr(:2,4,4),:3,:4,:5,substr(:6,1,11)||','||substr(:6,12,2))",(lines[0], lines[0], lines[1], lines[2], lines[3], lines[4], lines[4]))

        cur.execute("insert into LOG_FTP values (sysdate, :1, :2, :3)", logbuf)
        con.commit();
        os.remove("/home/siviero/"+nome)

con.commit()
con.close()

