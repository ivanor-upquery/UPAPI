#!/bin/bash
export ORACLE_SID=ORCLCDB
export ORACLE_BASE=/opt/oracle/oradata
export ORACLE_HOME=/opt/oracle/product/19c/dbhome_1
export PATH=$PATH:$ORACLE_HOME/bin
export PATH=/usr/local/bin:$PATH
/usr/bin/python3 /opt/oracle/upapi/put_ftp_finatto.py
/usr/bin/python3 /opt/oracle/upapi/put_ftp_siviero.py
