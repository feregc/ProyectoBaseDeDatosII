
import unicodedata
import requests
import json
import base64
import pyodbc
import os

class connection:

  def sqlConnection(account):
        #MSSQL DATABASE CONNECTOR
    if os.name == 'nt':
        opsys = 'Windows'
    elif os.name == 'posix':
        opsys = 'Linux'

    if opsys == 'Windows':
        connectionStringW = "Driver={ODBC Driver 17 for SQL Server};" + "Server=10.0.5.95\MSSQLSERVER2016; Database={}; uid=proyecto_admin; pwd=admin123; autocommit=true".format(account)
        sqlcon = pyodbc.connect(connectionStringW)
    elif opsys == 'Linux':
        connectionStringL = "DRIVER={ODBC Driver 17 for SQL Server};" + "Server=10.0.5.95\MSSQLSERVER2016; Database={}; UID=proyecto_admin; pwd=admin123; autocommit=true".format(account)
        sqlcon = pyodbc.connect(connectionStringL)
    return sqlcon
