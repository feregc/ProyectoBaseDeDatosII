import pyodbc
import os

class connection:

  def sqlConnection(account):
        #MSSQL DATABASE CONNECTOR
    if os.name == 'nt':
        opsys = 'Windows'
    elif os.name == 'posix':
        opsys = 'Linux'

    #Connecting Queries usingg ODBC Driver 17; update connection strings accoring to your server and drivers
    if opsys == 'Windows':
        connectionStringW = "Driver={ODBC Driver 17 for SQL Server};" + "Server=10.0.5.95\MSSQLSERVER2016; Database={}; uid=proyecto_admin; pwd=admin123; autocommit=true".format(account)
        sqlcon = pyodbc.connect(connectionStringW)
    elif opsys == 'Linux':
        connectionStringL = "DRIVER={ODBC Driver 17 for SQL Server};" + "Server=10.0.5.95\MSSQLSERVER2016; Database={}; UID=proyecto_admin; pwd=admin123; autocommit=true".format(account)
        sqlcon = pyodbc.connect(connectionStringL)
    return sqlcon
