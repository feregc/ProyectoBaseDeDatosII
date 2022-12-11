##########################################################################################################
#Library list
import os
import pyodbc
import time
import pathlib
from pathlib import Path
import importlib
import importlib.util
import pandas as pd
import numpy as np

#################################################################################################################################
# importing log to database class from the determined location
drive = Path(__file__).drive
logger_module = ''

if os.name == 'nt':
    ConnectorSpecs = importlib.util.spec_from_file_location("Connector.py", os.path.expandvars("{}\\Users\\$USERNAME\\Desktop\\ProyectoBaseDeDatosII\\ETL\\AW-ETL\\SQL\\Connector.py".format(drive)))

Connector = importlib.util.module_from_spec(ConnectorSpecs)
ConnectorSpecs.loader.exec_module(Connector)

sqlcon = Connector.connection.sqlConnection('AdventureWorks2016')

queryPK = """
    select C.COLUMN_NAME FROM
    INFORMATION_SCHEMA.TABLE_CONSTRAINTS T
    JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C
    ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME
    WHERE
    C.TABLE_NAME='{}'
    and T.CONSTRAINT_TYPE='PRIMARY KEY' 
"""

class SQLTables():
    def __init__(self):
        global sqlcon
        self.sqlcon = sqlcon

    def pkCheck(self,connection,table):
        cursorPK = connection.cursor()
        cursorPK.execute(queryPK.format(table))
        pk = cursorPK.fetchone()
        pk = pk[0]
        connection.commit()

        return pk    

    def tableInserterMany(self,connection,table,listColumns,listValues):
        print('Running table inserter function')
        
        cursorPK = connection.cursor()
        cursorPK.execute(queryPK.format(table))
        pk = cursorPK.fetchone()
        pk = pk[0]
        connection.commit()

        sep3 = "=?, "
        sep4 = ", "

        columnsUpdate = sep3.join(listColumns)+"=?"
        columnsInsert = sep4.join(listColumns)

        parameter = ''
        for i in listColumns:
            parameter = parameter + '?,'
        parameterList =  parameter[:-1]
        
        if os.name == 'nt':

            try:

                sql = f"BEGIN TRAN \
                    UPDATE {table} WITH (serializable) set {columnsUpdate} \
                    WHERE {pk}=? \
                    IF @@rowcount = 0 \
                    BEGIN \
                        INSERT INTO {table} ({columnsInsert})  \
                        VALUES({parameterList})\
                    END\
                    COMMIT TRAN"
                cursor = connection.cursor()
                cursor.fast_executemany = True
                cursor.executemany(sql, listValues)
                connection.commit()

                print(f"Rows Upserted in {table} ",len(listValues))
            except Exception as error:
                print(error)
                pass
            finally:
                cursor.close()

    

    def tableCheckerMany(self,connection,table,column,field,listTeam):
        print('Running table checker function')
        
        sep = "','" 
        sep2 = "'),('"
        teams = "'"+sep.join(listTeam)+"'"
        teamsm = "('"+sep2.join(listTeam)+ "')"
        
        if os.name == 'nt':

            sql = f"\
                MERGE INTO {table} as Target\
                USING (SELECT * FROM (VALUES {teamsm}) AS s ({column})) AS Source\
                ON Target.{column} = Source.{column}\
                WHEN NOT MATCHED By Target THEN \
                INSERT ({column}) VALUES (Source.{column});\
            "
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()

        elif os.name == 'posix':
        
            # sql="begin tran \
            #         BEGIN IF NOT EXISTS(SELECT * FROM {} WHERE {} = {})\
            #             BEGIN INSERT INTO {} ({}) values ({})\
            #             END\
            #         END\
            #         commit tran".format(table,column,teams,table,column,teams)
            sql = f"\
                MERGE INTO {table} as Target\
                USING (SELECT * FROM (VALUES {teamsm}) AS s ({column})) AS Source\
                ON Target.{column} = Source.{column}\
                WHEN NOT MATCHED By Target THEN \
                INSERT ({column}) VALUES (Source.{column});\
            "
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()

    def tableChecker(self,connection,table,column,field):
        
        if os.name == 'nt':

            sql=f"begin tran \
                BEGIN IF NOT EXISTS(SELECT * FROM {table} WHERE {column} = ?)\
                    BEGIN INSERT INTO {table} ({column}) values (?)\
                    END\
                END\
                commit tran"
            vals = (field,field)
            cursor = connection.cursor()
            cursor.execute(sql,vals)
            connection.commit()
        elif os.name == 'posix':
        
            sql="begin tran \
                    BEGIN IF NOT EXISTS(SELECT * FROM {} WHERE {} = '{}')\
                        BEGIN INSERT INTO {} ({}) values ('{}')\
                        END\
                    END\
                    commit tran".format(table,column,field,table,column,field)
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()

    def tableChecker2(self,connection,table,column,field):
        
        if os.name == 'nt':

            sql=f"begin tran\
                    BEGIN IF NOT EXISTS(SELECT * FROM {table} WHERE {column} = ?)\
                            Select '0'\
                        Else\
                            Select '1'\
                    END\
                    commit tran"
            vals = (field)
            cursor = connection.cursor()
            cursor.execute(sql,vals)
            return cursor.fetchAll()[0][0]
        elif os.name == 'posix':
        
            sql="begin tran\
                BEGIN IF NOT EXISTS(SELECT * FROM {} WHERE {} = '{}')\
                        Select '0'\
                    Else\
                        Select '1'\
                END\
                commit tran".format(table,column,field)
            cursor = connection.cursor()
            cursor.execute(sql)
            return cursor.fetchAll()[0][0]

    def tableSearch(self,field,table):
        for record in table:
            if record[1] == field:
                return record[0]