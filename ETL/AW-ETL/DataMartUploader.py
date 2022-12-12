#!/usr/bin/env python
# coding: utf-8

##########################################################################################################
#Library list

#Module for ODBC Connection to MSSQLServer 2016
import pyodbc

#data Handling Modules Pandas and Numpy.
import pandas as pd
import numpy as np

#General Modules for System handles and Custom Classes Loads
import os
from pathlib import Path
import importlib.util

#SQL Comprehension module for SQLQueries
import sql_metadata

#Custom Built Class for SQL Data Upserts and Other Processes
from SQL.sqlTables2 import SQLTables

#############################################################################################################
#Loading Custom Built ODBC Connector Class
drive = Path(__file__).drive

if os.name == 'nt':
    ConnectorSpecs = importlib.util.spec_from_file_location("Connector.py", os.path.expandvars("{}\\Users\\$USERNAME\\Desktop\\ProyectoBaseDeDatosII\\ETL\\AW-ETL\\SQL\\Connector.py".format(drive)))

Connector = importlib.util.module_from_spec(ConnectorSpecs)
ConnectorSpecs.loader.exec_module(Connector)
#############################################################################################################
#MSSQL DATABASE CONNECTOR
#ODBC Connectors usin Instance of Loaded Class
sqlcon = Connector.connection.sqlConnection('AdventureWorks2016')
sqlcon2 = Connector.connection.sqlConnection('ProyectoBIIV3')

#############################################################################################################
#Variables for SQL Queries to Extract Data from AdventureWorks2016 Data
selectProductSubCategory = "SELECT [ProductSubcategoryID] as idProductSubCategory,[ProductCategoryID] as idProductCategory,[Name] FROM [AdventureWorks2016].[Production].[ProductSubcategory]"
selectProductCategory = "SELECT [ProductCategoryID] as idProductCategory,[Name] as nameProductCategory FROM [AdventureWorks2016].[Production].[ProductCategory]"
selectProducts = "SELECT [ProductID] as idProduct,[Name] as nameProduct,[ListPrice] as listPrice,[StandardCost] as standardCost,[ProductSubcategoryID] as idProductSubCategory FROM [AdventureWorks2016].[Production].[Product]"
selectCountryRegion = "SELECT [CountryRegionCode] as idCountryRegion,[Name] as nameCountryRegion FROM [AdventureWorks2016].[Person].[CountryRegion]"
selectStateProvince = "SELECT [StateProvinceID] as idStateProvince ,[Name] as nameStateProvince,[CountryRegionCode] as idCountryRegion ,[TerritoryID] as idTerritory FROM [AdventureWorks2016].[Person].[StateProvince]"
selectTerritory = "SELECT [TerritoryID] as idTerritory ,[CountryRegionCode] as idCountryRegion,[Name] as name,[Group] as territoryGroup FROM [AdventureWorks2016].[Sales].[SalesTerritory]"

selectLocation = """
    SELECT [AddressID] as idLocation
      ,[AddressLine1] + ' ' + isnull([AddressLine2],'') as [address]
      ,[City] as city
      ,[PostalCode] as postalCode
      ,[StateProvinceID] as idStateProvince
  FROM [AdventureWorks2016].[Person].[Address]
"""

selectCustomer = """
    SELECT [CustomerID] as idCustomer
	  ,[FirstName] as firstName
      ,[LastName] as lastName
	  ,[AddressID] as idLocation
  FROM [AdventureWorks2016].[Sales].[Customer] as c
  LEFT JOIN Person.Person as pp on pp.BusinessEntityID = c.PersonID
  LEFT JOIN Person.BusinessEntityAddress as bea on pp.BusinessEntityID = bea.BusinessEntityID
"""

selectStore = """
    SELECT s.[BusinessEntityID] as idStore
		,bea.AddressID as idLocation
      ,[Name] as nameStore
      ,[SalesPersonID] as idEmployee
  FROM [AdventureWorks2016].[Sales].[Store] as s
  LEFT JOIN Person.BusinessEntityAddress as bea on s.BusinessEntityID = bea.BusinessEntityID
"""

selectSalesPerson = """
    SELECT sp.[BusinessEntityID] as idEmployee
        ,hre.[NationalIDNumber] as dni
		,pp.[FirstName] as firstName
      ,pp.[LastName] as lastName
      
	  ,hre.[Gender] as gender
	  ,hre.[HireDate] as hireDate

      
  FROM [AdventureWorks2016].[Sales].[SalesPerson] as sp
  LEFT JOIN [HumanResources].[Employee] as hre on hre.BusinessEntityID = sp.BusinessEntityID
  LEFT JOIN Person.Person as pp on pp.BusinessEntityID = sp.BusinessEntityID
"""

selectSalesTaxes = """
     SELECT	
        [SalesOrderID] as idSale
        ,[TaxAmt] as taxAmount
        ,[Freight] as freight
  FROM [AdventureWorks2016].[Sales].[SalesOrderHeader]
"""
#Fact Table 
selectSales = """
    SELECT	
        sod.SalesOrderDetailID as idSalesDetail
		,soh.[SalesOrderID] as idSale
		,[CustomerID] as idCustomer
      ,[SalesPersonID] as idEmployee
      ,[OrderDate] as idDate
      ,[TerritoryID] as idTerritory
      ,[ShipToAddressID] as idLocation
	  ,sod.[ProductID] as idProduct
      ,sod.OrderQty as orderQuantity
	  ,sod.UnitPrice as unitPrice

  FROM [AdventureWorks2016].[Sales].[SalesOrderHeader] as soh
  LEFT JOIN [Sales].[SalesOrderDetail] as sod on sod.SalesOrderID = soh.SalesOrderID
"""
#######################################################################################################
#This Section is to declare Handlers Dictionarys for Different Parsing Functions

#This Dictionary is to map the DataMart Tables to thier corresponding Extraction Queries
lowlevelTables = {"dimProductCategory":selectProductCategory
                ,"dimProductSubCategory":selectProductSubCategory
                ,"dimProducts":selectProducts
                ,"dimCountryRegion":selectCountryRegion
                ,"dimTerritory":selectTerritory
                ,"dimStateProvince":selectStateProvince
                ,"dimLocations":selectLocation
                ,"dimCustomers":selectCustomer
                ,"dimEmployees":selectSalesPerson
                ,"dimStores":selectStore
                ,"dimSalesTax":selectSalesTaxes
                }

#This Dictionary is to handle Cube Level Data assigning null to default values
lowlevelTablesNullHandlers = {"dimProductCategory":{'idProductCategory': 0, 'nameProductCategory': None}
                ,"dimProductSubCategory":{'idProductSubCategory': 0,'idProductCategory': 0, 'Name': None}
                ,"dimCountryRegion":{'idCountryRegion':'NAN','nameCountryRegion':None}
                ,"dimTerritory":{'idTerritory': 0,'idCountryRegion':'NAN','name': None, 'territoryGroup':None}
                ,"dimStateProvince":{'idStateProvince': 0,'nameStateProvince': None,'idCountryRegion':'NAN','idTerritory':0}
                ,"dimLocations":{'idLocation': 0,'address': None, 'city': None, 'postalCode':None,'idStateProvince':0}
                ,"dimEmployees":{'idEmployee': 0,'dni': None, 'firstName': None, 'lastName':None,'gender':None}
                ,"dimStores":{'idStore': 0,'nameStore': None, 'idLocation':0,'idEmployee': 0}
                }

#this Dictionary is to replace the null values keys to defauilt values
lowlevelTablesNullReplacers = {"dimProducts":'idProductSubCategory'
                ,"dimCustomers":'idLocation'
                }
#############################################################################
#Dictionary value Searcher Function
def search(dictionary, searchString):
    return [key for key,val in dictionary.items() if searchString in val]

####### FUNCTION TO UPLOAD FACT SALES IN DB TABLE
def insert_FactSales(db,vals):
    
    query = "BEGIN TRAN \
        UPDATE [ProyectoBIIV3].[dbo].[factSales]  WITH (serializable) set [idSalesDetail]=?,[idSale]=?,[idCustomer]=?\
            ,[idEmployee]=?,[idDate]=?,[idTerritory]=?,[idLocation]=?,[idProduct]=?,[orderQuantity]=?,[unitPrice]=?\
        WHERE [idSalesDetail]=? \
        IF @@rowcount = 0 \
        BEGIN \
            INSERT INTO [ProyectoBIIV3].[dbo].[factSales] ([idSalesDetail],[idSale],[idCustomer]\
                ,[idEmployee],[idDate],[idTerritory],[idLocation]\
                ,[idProduct],[orderQuantity],[unitPrice]\
            )  \
            VALUES(?,?,?,?,?,?,?,?,?,?)\
        END\
        COMMIT TRAN"

    try:
        conn = db
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(query, vals)

        conn.commit()
        print("Rows Inserted in Fact Sales table: ",len(vals))
    except pyodbc.Error as error:
        print('Error uploading data: '+str(error))
        pass
    finally:
        cursor.close()

try:
    ####### Loop through the Dimension Low Level Tables for upsert Process in the DataMart
    for key,tables in lowlevelTables.items():
        try:
            print("Starting Low Hierarchy Tables Upload")
            # Starting Instance of Created Class SQLTables and Extracting data from AdventureWorks Database
            sqlTable = SQLTables()
            tableData = pd.read_sql_query(tables,sqlcon)

            #Replacing Default NAN Values in pandas Dataframe. Obtaining Columns available in the Dataframe for later Function Proccesing
            tableData = tableData.replace({np.nan: None})
            cols = [c for c in tableData.columns if 'index' not in c]

            #SQL_Metadata parsing to obtain table names and columns from existing Queries
            # NOTING: All queries created previously should be in the following format:
            # [Database].[Schema].[Table] for the Tables
            # as ColumnName for the Columns
            advWorksTable = sql_metadata.Parser(tables).tables
            advWorksTable = advWorksTable[0].split("[")[3][:-1]
            advWorkstablePK = sqlTable.pkCheck(sqlcon,advWorksTable)
            advWorksTableAliases = sql_metadata.Parser(tables).columns_aliases

            try:
                # Try Block to map the primarykeys from the AdventureWorks database tables to the pandas dataframe
                pkAlias = list(advWorksTableAliases.keys())[list(advWorksTableAliases.values()).index("["+advWorkstablePK+"]")]
            except ValueError as v:
                # If value not found then search for matching values in the dictionary
                #This Exception handle is due to table joins and Aliases as they appear in the sql_metadata parses as the format
                #[Database].[Schema].[Table] for the ambiguos columns
                print(v)
                print("Searching for matching values that contains the string in the dictionary")
                matchingVal = search(advWorksTableAliases,"["+advWorkstablePK+"]")[0]
                pkAlias = matchingVal
                print("PK Key Found!!")
                pass
            
            #Conditional Block to Assign the default values to the corresponding tables in the dictionary lowlevelTablesNullHandlers
            if key in lowlevelTablesNullHandlers:
                tableData = tableData.append(lowlevelTablesNullHandlers[key], ignore_index = True)

            #Conditional Block to Assign Nulls to the default values created to the corresponding tables in the dictionary lowlevelTablesNullReplacers
            if key in lowlevelTablesNullReplacers:
                tableData[lowlevelTablesNullReplacers[key]] = tableData[lowlevelTablesNullReplacers[key]].replace({None: 0})

            #Replacing Nan Values again
            tableData = tableData.replace({np.nan: None})

            #Transforming Dataframe for Upsert Query Parameters
            pkList = tableData[pkAlias].tolist()
            pkDF = pd.DataFrame(pkList,columns=[pkAlias])
            data = pd.concat([tableData.reset_index(drop=True),pkDF.reset_index(drop=True),tableData.reset_index(drop=True)],axis=1)

            vals = list(data.itertuples(index=False, name=None))
            
            #Starting Function with datamart connection, corresponding table to upsert as key, columns in the table and values from the pandas Dataframe
            sqlTable.tableInserterMany(sqlcon2,key,cols,vals)
           
        except Exception as e:
            print(e)

        pass


    
    ####* Parsing Facts Table
    try:
        print("Uploading Fact table Data!")
        tableSalesData = pd.read_sql_query(selectSales,sqlcon)
        tableSalesData = tableSalesData.replace({np.nan:None})

        #Defaulting Null Values of idEmployee column
        tableSalesData['idEmployee'] = tableSalesData['idEmployee'].replace({None: 0})
        
        #Transforming Dataframe for Upsert Query Parameters
        pkList = tableSalesData['idSalesDetail'].tolist()
        pkDF = pd.DataFrame(pkList,columns=['idSalesDetail'])
        dataSales = pd.concat([tableSalesData.reset_index(drop=True),pkDF.reset_index(drop=True),tableSalesData.reset_index(drop=True)],axis=1)

        valsFactSales = list(dataSales.itertuples(index=False, name=None))
        if not valsFactSales:
            print("passing by...")
        else:
            #Starting FactSales UPSERT Function
            insert_FactSales(sqlcon,valsFactSales)

    except Exception as e:
        print(e)
        pass

    print("Finished ETL Adventure Works Script succesfully!!!")
except Exception as e:
    print(e)

raise SystemExit




