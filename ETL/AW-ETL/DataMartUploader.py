#!/usr/bin/env python
# coding: utf-8

##########################################################################################################
#Library list

import pyodbc
import pandas as pd
import numpy as np
import os

import sql_metadata
from SQL.sqlTables2 import SQLTables
from pathlib import Path

import importlib.util
#############################################################################################################
drive = Path(__file__).drive

if os.name == 'nt':
    ConnectorSpecs = importlib.util.spec_from_file_location("Connector.py", os.path.expandvars("{}\\Users\\$USERNAME\\Desktop\\ProyectoBaseDeDatosII\\ETL\\AW-ETL\\SQL\\Connector.py".format(drive)))

Connector = importlib.util.module_from_spec(ConnectorSpecs)
ConnectorSpecs.loader.exec_module(Connector)
#############################################################################################################
#MSSQL DATABASE CONNECTOR
#SELECT Table 
sqlcon = Connector.connection.sqlConnection('AdventureWorks2016')
sqlcon2 = Connector.connection.sqlConnection('ProyectoBIIV3')



selectProductSubCategory = "SELECT [ProductSubcategoryID] as idProductSubCategory,[ProductCategoryID] as idProductCategory,[Name] FROM [AdventureWorks2016].[Production].[ProductSubcategory]"
selectProductCategory = "SELECT [ProductCategoryID] as idProductCategory,[Name] as nameProductCategory FROM [AdventureWorks2016].[Production].[ProductCategory]"
selectProducts = "SELECT [ProductID] as idProduct,[Name] as nameProduct,[ListPrice] as listPrice,[StandardCost] as standarCost,[ProductSubcategoryID] as idProductSubCategory FROM [AdventureWorks2016].[Production].[Product]"
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
    WHERE AddressID is not null
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
                }

#############################################################################
#Dictionary value Searcher Function
def search(dictionary, searchString):
    return [key for key,val in dictionary.items() if searchString in val]

####### FUNCTION TO UPLOAD FACT SALES IN DB TABLE
def insert_FactSales(db,vals):
    
    query = "BEGIN TRAN \
        UPDATE snag_dialpad_calls_callcenter WITH (serializable) set [idSalesDetail]=?,[idSale]=?,[idCustomer]=?\
            ,[idEmployee]=?,[idDate]=?,[idTerritory]=?,[idLocation]=?,[idProduct]=?,[orderQuantity]=?,[unitPrice]=?\
        WHERE [idSalesDetail]=? \
        IF @@rowcount = 0 \
        BEGIN \
            INSERT INTO snag_dialpad_calls_callcenter ([idSalesDetail],[idSale],[idCustomer]\
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
            # Starting Instance of Created Clas SQLTables
            sqlTable = SQLTables()
            tableData = pd.read_sql_query(tables,sqlcon)

            tableData = tableData.replace({np.nan: None})
            cols = [c for c in tableData.columns if 'index' not in c]

            advWorksTable = sql_metadata.Parser(tables).tables
            advWorksTable = advWorksTable[0].split("[")[3][:-1]
            advWorkstablePK = sqlTable.pkCheck(sqlcon,advWorksTable)

            advWorksTableAliases = sql_metadata.Parser(tables).columns_aliases

            try:
                pkAlias = list(advWorksTableAliases.keys())[list(advWorksTableAliases.values()).index("["+advWorkstablePK+"]")]
            except ValueError as v:
                print(v)
                print("Searching for matching values that contains the string in the dictionary")
                matchingVal = search(advWorksTableAliases,"["+advWorkstablePK+"]")[0]
                pkAlias = matchingVal
                print("PK Key Found!!")
                pass

            pkList = tableData[pkAlias].tolist()
            pkDF = pd.DataFrame(pkList,columns=[pkAlias])
            data = pd.concat([tableData.reset_index(drop=True),pkDF.reset_index(drop=True),tableData.reset_index(drop=True)],axis=1)

            vals = list(data.itertuples(index=False, name=None))
            
            sqlTable.tableInserterMany(sqlcon2,key,cols,vals)
           
        except Exception as e:
            print(e)

        pass


    
    ####* Parsing Facts Table
    try:
        print("Uploading Data!")
        tableSalesData = pd.read_sql_query(selectSales,sqlcon)
        tableSalesData = tableSalesData.replace({np.nan:None})

        pkList = tableSalesData['idSalesDetail'].tolist()
        pkDF = pd.DataFrame(pkList,columns=['idSalesDetail'])
        dataSales = pd.concat([tableSalesData.reset_index(drop=True),pkDF.reset_index(drop=True),tableSalesData.reset_index(drop=True)],axis=1)

        valsFactSales = list(dataSales.itertuples(index=False, name=None))
        if not valsFactSales:
            print("passing by...")
        else:
            insert_FactSales(sqlcon,valsFactSales)

    except Exception as e:
        print(e)
        pass

    print("Finished ETL Adventure Works Script succesfully!!!")
except Exception as e:
    print(e)

raise SystemExit




