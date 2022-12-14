CREATE DATABASE ProyectoBIIV3;

USE ProyectoBIIV3;
GO

CREATE TABLE dimCountryRegion(
	idCountryRegion NVARCHAR(3) NOT NULL,
	nameCountryRegion NVARCHAR(50) NULL,
	CONSTRAINT PK_CountryRegion PRIMARY KEY (idCountryRegion)
);
GO

CREATE TABLE dimTerritory(
	idTerritory INTEGER NOT NULL,
	name NVARCHAR(50) NULL,
	territoryGroup NVARCHAR(50) NULL,
	idCountryRegion NVARCHAR(3) NOT NULL,
	CONSTRAINT PK_Territory PRIMARY KEY (idTerritory),
	CONSTRAINT FK_Territory_CountryRegion FOREIGN KEY (idCountryRegion) REFERENCES dimCountryRegion(idCountryRegion)
);
GO

CREATE TABLE dimStateProvince(
	idStateProvince INTEGER NOT NULL,
	nameStateProvince NVARCHAR(50) NULL, 
	idCountryRegion NVARCHAR(3) NOT NULL,
	idTerritory INTEGER NOT NULL,
	CONSTRAINT PK_StateProvince PRIMARY KEY (idStateProvince),
	CONSTRAINT FK_StateProvince_CountryRegion FOREIGN KEY (idCountryRegion) REFERENCES dimCountryRegion(idCountryRegion),
	CONSTRAINT FK_StateProvince_Territory FOREIGN KEY (idTerritory) REFERENCES dimTerritory(idTerritory)
);
GO

CREATE TABLE dimLocations(
	idLocation INTEGER NOT NULL,
	adress NVARCHAR(60) NULL,
	city NVARCHAR(30) NULL,
	postalCode NVARCHAR(15) NULL,
	idStateProvince INTEGER NOT NULL,
	CONSTRAINT PK_Locations PRIMARY KEY (idLocation),
	CONSTRAINT FK_Locations_StateProvince FOREIGN KEY (idStateProvince) REFERENCES dimStateProvince(idStateProvince)
);
GO

CREATE TABLE dimCustomers(
	idCustomer INTEGER NOT NULL,
	firstName NVARCHAR(50) NULL,
	lastName NVARCHAR(50) NULL,
	idLocation INTEGER NULL,
	CONSTRAINT PK_Customers PRIMARY KEY (idCustomer),
	CONSTRAINT FK_Customers_Location FOREIGN KEY (idLocation) REFERENCES dimLocations(idLocation)
);
GO

CREATE TABLE dimEmployees(
	idEmployee INTEGER NOT NULL,
	dni NVARCHAR(15) NULL,
	firstName NVARCHAR(50) NULL,
	lastName NVARCHAR(50) NULL,
	gender NCHAR(1) CHECK (gender IN ('F','M')) NULL,
	hireDate DATE NULL,
	CONSTRAINT PK_Employee PRIMARY KEY (idEmployee)
);
GO

CREATE TABLE dimStores(
	idStore INTEGER NOT NULL,
	nameStore NVARCHAR(50) NULL,
	idLocation INTEGER NOT NULL,
	idEmployee INTEGER NOT NULL,
	CONSTRAINT PK_Stores PRIMARY KEY (idStore),
	CONSTRAINT FK_Stores_Location FOREIGN KEY (idLocation) REFERENCES dimLocations(idLocation),
	CONSTRAINT FK_Stores_Employee FOREIGN KEY (idEmployee) REFERENCES dimEmployees(idEmployee)
);
GO

CREATE TABLE dimProductCategory(
	idProductCategory INTEGER NOT NULL,
	nameProductCategory NVARCHAR(50) NULL,
	CONSTRAINT PK_ProductCategory PRIMARY KEY (idProductCategory)
);
GO

CREATE TABLE dimProductSubCategory(
	idProductSubCategory INTEGER NOT NULL,
	Name NVARCHAR(50) NULL,
	idProductCategory INTEGER NOT NULL,
	CONSTRAINT PK_ProductSubCategory PRIMARY KEY (idProductSubCategory),
	CONSTRAINT FK_ProductSubCategory_ProductCategory FOREIGN KEY (idProductCategory) REFERENCES dimProductCategory(idProductCategory)
);
GO

CREATE TABLE dimProducts(
	idProduct INTEGER NOT NULL,
	nameProduct NVARCHAR(50) NOT NULL,
	listPrice MONEY NOT NULL,
	standardCost MONEY NOT NULL,
	idProductSubCategory INTEGER NULL,
	CONSTRAINT PK_Product PRIMARY KEY (idProduct),
	CONSTRAINT FK_Product_ProductSubCategory FOREIGN KEY (idProductSubCategory) REFERENCES dimProductSubCategory(idProductSubCategory)
);
GO

DECLARE @StartDate  date = '20050101';

DECLARE @CutoffDate date = DATEADD(DAY, -1, DATEADD(YEAR, 20, @StartDate));

;WITH seq(n) AS 
(
  SELECT 0 UNION ALL SELECT n + 1 FROM seq
  WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)
),
d(d) AS 
(
  SELECT DATEADD(DAY, n, @StartDate) FROM seq
),
src AS
(
  SELECT
    TheDate         = CONVERT(date, d),
    TheDay          = DATEPART(DAY,       d),
    TheDayName      = DATENAME(WEEKDAY,   d),
    TheWeek         = DATEPART(WEEK,      d),
    TheISOWeek      = DATEPART(ISO_WEEK,  d),
    TheDayOfWeek    = DATEPART(WEEKDAY,   d),
    TheMonth        = DATEPART(MONTH,     d),
    TheMonthName    = DATENAME(MONTH,     d),
    TheQuarter      = DATEPART(Quarter,   d),
    TheYear         = DATEPART(YEAR,      d),
    TheFirstOfMonth = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
    TheLastOfYear   = DATEFROMPARTS(YEAR(d), 12, 31),
    TheDayOfYear    = DATEPART(DAYOFYEAR, d)
  FROM d
),
dim AS
(
  SELECT
    TheDate, 
    TheDay,
    TheDaySuffix        = CONVERT(char(2), CASE WHEN TheDay / 10 = 1 THEN 'th' ELSE 
                            CASE RIGHT(TheDay, 1) WHEN '1' THEN 'st' WHEN '2' THEN 'nd' 
                            WHEN '3' THEN 'rd' ELSE 'th' END END),
    TheDayName,
    TheDayOfWeek,
   
    TheWeek,
    TheISOweek,
   
    TheMonthName,
	TheMonth,
   
    TheQuarter,
    TheFirstOfQuarter   = MIN(TheDate) OVER (PARTITION BY TheYear, TheQuarter),
    TheLastOfQuarter    = MAX(TheDate) OVER (PARTITION BY TheYear, TheQuarter),
    TheYear,
   
    Style120            = CONVERT(char(10), TheDate, 120)
  FROM src
)

SELECT * INTO dbo.dimDate FROM dim
  ORDER BY TheDate
  OPTION (MAXRECURSION 0);

ALTER TABLE dimDate 
ALTER COLUMN TheDate DATE NOT NULL

ALTER TABLE dimDate 
ADD CONSTRAINT  PK_Date PRIMARY KEY NONCLUSTERED (TheDate)
CREATE UNIQUE CLUSTERED INDEX PK_DateDimension ON dbo.dimDate(TheDate);


CREATE TABLE dimSalesTax(
	idSale INTEGER NOT NULL,
	taxAmount MONEY NULL,
	freight MONEY NULL,
	CONSTRAINT PK_SalesTaxes PRIMARY KEY (idSale)
);
GO

CREATE TABLE factSales(
	idSalesDetail INTEGER NOT NULL,
	idSale INTEGER NOT NULL,
	idCustomer INTEGER NOT NULL,
	idEmployee INTEGER NOT NULL,
	idDate DATE NOT NULL,
	idTerritory INTEGER NOT NULL,
	idLocation INTEGER NOT NULL,
	idProduct INTEGER NOT NULL,
	orderQuantity INTEGER NOT NULL,
	unitPrice MONEY NOT NULL
	CONSTRAINT PK_Sales PRIMARY KEY (idSalesDetail),
	CONSTRAINT FK_Sales_SalesTaxes FOREIGN KEY (idSale) REFERENCES dimSalesTax(idSale),
	CONSTRAINT FK_Sales_Customer FOREIGN KEY (idCustomer) REFERENCES dimCustomers(idCustomer),
	CONSTRAINT FK_Sales_Employee FOREIGN KEY (idEmployee) REFERENCES dimEmployees(idEmployee),
	CONSTRAINT FK_Sales_Date FOREIGN KEY (idDate) REFERENCES dimDate(TheDate),
	CONSTRAINT FK_Sales_Territory FOREIGN KEY (idTerritory) REFERENCES dimTerritory(idTerritory),
	CONSTRAINT FK_Sales_Location FOREIGN KEY (idLocation) REFERENCES dimLocations(idLocation),
	CONSTRAINT FK_Sales_Product FOREIGN KEY (idProduct) REFERENCES dimProducts(idProduct)
);
GO

CREATE VIEW [dbo].[salesTaxCTEView]
AS
		With sales as (
	SELECT         idSalesDetail, idSale, idCustomer, idEmployee, idDate, idTerritory, idLocation, SUM(orderQuantity)* SUM(unitPrice) as totalDue
	FROM            factSales
	GROUP BY  idSalesDetail, idSale, idCustomer, idEmployee, idDate, idTerritory, idLocation
	),
	salesFinal as(SELECT idSale, idCustomer, idEmployee, idDate, idTerritory, idLocation, SUM(totalDue) as subTotal
	 FROM sales
	 GROUP BY  idSale, idCustomer, idEmployee, idDate, idTerritory, idLocation
	 
	)
SELECT sF.idSale, idCustomer, idEmployee, idDate, idTerritory, idLocation, subTotal, sTax.taxAmount, sTax.freight
 FROM salesFinal as sF
LEFT JOIN [dbo].[dimSalesTax] as sTax on sF.idSale = sTax.idSale

GO