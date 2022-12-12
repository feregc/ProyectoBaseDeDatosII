CREATE DATABASE ProyectoBIIV3;

USE ProyectoBIIV3;
GO

CREATE TABLE dimCountryRegion(
	idCountryRegion NVARCHAR(3) NOT NULL,
	nameCountryRegion NVARCHAR(50) NOT NULL,
	CONSTRAINT PK_CountryRegion PRIMARY KEY (idCountryRegion)
);
GO

CREATE TABLE dimTerritory(
	idTerritory INTEGER NOT NULL,
	nameTerritory NVARCHAR(50) NOT NULL,
	territoryGroup NVARCHAR(50) NOT NULL,
	idCountryRegion NVARCHAR(3) NOT NULL,
	CONSTRAINT PK_Territory PRIMARY KEY (idTerritory),
	CONSTRAINT FK_Territory_CountryRegion FOREIGN KEY (idCountryRegion) REFERENCES dimCountryRegion(idCountryRegion)
);
GO

CREATE TABLE dimStateProvince(
	idStateProvince INTEGER NOT NULL,
	nameStateProvince NVARCHAR(50) NOT NULL, 
	idCountryRegion NVARCHAR(3) NOT NULL,
	idTerritory INTEGER NOT NULL,
	CONSTRAINT PK_StateProvince PRIMARY KEY (idStateProvince),
	CONSTRAINT FK_StateProvince_CountryRegion FOREIGN KEY (idCountryRegion) REFERENCES dimCountryRegion(idCountryRegion),
	CONSTRAINT FK_StateProvince_Territory FOREIGN KEY (idTerritory) REFERENCES dimTerritory(idTerritory)
);
GO

CREATE TABLE dimLocations(
	idLocation INTEGER NOT NULL,
	adress NVARCHAR(60) NOT NULL,
	city NVARCHAR(30) NOT NULL,
	postalCode NVARCHAR(15) NOT NULL,
	idStateProvince INTEGER NOT NULL,
	CONSTRAINT PK_Locations PRIMARY KEY (idLocation),
	CONSTRAINT FK_Locations_StateProvince FOREIGN KEY (idStateProvince) REFERENCES dimStateProvince(idStateProvince)
);
GO

CREATE TABLE dimCustomers(
	idCustomer INTEGER NOT NULL,
	firstName NVARCHAR(50) NOT NULL,
	lastName NVARCHAR(50) NOT NULL,
	idLocation INTEGER NOT NULL,
	CONSTRAINT PK_Customers PRIMARY KEY (idCustomer),
	CONSTRAINT FK_Customers_Location FOREIGN KEY (idLocation) REFERENCES dimLocations(idLocation)
);
GO

CREATE TABLE dimEmployees(
	idEmployee INTEGER NOT NULL,
	dni NVARCHAR(15) NOT NULL,
	firstName NVARCHAR(50) NOT NULL,
	lastName NVARCHAR(50) NOT NULL,
	gender NCHAR(1) CHECK (gender IN ('F','M')),
	hireDate DATE NOT NULL,
	CONSTRAINT PK_Employee PRIMARY KEY (idEmployee)
);
GO

CREATE TABLE dimStores(
	idStore INTEGER NOT NULL,
	nameStore NVARCHAR(50) NOT NULL,
	idLocation INTEGER NOT NULL,
	idEmployee INTEGER NOT NULL,
	CONSTRAINT PK_Stores PRIMARY KEY (idStore),
	CONSTRAINT FK_Stores_Location FOREIGN KEY (idLocation) REFERENCES dimLocations(idLocation),
	CONSTRAINT FK_Stores_Employee FOREIGN KEY (idEmployee) REFERENCES dimEmployees(idEmployee)
);
GO

CREATE TABLE dimProductCategory(
	idProductCategory INTEGER NOT NULL,
	nameProductCategory NVARCHAR(50) NOT NULL,
	CONSTRAINT PK_ProductCategory PRIMARY KEY (idProductCategory)
);
GO

CREATE TABLE dimProductSubCategory(
	idProductSubCategory INTEGER NOT NULL,
	nameProductSubCategory NVARCHAR(50) NOT NULL,
	idProductCategory INTEGER NOT NULL,
	CONSTRAINT PK_ProductSubCategory PRIMARY KEY (idProductSubCategory),
	CONSTRAINT FK_ProductSubCategory_ProductCategory FOREIGN KEY (idProductCategory) REFERENCES dimProductCategory(idProductCategory)
);
GO

CREATE TABLE dimProducts(
	idProduct INTEGER NOT NULL,
	nameProduct NVARCHAR(50) NOT NULL,
	listPrice MONEY NOT NULL,
	standarCost MONEY NOT NULL,
	idProductSubCategory INTEGER NOT NULL,
	CONSTRAINT PK_Product PRIMARY KEY (idProduct),
	CONSTRAINT FK_Product_ProductSubCategory FOREIGN KEY (idProductSubCategory) REFERENCES dimProductSubCategory(idProductSubCategory)
);
GO

CREATE TABLE dimDate(
	TheDate DATE NOT NULL,
	TheDay INTEGER NOT NULL,
	TheDaySuffix NCHAR(2) NOT NULL,
	TheDayName NVARCHAR(30) NOT NULL,
	TheDayOfWeek INTEGER NOT NULL,
	TheWeek INTEGER NOT NULL,
	TheISOweek INTEGER NOT NULL,
	TheMonthName NVARCHAR(30) NOT NULL,
	TheQuarter INTEGER NOT NULL,
	TheFirstOfQuarter DATE NOT NULL,
	TheLastOfQuarter DATE NOT NULL,
	TheYear INTEGER NOT NULL,
	Style120 NCHAR(10) NOT NULL,
	CONSTRAINT PK_Date PRIMARY KEY (TheDate)
);
GO


CREATE TABLE dimSalesTaxes(
	idSales INTEGER NOT NULL,
	taxAmount MONEY NOT NULL,
	freight MONEY NOT NULL,
	CONSTRAINT PK_SalesTaxes PRIMARY KEY (idSales)
);
GO

CREATE TABLE factSales(
	idSalesDetail INTEGER NOT NULL,
	idSales INTEGER NOT NULL,
	idCustomer INTEGER NOT NULL,
	idEmployee INTEGER NOT NULL,
	idDate DATE NOT NULL,
	idTerritory INTEGER NOT NULL,
	idLocation INTEGER NOT NULL,
	idProduct INTEGER NOT NULL,
	orderQuantity INTEGER NOT NULL,
	unitPrice MONEY NOT NULL
	CONSTRAINT PK_Sales PRIMARY KEY (idSalesDetail),
	CONSTRAINT FK_Sales_SalesTaxes FOREIGN KEY (idSales) REFERENCES dimSalesTaxes(idSales),
	CONSTRAINT FK_Sales_Customer FOREIGN KEY (idCustomer) REFERENCES dimCustomers(idCustomer),
	CONSTRAINT FK_Sales_Employee FOREIGN KEY (idEmployee) REFERENCES dimEmployees(idEmployee),
	CONSTRAINT FK_Sales_Date FOREIGN KEY (idDate) REFERENCES dimDate(TheDate),
	CONSTRAINT FK_Sales_Territory FOREIGN KEY (idTerritory) REFERENCES dimTerritory(idTerritory),
	CONSTRAINT FK_Sales_Location FOREIGN KEY (idLocation) REFERENCES dimLocations(idLocation),
	CONSTRAINT FK_Sales_Product FOREIGN KEY (idProduct) REFERENCES dimProducts(idProduct)
);
GO
