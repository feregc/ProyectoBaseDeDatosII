CREATE DATABASE ProyectoBIIV3;

USE ProyectoBIIV3;
GO

CREATE TABLE dimCountryRegion(
	idCountryRegion NVARCHAR(3) NOT NULL,
	nameCountryRegion NVARCHAR(50) NOT NULL,
	CONSTRAINT PK_CountryRegion PRIMARY KEY (idCountryRegion)
);
GO

CREATE TABLE dimStateProvince(
	idStateProvince INTEGER NOT NULL,
	nameStateProvince NVARCHAR(50) NOT NULL, 
	idCountryRegion NVARCHAR(3) NOT NULL,
	CONSTRAINT PK_StateProvince PRIMARY KEY (idStateProvince),
	CONSTRAINT FK_StateProvince_CountryRegion FOREIGN KEY (idCountryRegion) REFERENCES dimCountryRegion(idCountryRegion)
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

CREATE TABLE dimStores(
	idStore INTEGER NOT NULL,
	nameStore NVARCHAR(50) NOT NULL,
	idLocation INTEGER NOT NULL,
	CONSTRAINT PK_Stores PRIMARY KEY (idStore),
	CONSTRAINT FK_Stores_Location FOREIGN KEY (idLocation) REFERENCES dimLocations(idLocation)
);
GO

CREATE TABLE dimProductCategory(
	idProductCategory INTEGER NOT NULL,
	nameProductCategory NVARCHAR(50) NOT NULL,
	CONSTRAINT PK_ProductCategory PRIMARY KEY (idProductCategory)
);
GO

CREATE TABLE dimProducts(
	idProduct INTEGER NOT NULL,
	nameProduct NVARCHAR(50) NOT NULL,
	listPrice MONEY NOT NULL,
	standarCost MONEY NOT NULL,
	idProductCategory INTEGER NOT NULL,
	CONSTRAINT PK_Product PRIMARY KEY (idProduct),
	CONSTRAINT FK_product_ProductCategory FOREIGN KEY (idProductCategory) REFERENCES dimProductCategory(idProductCategory)
);
GO

CREATE TABLE dimEmployees(
	idEmployee INTEGER NOT NULL,
	dni NVARCHAR(15) NOT NULL,
	firstName NVARCHAR(50) NOT NULL,
	lastName NVARCHAR(50) NOT NULL,
	gender NCHAR(1) CHECK (gender IN ('F','M')),
	CONSTRAINT PK_Employee PRIMARY KEY (idEmployee)
);
GO

CREATE TABLE dimTime(
	idTime INTEGER NOT NULL,
	year DATE NOT NULL,
	month DATE NOT NULL,
	trimester DATE NOT NULL,
	CONSTRAINT PK_Time PRIMARY KEY (idTime)
);
GO

CREATE TABLE factSales(
	idSales INTEGER IDENTITY NOT NULL,
	idCustomer INTEGER NOT NULL,
	idEmployee INTEGER NOT NULL,
	idTime INTEGER NOT NULL,
	idStore INTEGER NOT NULL,
	idLocation INTEGER NOT NULL,
	idProduct INTEGER NOT NULL,
	quantity INTEGER NOT NULL,
	total MONEY NOT NULL
	CONSTRAINT PK_Sales PRIMARY KEY (idSales),
	CONSTRAINT FK_Sales_Customer FOREIGN KEY (idCustomer) REFERENCES dimCustomers(idCustomer),
	CONSTRAINT FK_Sales_Employee FOREIGN KEY (idEmployee) REFERENCES dimEmployees(idEmployee),
	CONSTRAINT FK_Sales_Time FOREIGN KEY (idTime) REFERENCES dimTime(idTime),
	CONSTRAINT FK_Sales_Store FOREIGN KEY (idStore) REFERENCES dimStores(idStore),
	CONSTRAINT FK_Sales_Location FOREIGN KEY (idLocation) REFERENCES dimLocations(idLocation),
	CONSTRAINT FK_Sales_Product FOREIGN KEY (idProduct) REFERENCES dimProducts(idProduct)
);
GO