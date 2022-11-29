CREATE DATABASE ProyectoBIIV2;

USE ProyectoBIIV2;
GO

CREATE TABLE dimCustomer(
	idCustomer INTEGER NOT NULL,
	firstName NVARCHAR(50) NOT NULL,
	lastName NVARCHAR(50) NOT NULL,
	CONSTRAINT PK_Customer PRIMARY KEY (idCustomer)
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

CREATE TABLE dimStores(
	idStore INTEGER NOT NULL,
	nameStore NVARCHAR(50) NOT NULL,
	CONSTRAINT PK_Store PRIMARY KEY (idStore)
);
GO

CREATE TABLE dimLocation(
	idLocation INTEGER NOT NULL,
	province NVARCHAR(50) NOT NULL,
	city NVARCHAR(50) NOT NULL,
	postalCode NVARCHAR(15) NOT NULL,
	CONSTRAINT PK_Location PRIMARY KEY (idLocation)
);
GO

CREATE TABLE dimProducts(
	idProduct INTEGER NOT NULL,
	nameProduct NVARCHAR(50) NOT NULL,
	price MONEY NOT NULL,
	category NVARCHAR (50)
	CONSTRAINT PK_Product PRIMARY KEY (idProduct)
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
	CONSTRAINT FK_Sales_Customer FOREIGN KEY (idCustomer) REFERENCES dimCustomer(idCustomer),
	CONSTRAINT FK_Sales_Employee FOREIGN KEY (idEmployee) REFERENCES dimEmployees(idEmployee),
	CONSTRAINT FK_Sales_Time FOREIGN KEY (idTime) REFERENCES dimTime(idTime),
	CONSTRAINT FK_Sales_Store FOREIGN KEY (idStore) REFERENCES dimStores(idStore),
	CONSTRAINT FK_Sales_Location FOREIGN KEY (idLocation) REFERENCES dimLocation(idLocation),
	CONSTRAINT FK_Sales_Product FOREIGN KEY (idProduct) REFERENCES dimProducts(idProduct)
);
GO