/* SQL Script*/

--- CREATE TABLE ---
CREATE TABLE invoices_2009(
BillingAddress TEXT,
BillingCity TEXT,
BillingCountry TEXT,
BillingPostalCode TEXT,
BillingState TEXT,
CustomerId INTEGER,
InvoiceDate TEXT,
InvoiceId INTEGER PRIMARY KEY,
Total REAL);

CREATE TABLE invoices_2010(
BillingAddress TEXT,
BillingCity TEXT,
BillingCountry TEXT,
BillingPostalCode TEXT,
BillingState TEXT,
CustomerId INTEGER,
InvoiceDate TEXT,
InvoiceId INTEGER PRIMARY KEY,
Total REAL);

CREATE TABLE invoices_2011(
BillingAddress TEXT,
BillingCity TEXT,
BillingCountry TEXT,
BillingPostalCode TEXT,
BillingState TEXT,
CustomerId INTEGER,
InvoiceDate TEXT,
InvoiceId INTEGER PRIMARY KEY,
Total REAL);

CREATE TABLE invoices_2012(
BillingAddress TEXT,
BillingCity TEXT,
BillingCountry TEXT,
BillingPostalCode TEXT,
BillingState TEXT,
CustomerId INTEGER,
InvoiceDate TEXT,
InvoiceId INTEGER PRIMARY KEY,
Total REAL);

CREATE TABLE invoices_2013(
BillingAddress TEXT,
BillingCity TEXT,
BillingCountry TEXT,
BillingPostalCode TEXT,
BillingState TEXT,
CustomerId INTEGER,
InvoiceDate TEXT,
InvoiceId INTEGER PRIMARY KEY,
Total REAL);

--- DROP TABLE ---
DROP TABLE invoices_2009; 
DROP TABLE invoices_2010;
DROP TABLE invoices_2011;
DROP TABLE invoices_2012;
DROP TABLE invoices_2013;

--- Query 1 ---
select SUM(Total) AS Price, 
       strftime("%m-%Y", InvoiceDate) AS 'month-year' 
       FROM invoices_2012 GROUP BY strftime("%m-%Y", InvoiceDate);

--- Query 2 ---
SELECT COUNT (DISTINCT CustomerId), strftime("%m-%Y", InvoiceDate) AS 'month-year'
  FROM invoices_2012 GROUP BY strftime("%m-%Y", InvoiceDate);