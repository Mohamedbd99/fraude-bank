CREATE DATABASE IF NOT EXISTS bankdb;
USE bankdb;

-- Table principale : toutes les transactions bancaires
CREATE TABLE IF NOT EXISTS transactions (
  TransactionID           VARCHAR(20) PRIMARY KEY,
  AccountID               VARCHAR(20),
  TransactionAmount       DECIMAL(10,2),
  TransactionDate         DATETIME,
  TransactionType         VARCHAR(10),
  Location                VARCHAR(100),
  DeviceID                VARCHAR(20),
  IPAddress               VARCHAR(45),
  MerchantID              VARCHAR(20),
  Channel                 VARCHAR(20),
  CustomerAge             INT,
  CustomerOccupation      VARCHAR(50),
  TransactionDuration     INT,
  LoginAttempts           INT,
  AccountBalance          DECIMAL(10,2),
  PreviousTransactionDate DATETIME
);

-- Charger les 2512 transactions depuis le CSV
LOAD DATA INFILE '/var/lib/mysql-files/transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(TransactionID, AccountID, TransactionAmount, TransactionDate, TransactionType,
 Location, DeviceID, IPAddress, MerchantID, Channel, CustomerAge,
 CustomerOccupation, TransactionDuration, LoginAttempts, AccountBalance,
 PreviousTransactionDate);

-- Table pour recevoir les résultats de détection de fraude (Sqoop export)
CREATE TABLE IF NOT EXISTS fraud_alerts (
  TransactionID       VARCHAR(20),
  AccountID           VARCHAR(20),
  TransactionAmount   DECIMAL(10,2),
  TransactionDate     DATETIME,
  TransactionType     VARCHAR(10),
  Location            VARCHAR(100),
  DeviceID            VARCHAR(20),
  IPAddress           VARCHAR(45),
  MerchantID          VARCHAR(20),
  Channel             VARCHAR(20),
  CustomerAge         INT,
  CustomerOccupation  VARCHAR(50),
  TransactionDuration INT,
  LoginAttempts       INT,
  AccountBalance      DECIMAL(10,2),
  PreviousTransactionDate DATETIME
);
