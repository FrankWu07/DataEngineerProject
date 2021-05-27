----------create dimension table and fact table  -------------

use database ETL;
use warehouse ETL_TEST;
-- create a new schema for dimension table and fact table
create or replace schema DIM;
use schema DIM;

-- create a dimdate dimension table
create or replace table ETL.DIM.DIMEDATE(
  DATEKEY INT NOT NULL PRIMARY KEY,
  DATETIME TIMESTAMP NOT NULL,
  YEAR NUMBER NOT NULL,
  MONTH NUMBER NOT NULL,
  DAY NUMBER NOT NULL,
  HOUR NUMBER NOT NULL,
  MINUTE NUMBER NOT NULL,
  SECOND NUMBER NOT NULL
);

-- create platform dimension table 
create or replace table ETL.DIM.DIMPLATFORM
(
  PLATKEY INT PRIMARY KEY AUTOINCREMENT start 1 increment 1,
  PLATFORM VARCHAR NOT NULL
);

--create a site dimension
create or replace table ETL.DIM.DIMSITE
(
  SITEKEY INT PRIMARY KEY AUTOINCREMENT start 1 increment 1,
  SITE VARCHAR NOT NULL
);

-- create a video name dimension
create or replace table ETL.DIM.DIMVIDEO
(
  VIDEOKEY INT PRIMARY KEY AUTOINCREMENT start 1 increment 1,
  VIDEONAME VARCHAR NOT NULL
);

--create fact table
create or replace table ETL.DIM.FACTVIDEO
(
  PK INT PRIMARY KEY AUTOINCREMENT start 1 increment 1,
  DATEKEY INT,
  PLATKEY INT,
  SITEKEY INT,
  VIDEOKEY INT,
  EVENTS VARCHAR,
  CONSTRAINT fkey_1 FOREIGN KEY(DATEKEY) REFERENCES ETL.DIM.DIMEDATE(DATEKEY),
  CONSTRAINT fkey_2 FOREIGN KEY(PLATKEY) REFERENCES ETL.DIM.DIMPLATFORM(PLATKEY),
  CONSTRAINT fkey_3 FOREIGN KEY(SITEKEY) REFERENCES ETL.DIM.DIMSITE(SITEKEY),
  CONSTRAINT fkey_4 FOREIGN KEY(VIDEOKEY) REFERENCES ETL.DIM.DIMVIDEO(VIDEOKEY)
);