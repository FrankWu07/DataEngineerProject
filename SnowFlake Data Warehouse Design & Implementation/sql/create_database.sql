use role sysadmin;

-- CREATE AND USE ETL DATABASE
create or replace database ETL;

-- create schema
create schema DEV;

-- create a warehouse
CREATE OR REPLACE WAREHOUSE ETL_TEST WITH 
    WAREHOUSE_SIZE = 'SMALL' 
    WAREHOUSE_TYPE = 'STANDARD' 
    AUTO_SUSPEND = 600 
    AUTO_RESUME = TRUE;

use warehouse ETL_TEST;

-- create landing table
create or replace table ETL.DEV.VIDEOS
(
  DATETIME TIMESTAMP,
  VIDEOTITLE VARCHAR,
  EVENTS VARCHAR
);

----------------------------------------------------DEV-------------------------------------------------
-- Create a file format object that specifies the Parquet file format type.
create or replace file format ETL.DEV.VIDEO_PARQUET_FORMAT
  type = parquet
;

-- 1. Create an external s3 stage:
-- Note: Stage location should be pointed to a folder location and not to a file.
create or replace stage ETL.DEV.VIDEO_STAGE
  url='s3://etl-yuewu/video_data/'
  credentials=(AWS_KEY_ID='', AWS_SECRET_KEY='')
  encryption=(TYPE='AWS_SSE_S3');
  
-- 2. Verify the stage is created using:
show stages;

-- 3. Create a pipe using auto_ingest=true:
create or replace pipe ETL.DEV.VIDEO_PIPE 
  auto_ingest = true
as copy into ETL.DEV.VIDEOS
from (
  select 
    $1:DateTime::timestamp,
    $1:VideoTitle::varchar,
    $1:events::varchar
  from @ETL.DEV.VIDEO_STAGE
  )
file_format = (format_name = ETL.DEV.VIDEO_PARQUET_FORMAT)
PATTERN='.*.parquet'
on_error = 'skip_file';

-- 4.Verify the pipe is created using:                                                                                                                                                        
show pipes;

-- 5. Run SHOW STAGES
-- to check for the NOTIFICATION_CHANNEL

show stages;
-- 6. Setup SQS notification ( https://www.snowflake.net/your-first-steps-with-snowpipe/ )

-- 7. Upload the file CSV file to the static folder in S3

-- 8. Run the following to check if the uploaded file is in your stage location:
ls @ETL.DEV.VIDEO_STAGE; 

-- 9. Wait for 10-15seconds and check the result: 
select * from ETL.DEV.VIDEOS;
--truncate table ETL.DEV.VIDEOS;

-- create view to parse source data to determined from the “events” column containing “206”
create or replace view ETL.DEV.VIDEO_VW as
(
  select 
    DATETIME,
    to_char(DATETIME,'YYYYMMDDHH24MISS') as DATEKEY,
    VIDEOTITLE,
    case
      when split_part(VIDEOTITLE,'|',1) like '%news%' then 'Desktop'
      when split_part(VIDEOTITLE,'|',1) like '%App%' then split_part(split_part(VIDEOTITLE,'|',1),' ',-1)
    end as Platform,
    case
      when split_part(VIDEOTITLE,'|',1) like '%news%' then split_part(VIDEOTITLE,'|',1) 
      when split_part(VIDEOTITLE,'|',1) like '%App%' then split_part(split_part(VIDEOTITLE,'|',1),' ',1)
    end as Site,
    split_part(VIDEOTITLE,'|',-1) as VIDEONAME,
    EVENTS
  from ETL.DEV.VIDEOS
  where EVENTS LIKE '%206%'
  and array_size(split(videotitle,'|')) <> 1
);
-- check view
select * from ETL.DEV.VIDEO_VW;