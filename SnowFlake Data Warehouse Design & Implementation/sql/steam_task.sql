----------- create stream -------------
use schema dev;
create or replace stream ETL.DEV.VIDEO_STREAM ON table ETL.DEV.VIDEOS;
show streams;
select * from ETL.DEV.VIDEO_STREAM;

-- grant sysadmin permission to run task
use role accountadmin;
grant execute task on account to role SYSADMIN;
use ROLE SYSADMIN;

------ create dimension table task -------
create or replace task ETL.DEV.MERGE_DATE
  WAREHOUSE = ETL_TEST
  schedule = '1 minute'
when system$stream_has_data('ETL.DEV.VIDEO_STREAM')
as 
MERGE INTO ETL.DIM.DIMEDATE dd
USING
(
  SELECT DISTINCT datekey,datetime from  ETL.DEV.VIDEO_VW
) as vv
 on dd.datekey = vv.datekey
 and dd.datetime = vv.datetime
when matched then update 
  set dd.year = year(vv.datetime),
      dd.month = month(vv.datetime),
      dd.day = DAYOFMONTH(vv.datetime),
      dd.hour = HOUR(vv.datetime),
      dd.minute = MINUTE(vv.datetime),
      dd.second = SECOND(vv.datetime)
when not matched 
  then insert
    (datekey,datetime,year,month,day,hour,minute,second)
  values
    (vv.datekey,vv.datetime,year(vv.datetime),month(vv.datetime),DAYOFMONTH(vv.datetime),
     HOUR(vv.datetime),MINUTE(vv.datetime),SECOND(vv.datetime));

show tasks;
alter task ETL.DEV.MERGE_DATE resume;

----- create dimension platform task ----
create or replace task ETL.DEV.MERGE_PLATFORM
  WAREHOUSE = ETL_TEST
  schedule = '1 minute'
when system$stream_has_data('ETL.DEV.VIDEO_STREAM')
as
MERGE into ETL.DIM.DIMPLATFORM as dp
USING
(
  select distinct platform from ETL.DEV.VIDEO_VW
) as vv
on dp.platform = vv.platform
when not matched
  then insert (platform)
  values (vv.platform);

alter task ETL.DEV.MERGE_PLATFORM resume;

show tasks;

----- create dimension site task -------
create or replace task ETL.DEV.MERGE_SITE
  WAREHOUSE = ETL_TEST
  schedule = '1 minute'
when system$stream_has_data('ETL.DEV.VIDEO_STREAM')
as
MERGE into ETL.DIM.DIMSITE ds
USING
(
  SELECT distinct site from ETL.DEV.VIDEO_VW
) as vv
on ds.site = vv.site
when not matched
  then insert
    (site)
  values
    (vv.site);

alter task ETL.DEV.MERGE_SITE resume;

show tasks;


-------- create video name dimension task
create or replace task ETL.DEV.MERGE_VIDEONAME
  WAREHOUSE = ETL_TEST
  schedule = '1 minute'
when system$stream_has_data('ETL.DEV.VIDEO_STREAM')
as
MERGE into ETL.DIM.DIMVIDEO dv
USING 
(
  select distinct videoname from ETL.DEV.VIDEO_VW
) as vv
on dv.videoname = vv.videoname
when not matched
  then insert
    (videoname)
  values
    (vv.videoname);

alter task ETL.DEV.MERGE_VIDEONAME resume;
show tasks;

---------- create fact table task ------------
create or replace task ETL.DEV.MERGE_FACT
  WAREHOUSE = ETL_TEST
  schedule = '2 minute'
when system$stream_has_data('ETL.DEV.VIDEO_STREAM')
as
MERGE INTO ETL.DIM.FACTVIDEO fv 
USING
(
  select
    dd.DATEKEY,dp.PLATKEY,ds.SITEKEY,dv.VIDEOKEY,vv.EVENTS
  from ETL.DEV.VIDEO_VW vv
  inner join ETL.DIM.DIMEDATE as dd
  on dd.datetime = vv.datetime
  inner join ETL.DIM.DIMPLATFORM as dp
  on dp.platform = vv.platform
  inner join ETL.DIM.DIMSITE as ds
  on ds.site = vv.site
  inner join ETL.DIM.DIMVIDEO as dv
  on dv.videoname = vv.videoname
) as a
on fv.DATEKEY = a.DATEKEY
and fv.PLATKEY = a.PLATKEY
and fv.SITEKEY = a.SITEKEY
and fv.VIDEOKEY = a.VIDEOKEY
and fv.EVENTS = a.EVENTS
when not matched
  then insert
  (DATEKEY,PLATKEY,SITEKEY,VIDEOKEY,EVENTS)
  values
  (a.DATEKEY,a.PLATKEY,a.SITEKEY,a.VIDEOKEY,a.EVENTS);

alter task ETL.DEV.MERGE_FACT resume;
alter task ETL.DEV.MERGE_FACT suspend;
show tasks;




-- show task next run time
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state 
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;