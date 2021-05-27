# SnowFlake Data Warehouse Design & Implementation

## Contents

- [Introduction](#Introduction)
- [Design](#Design)
  * [Table Design](#Table-Design)
  * [Process Design](#Process-Design)
- [Result](#Result)
- [Project Files](#Project-Files)
- [Future Work](#Future-Work)

## Introduction

* This goal project is constructing a star schema data warehouse and building an ETL process for Video Data. The raw data can be found as following links:
  1. [CSV Format](https://s3-ap-southeast-2.amazonaws.com/jiangren-de-bucket/assignments/video_data.csv)
  2. [Parquet Format](https://s3-ap-southeast-2.amazonaws.com/jiangren-de-bucket/assignments/video_data.gz.parquet)

* The final achievement of this project is that once uploading raw video data into AWS S3 bucket, the raw data will directly load into snowflake database which we created before, and then reformated and insert data into dimension table as well as generate surrogate key. Finally append data into fact table.  

## Design

### Table Design

<u>SK stands for surrogate key.</u>

- ***Raw Table - VIDEOS***

| COLUMN_NAME |   DATA_TYPE   | PK   | NULLABLE | DATA_DEFAULT | COLUMN_ID | COMMENTS |
| :---------: | :-----------: | ---- | :------: | :----------: | :-------: | :------: |
|  DATETIME   | TIMESTAMP_NTZ | N    |   YES    |     NULL     |     1     | Raw File |
| VIDEOTITLE  |    VARCHAR    | N    |   YES    |     NULL     |     2     | Raw File |
|   EVENTS    |    VARCHAR    | N    |   YES    |     NULL     |     3     | Raw File |

- ***View Table - VIDEO_VW***

| COLUMN_NAME |   DATA_TYPE   |  PK  | NULLABLE | DATA_DEFAULT | COLUMN_ID |              COMMENTS              |
| :---------: | :-----------: | :--: | :------: | :----------: | :-------: | :--------------------------------: |
|  DATETIME   | TIMESTAMP_NTZ |  N   |    No    |     NULL     |     1     |  Reformatted from VIDEOS.DATATIME  |
|   DATEKEY   |    VARCHAR    |  N   |    No    |     NULL     |     2     |  Reformatted from VIDEOS.DATATIME  |
| VIDEOTITLE  |    VARCHAR    |  N   |    No    |     NULL     |     3     | Reformatted from VIDEOS.VIDEOTITLE |
|  PLATFORM   |    VARCHAR    |  N   |    No    |     NULL     |     4     | Reformatted from VIDEOS.VIDEOTITLE |
|    SITE     |    VARCHAR    |  N   |    No    |     NULL     |     5     | Reformatted from VIDEOS.VIDEOTITLE |
|  VIDEONAME  |    VARCHAR    |  N   |    No    |     NULL     |     6     | Reformatted from VIDEOS.VIDEOTITLE |
|   EVENTS    |    VARCHAR    |  N   |    No    |     NULL     |     7     |   Reformatted from VIDEOS.EVENTS   |

- ***Date Dimension Table - DIMEDATE***

| COLUMN_NAME  |   DATA_TYPE   |  PK  | NULLABLE | DATA_DEFAULT | COLUMN_ID |            COMMENTS            |
| :----------: | :-----------: | :--: | :------: | :----------: | :-------: | :----------------------------: |
| DATEKEY (SK) |    NUMBER     |  Y   |    No    |              |     1     | Derived from VIDEO_VW.DATEKEY  |
|   DATETIME   | TIMESTAMP_NTZ |  Y   |    No    |     NULL     |     2     | Derived from VIDEO_VW.DATETIME |
|     YEAR     |    NUMBER     |  N   |    No    |     NULL     |     3     | Derived from VIDEO_VW.DATETIME |
|    MONTH     |    NUMBER     |  N   |    No    |     NULL     |     4     | Derived from VIDEO_VW.DATETIME |
|     DAY      |    NUMBER     |  N   |    No    |     NULL     |     5     | Derived from VIDEO_VW.DATETIME |
|     HOUR     |    NUMBER     |  N   |    No    |     NULL     |     6     | Derived from VIDEO_VW.DATETIME |
|    MINUTE    |    NUMBER     |  N   |    No    |     NULL     |     7     | Derived from VIDEO_VW.DATETIME |
|    SECOND    |    NUMBER     |  N   |    No    |     NULL     |     8     | Derived from VIDEO_VW.DATETIME |



* ***Platform Dimension Table  - DIMPLATFORM***

  | COLUMN_NAME  | DATA_TYPE |  PK  | NULLABLE | DATA_DEFAULT | COLUMN_ID |            COMMENTS            |
  | :----------: | :-------: | :--: | :------: | :----------: | :-------: | :----------------------------: |
  | PLATKEY (SK) |  NUMBER   |  Y   |    No    |              |     1     |                                |
  |   PLATFORM   |  VARCHAR  |  N   |    No    |     NULL     |     2     | Derived from VIDEO_VW.PLATFORM |

  

* ***Site Dimension Table - DIMSITE***

  | COLUMN_NAME  | DATA_TYPE | PK   | NULLABLE | DATA_DEFAULT | COLUMN_ID | COMMENTS                   |
  | ------------ | --------- | ---- | -------- | ------------ | --------- | -------------------------- |
  | SITEKEY (SK) | NUMBER    | Y    | No       |              | 1         |                            |
  | SITE         | VARCHAR   | N    | No       | NULL         | 2         | Derived from VIDEO_VW.SITE |

  

* ***Video Name Dimension Table - DIMVIDEO***

  |  COLUMN_NAME  | DATA_TYPE |  PK  | NULLABLE | DATA_DEFAULT | COLUMN_ID |            COMMENTS             |
  | :-----------: | :-------: | :--: | :------: | :----------: | :-------: | :-----------------------------: |
  | VIDEOKEY (SK) |  NUMBER   |  Y   |    No    |              |     1     |                                 |
  |   VIDEONAME   |  VARCHAR  |  N   |    No    |     NULL     |     2     | Derived from VIDEO_VW.VIDEONAME |

  

* ***Fact Table - FACTVIDEO***

  | COLUMN_NAME | DATA_TYPE |  PK  | NULLABLE | DATA_DEFAULT | COLUMN_ID |             COMMENTS             |
  | :---------: | :-------: | :--: | :------: | :----------: | :-------: | :------------------------------: |
  |     PK      |  NUMBER   |  Y   |    No    |              |     1     |          Auto Generate           |
  |   DATEKEY   |  NUMBER   |  N   |    No    |     NULL     |     2     |  Derived from DIMEDATE.DATEKEY   |
  |   PLATKEY   |  NUMBER   |  N   |    No    |     NULL     |     3     | Derived from DIMPLATFORM.PLATKEY |
  |   SITEKEY   |  NUMBER   |  N   |    No    |     NULL     |     4     |   Derived from DIMSITE.SITEKEY   |
  |  VIDEOKEY   |  NUMBER   |  N   |    No    |     NULL     |     5     |  Derived from DIMVIDEO.VIDEOKEY  |
  |   EVENTS    |  VARCHAR  |  N   |    No    |     NULL     |     6     |   Derived from VIDEO_VW.EVENTS   |



### Process Design

#### Graphic Workflow

![image-20210331221956342](https://tva1.sinaimg.cn/large/008eGmZEgy1gp3b1yxu8kj30n20ywdj5.jpg)



## Result

* Source table (ETL.DEV.VIDEOS) display 

  ![image-20210313023207695](https://tva1.sinaimg.cn/large/008eGmZEgy1gp3bib91t9j31ri0o8grf.jpg)

* Refined view (ETL.DEV.VIDEO_VW ) display 

  ![image-20210313023337788](https://tva1.sinaimg.cn/large/008eGmZEgy1gp2udyrf1pj31rw0o0tew.jpg)

* DimDate table display 

  ![image-20210313023446704](https://tva1.sinaimg.cn/large/008eGmZEgy1gp2ue3texcj31re0q8jvc.jpg)

* DimPlatform table display 

  ![image-20210313023509883](https://tva1.sinaimg.cn/large/008eGmZEgy1gp2ue7aeuoj31ru0js40c.jpg)

* DimSite table display

  ![image-20210313023545423](https://tva1.sinaimg.cn/large/008eGmZEgy1gp2ueaw2wij31ru0fcq4e.jpg)

* DimVideo table display 

  ![image-20210313023619987](https://tva1.sinaimg.cn/large/008eGmZEgy1gp2ueegcyvj31rs0psq71.jpg)

* Fact table display

  ![image-20210313025742793](https://tva1.sinaimg.cn/large/008eGmZEgy1gp2uem0ma2j31rw0mstcy.jpg)



## Project Files

`sql` folder: contains all sql command for this project.

## Future Work

1. When create table, the data type length should be set appropriatly for saving memory, rather than maximum length. 

2. If the source dimension data contains not only the PK but also some attributes, and we want to track the changes of attributes, we should use Dimension Type Two.

3. It should be design a control flow task for warehouse to check all dimension table insert data completely before starting append data into fact table. 

   