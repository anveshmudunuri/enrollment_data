[2021 Top 10 Enrolled States.csv](https://github.com/user-attachments/files/17008305/2021.Top.10.Enrolled.States.csv)
## Enrollment Data Load

This repository contains a serverless data ingestion pipeline that leverages AWS Lambda to ingest data from API response to S3 and load it into a Snowflake data warehouse.

- Using requests library to fetch data from enrollment API.
- Using boto3 library to push csv files to S3.
- Create External Stage to AWS S3 in Snowflake.
- Create File Format in Snowflake.
- Create table to hold the data from S3 in snowflake.
- Create Snowpipe in Snowflake to read data from External Stage (S3).

### API call: 
Refer to the enrollment_data.py attached above for python code which calls API read json, convert to CSV and load it to S3.

### Snowflake Steps for Data Load Automation


### Create File Format in Snowflake:
``` SQL
CREATE OR REPLACE FILE FORMAT RAW_DATABASE.ANVESH_DEV.ENROLLMENT_DATA_CSV_FORMAT
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;
```

### Create External Stage to AWS S3 bucket:
``` SQL
CREATE OR REPLACE STAGE RAW_DATABASE.ANVESH_DEV.ENROLLMENT_STAGE
URL = 's3://enrollmentbucket/enrollment_data/'
CREDENTIALS = ( AWS_KEY_ID = 'XXXXXXXXXXXx' AWS_SECRET_KEY = 'xXXXXXXXXXX' )
DIRECTORY = ( ENABLE = true );
```
### Create Table in Snowflake:
``` SQL
create or replace TABLE RAW_DATABASE.ANVESH_DEV.ENROLLMENT_DATA (
YEAR Number(38,0),
 NCESSCH VARCHAR(16777216),
NCESSCH_NUM NUMBER(38,0),
GRADE NUMBER(38,0),
RACE NUMBER(38,0),
SEX NUMBER(38,0),
ENROLLMENT NUMBER(38,0),
FIPS NUMBER(38,0),
LEAID VARCHAR(38,0)
);
```
Created Snwoflake Table which holds FIPS Number, State Name. this is created using data from API documentation:
``` SQL
create or replace TABLE RAW_DATABASE.ANVESH_DEV.EDU_STATE_NUMBER (
	NO NUMBER(38,0),
	STATE VARCHAR(16777216)
);
```

### Create Snowpipe to autoload data from S3 External Stage to Snowflake table
``` SQL
create or replace pipe RAW_DATABASE.ANVESH_DEV.ENROLLMENT_PIPE
AUTO_INGEST = TRUE as
copy into RAW_DATABASE.ANVESH_DEV.ENROLLMENT_DATA
from @RAW_DATABASE.ANVESH_DEV.ENROLLMENT_STAGE
file_format = (format_name = MANUAL.CSVWITHQUOTES)
pattern='enrollement_data_.*.csv'
ON_ERROR='CONTINUE';
```
Snowpipe will be polling to external stage for new files and will try to load as and when the new files arrive.


### Query to retrive top 10 states highest number of children enrolled in Pre-K
``` SQL
select top 10 ED.FIPS, SN.STATE,count(*) NUMBER_OF_ENROLLMENTS
from RAW_DATABASE.ANVESH_DEV.EDU_ENROLLMENT_DATA ED
left join RAW_DATABASE.ANVESH_DEV.EDU_STATE_NUMBER SN on (ED.FIPS=SN.NO)
where YEAR=2021
group by ED.FIPS,SN.STATE
ORDER BY NUMBER_OF_ENROLLMENTS DESC;
```
## Attached Results below:
<img width="1221" alt="image" src="https://github.com/user-attachments/assets/6e1ff446-dc3c-4f2b-8c32-cb897599e378">





