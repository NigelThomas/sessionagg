
create or replace schema "sessionagg";

!set force on
alter pump "sessionagg".* stop;
drop schema "sessionagg" cascade;
!set force off

create or replace schema "sessionagg";

set schema '"sessionagg"';
set path '"sessionagg"';

create or replace foreign stream "test_concat_fs"
( "eventtime" varchar(20)
, "sessionId" bigint
, "charValue" varchar(20)
, "intValue" integer
, "bigintValue" bigint
, "notes" varchar(100)
)
SERVER FILE_SERVER
OPTIONS
( PARSER 'CSV'
, DIRECTORY '/home/sqlstream/sessionagg/test'
, FILENAME_PATTERN 'testconcat.csv'
, SKIP_HEADER 'true'
);

create or replace stream "test_stream_in"
( "eventtime" varchar(20)
, "sessionId" bigint
, "charValue" varchar(20)
, "intValue" integer
, "bigintValue" bigint
, "binaryValue" varbinary(100)
, "notes"     varchar(100)
)
;

create or replace pump "test_pump_in" stopped
as
insert into "test_stream_in"
(rowtime, "eventtime", "sessionId", "charValue", "intValue","bigintValue"
, "binaryValue"
,"notes")
select STREAM CAST("eventtime" AS TIMESTAMP) 
, "eventtime"
, "sessionId"
, "charValue"
, "intValue" 
, "bigintValue" 
, varchar_to_varbinary('FEDCBA9876543210')
, "notes"
from "test_concat_fs";

CREATE OR REPLACE JAR "sessionagg_jar"
    LIBRARY 'file:/home/sqlstream/sessionagg/SessionAgg.jar'
    OPTIONS(0);


-- create a function for the HttpPost UDX

CREATE OR REPLACE FUNCTION "ConcatAgg"
( inputRows CURSOR
, "partitionColumnName" VARCHAR(128)
, "resultColumnName" VARCHAR(128)
, "timeoutSecs" INTEGER
, "fieldSeparator" VARCHAR(10)
, "rowSeparator" VARCHAR(10)
) RETURNS TABLE
( ROWTIME TIMESTAMP
, "sessionId" BIGINT
, "aggregatedValue" VARCHAR(2048)
)
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME '"sessionagg"."sessionagg_jar":com.sqlstream.udx.SessionConcatUdx.stringAgg';

create or replace view "test_out"
AS
SELECT STREAM * FROM STREAM("ConcatAgg"
   (CURSOR (SELECT STREAM s.ROWTIME,* FROM "test_stream_in" s)
   ,'sessionId'
   ,'aggregatedValue'
   , 240
   , '^'
   , '|'
   ));
