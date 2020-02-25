create or replace schema "sessionagg";

!set force on
alter pump "sessionagg".* stop;
drop schema "sessionagg" cascade;
!set force off

create or replace schema "sessionagg";

set schema '"sessionagg"';
set path '"sessionagg"';

create or replace foreign stream "test_fs"
( "eventtime" varchar(20)
, "sessionId" bigint
, "charValue" varchar(20)
, "notes" varchar(100)
)
SERVER FILE_SERVER
OPTIONS
( PARSER 'CSV'
, DIRECTORY '/home/sqlstream/sessionagg/test'
, FILENAME_PATTERN 'test.csv'
, SKIP_HEADER 'true'
);

create or replace stream "test_stream_in"
( "eventtime" varchar(20)
, "sessionId" bigint
, "charValue" varchar(20)
, "notes"     varchar(100)
)
;

create or replace pump "test_pump_in" stopped
as
insert into "test_stream_in"
(rowtime, "eventtime", "sessionId", "charValue", "notes")
select STREAM CAST("eventtime" AS TIMESTAMP)
, "eventtime"
, "sessionId"
, "charValue"
, "notes"
from "test_fs";

CREATE OR REPLACE JAR "sessionagg_jar"
    LIBRARY 'file:/home/sqlstream/sessionagg/SessionAgg.jar'
    OPTIONS(0);


-- create a function for the HttpPost UDX

CREATE OR REPLACE FUNCTION "StrAgg"
( inputRows CURSOR
, "partitionColumnName" VARCHAR(128)
, "concatColumnName" VARCHAR(128)
, "resultColumnName" VARCHAR(128)
, "timeoutSecs" INTEGER
) RETURNS TABLE
( ROWTIME TIMESTAMP
, "sessionId" BIGINT
, "aggregatedValue" VARCHAR(2048)
)
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME '"sessionagg"."sessionagg_jar":com.sqlstream.udx.SessionAggUdx.stringAgg';

create or replace view "test_out"
AS
SELECT STREAM * FROM STREAM("StrAgg"
   (CURSOR (SELECT STREAM s.ROWTIME,* FROM "test_stream_in" s)
   ,'sessionId'
   ,'charValue'
   ,'aggregatedValue'
   , 240
   ));
