select DATE(qh.start_time), cf.model_name, cf.function_name, sum(cf.tokens), sum(cf.token_credits)
from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
join SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY cf on cf.query_id = qh.query_id
where 1=1
and qh.start_time > '2025-07-20 00:00:00' and qh.start_time < '2025-10-07 00:00:00'
group by 1,2,3
order by DATE(qh.start_time) desc;


ALTER TABLE MTABUSVEHICLEMONITORING SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE STREAM 
mtabusstream 
ON TABLE 
MTABUSVEHICLEMONITORING;

select count(*) from MTABUSVEHICLEMONITORING;
-- 27405
-- 31053

select * from mtabusstream;
-- includes METADATA$ACTION / update/row_id

select count(*) from mtabusstream;
-- 3648

show streams;

describe stream mtabusstream;

call SYSTEM$STREAM_HAS_DATA ('mtabusstream');


select * 
from MTABUSVEHICLEMONITORING
where progressrate = 'normalProgress'
order by expecteddeparturetime desc 
limit 250;


select EXPECTEDDEPARTURETIME, TO_TIMESTAMP_NTZ(EXPECTEDDEPARTURETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM') as TimeStampx , VEHICLEREF, DESTINATIONNAME
FROM DEMO.DEMO.ICYMTA
WHERE EXPECTEDDEPARTURETIME is not null and TRIM(EXPECTEDDEPARTURETIME) != ''
order by EXPECTEDDEPARTURETIME desc;

select * FROM DEMO.DEMO.ICYMTA;