use schema snowflake.account_usage;
select 
    lh.USER_NAME,
    MAX(lh.EVENT_TIMESTAMP) as LAST_USED,
    ARRAY_UNIQUE_AGG(lh.REPORTED_CLIENT_VERSION) as SNOWSQL_VERSIONS,
    ARRAY_UNIQUE_AGG(lh.CLIENT_IP) as LOGIN_IPS,
    COUNT(*) as CNT_SESSIONS
from login_history lh 
where true
    and lh.EVENT_TIMESTAMP >= dateadd(day, -365, current_timestamp())
    and lh.REPORTED_CLIENT_TYPE like 'SNOWSQL%'
and lh.REPORTED_CLIENT_VERSION like '1.%'
    and lh.EVENT_TYPE = 'LOGIN'
group by lh.USER_NAME
order by lh.USER_NAME;

--     and lh.REPORTED_CLIENT_VERSION like '1.%'
-- 1.2

select * from login_history;