select 
user_name,
client_application_id
from snowflake.account_usage.sessions
where true
and created_on >= dateadd('days', -90, current_date()) --last 90 days, please adjust
and (
    client_application_id ilike '%SnowSQL%1.0.%'
    or client_application_id ilike '%SnowSQL%1.1.%'
    or client_application_id ilike '%SnowSQL%1.2.%'
)
group by 1,2
order by 1,2