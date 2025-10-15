SELECT symbol as symbol FROM STOCK;

select symbol, max(lastprice)
from stockvalues
where symbol in ('AMD', 'SNOW', 'AAPL', 'GOOG', 'GOOGL', 'JPM')
group by symbol
order by symbol asc;


select *, TO_TIMESTAMP(ts) AS formatted_timestamp
from   stockvalues
where lastprice is not null
order by ts desc;