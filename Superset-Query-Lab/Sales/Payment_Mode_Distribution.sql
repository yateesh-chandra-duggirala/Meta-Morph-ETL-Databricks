-- Create 
SELECT 
    "PAYMENT_MODE",
    count(*) AS "COUNT"
from 
    legacy.sales
group by 
    "PAYMENT_MODE";