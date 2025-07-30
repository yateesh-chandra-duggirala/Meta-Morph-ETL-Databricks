-- Avg Order Value Over Time
SELECT 
    CONCAT("SALE_MONTH", '-', "SALE_YEAR"),
    SUM("SALE_AMOUNT") / COUNT("SALE_ID") as "AVG_ORDER_VALUE"
FROM
    legacy.customer_sales_report
WHERE 
    "SALE_DATE" > current_date - 150
GROUP BY 
    "SALE_MONTH",
    "SALE_YEAR",
    EXTRACT(MONTH FROM "SALE_DATE")
ORDER BY 
    EXTRACT(MONTH FROM "SALE_DATE")