SELECT 
    "SALE_DATE",
    SUM("SALE_AMOUNT")
FROM 
    legacy.customer_sales_report
WHERE 
    "SALE_DATE" > current_date - 8
GROUP BY 
    "SALE_DATE"