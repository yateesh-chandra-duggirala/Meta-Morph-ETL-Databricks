SELECT 
    CONCAT(
        CASE "SALE_MONTH"
            WHEN 'January' THEN '01'
            WHEN 'February' THEN '02'
            WHEN 'March' THEN '03'
            WHEN 'April' THEN '04'
            WHEN 'May' THEN '05'
            WHEN 'June' THEN '06'
            WHEN 'July' THEN '07'
            WHEN 'August' THEN '08'
            WHEN 'September' THEN '09'
            WHEN 'October' THEN '10'
            WHEN 'November' THEN '11'
            WHEN 'December' THEN '12'
        END,
        ' - ',
        "SALE_MONTH"
    ) AS SALE_MONTH_ORDERED,
    "SALE_YEAR",
    SUM("SALE_AMOUNT") AS SUM_SALE
FROM 
    legacy.customer_sales_report
GROUP BY 
    "SALE_MONTH",
    "SALE_YEAR"
ORDER BY 
    SALE_MONTH_ORDERED ASC,
    "SALE_YEAR" DESC
LIMIT 100;
