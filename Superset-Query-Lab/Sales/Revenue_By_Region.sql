SELECT 
    s."REGION",  
    SUM(sp."TOTAL_REVENUE") as "REGIONAL_REVENUE"
FROM 
    legacy.supplier_performance sp 
JOIN 
    legacy.suppliers s 
on 
    trim(s."SUPPLIER_ID")= trim(sp."SUPPLIER_ID")
GROUP BY 
    s."REGION"