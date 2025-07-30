SELECT 
    s."REGION",
    count(p."PRODUCT_ID")
FROM 
    legacy.products p
join 
    legacy.suppliers s 
on
    trim(s."SUPPLIER_ID") = trim(p."SUPPLIER_ID")
GROUP BY
    s."REGION"