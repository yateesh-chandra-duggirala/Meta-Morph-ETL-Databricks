SELECT 
    "SUPPLIER_ID", 
    "PRODUCT_ID",
    "TOP_SELLING_PRODUCT",
    "CATEGORY",
    "TOTAL_REVENUE"
FROM 
    (
        SELECT 
            sp."SUPPLIER_ID",
            sp."TOP_SELLING_PRODUCT",
            p."PRODUCT_NAME",
            p."PRODUCT_ID",
            p."CATEGORY",
            sp."TOTAL_REVENUE",
            ROW_NUMBER() OVER(
                PARTITION BY sp."SUPPLIER_ID"
                order by "TOTAL_REVENUE" desc
            ) as rnk
        FROM 
            legacy.supplier_performance sp 
        join 
            legacy.products p 
        on 
            trim(p."SUPPLIER_ID") = trim(sp."SUPPLIER_ID")
        and 
            trim(p."PRODUCT_NAME") = trim(sp."TOP_SELLING_PRODUCT")
    ) as subquery
where
    subquery.rnk = 1