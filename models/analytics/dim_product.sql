WITH source_product AS (
    SELECT *
    FROM `vit-lam-data`.wide_world_importers.warehouse__stock_items
),
    source_product__rename_columns AS (
        SELECT 
            stock_item_id AS product_key 					
            , stock_item_name AS product_name 					
            , supplier_id AS supplier_key 					
            , color_id AS color_key					
            , unit_package_id AS unit_package_key 					
            , outer_package_id AS outer_package_key 					
            , brand AS brand 					
            , size AS size 					
            , lead_time_days AS lead_time_days 								
            , is_chiller_stock AS is_chiller_stock 					
            , barcode AS barcode 					
            , tax_rate AS tax_rate 					
            , unit_price AS unit_price 					
            , recommended_retail_price AS recommended_retail_price 					
            , typical_weight_per_unit AS typical_weight_per_unit 	
            , search_details AS search_details 
        FROM source_product	
    ),
    source_product__cast_type AS (
        SELECT 
              CAST(product_key AS INTEGER ) product_key					
            , CAST(product_name AS STRING ) product_name				
            , CAST(supplier_key AS INTEGER	)  supplier_key				
            , CAST(color_key AS INT ) color_key			
            , CAST(unit_package_key AS INTEGER ) unit_package_key					
            , CAST(outer_package_key AS INTEGER ) outer_package_key					
            , CAST(brand AS STRING ) brand				
            , CAST(size AS STRING )  size				
            , CAST(lead_time_days AS INTEGER ) lead_time_days							
            , CAST(is_chiller_stock AS BOOLEAN ) is_chiller_stock					
            , CAST(barcode AS STRING )	barcode				    
            , CAST(tax_rate AS FLOAT64 )	tax_rate			
            , CAST(unit_price AS FLOAT64 ) unit_price			
            , CAST(recommended_retail_price AS FLOAT64 ) recommended_retail_price			
            , CAST(typical_weight_per_unit AS FLOAT64 ) typical_weight_per_unit			
            , CAST(search_details as STRING ) search_details
        FROM source_product__rename_columns	
    )

SELECT
    dim_product.product_key
    , dim_product.product_name
    , dim_product.supplier_key
    , COALESCE(dim_supplier.supplier_name, "Invalid") supplier_name
    , dim_supplier.supplier_category_key supplier_category_key
    , COALESCE(dim_supplier.supplier_category_name, "Invalid") supplier_category_name
    , dim_product.color_key
    , COALESCE(dim_color.color_name, "Invalid") color_name
    , dim_product.unit_package_key
    , COALESCE(dim_package_type.package_type_name, "Invalid")  unit_package_name
    , dim_product.outer_package_key
    , dim_outer_package_name.package_type_name as outer_package_name
    , dim_product.brand
    , dim_product.size
    , dim_product.lead_time_days
    , dim_product.is_chiller_stock
    , dim_product.barcode
    , dim_product.tax_rate
    , dim_product.unit_price
    , dim_product.recommended_retail_price
    , dim_product.typical_weight_per_unit
    , dim_product.search_details
FROM source_product__cast_type as dim_product
LEFT JOIN {{ref('stg_dim_color')}} as dim_color
    ON dim_product.color_key = dim_color.color_key
LEFT JOIN {{ref('stg_dim_supplier')}} as dim_supplier
    ON dim_product.supplier_key = dim_supplier.supplier_key
LEFT JOIN {{ref('dim_package_type')}} AS dim_package_type
    ON dim_product.unit_package_key = dim_package_type.package_type_key
LEFT JOIN {{ref('dim_package_type')}} AS dim_outer_package_name
    ON dim_product.outer_package_key = dim_outer_package_name.package_type_key