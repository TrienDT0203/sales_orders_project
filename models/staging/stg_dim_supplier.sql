/*
lastEditedBy: TrienDT
updatedAt: 2023/10/05
}
*/

WITH source_supplier AS (
  select *
  from `vit-lam-data.wide_world_importers.purchasing__suppliers`
),
    source_supplier__renamed_columns AS (
        SELECT
            supplier_id AS supplier_key
            , supplier_name AS supplier_name
            , Supplier_Category_ID AS supplier_category_key
            , Delivery_Method_ID AS delivery_method_key 
            , Delivery_City_ID AS delivery_city_key
            , Postal_City_ID AS postal_city_key
        FROM source_supplier

    ),
    source_supplier__cast_type AS (
        SELECT
            CAST( supplier_key AS INT ) supplier_key 
            , CAST(supplier_name AS STRING ) supplier_name
            , CAST(supplier_category_key AS INT ) supplier_category_key
            , CAST(delivery_method_key AS INT ) delivery_method_key 
            , CAST(delivery_city_key AS INT ) delivery_city_key
            , CAST(postal_city_key AS INT ) postal_city_key
        FROM source_supplier__renamed_columns

    ),
    source_supplier__join_postal_city AS (
        SELECT 
            dim_supplier.*
            , COALESCE(dim_city.city_name, "Invalid") postal_city_name
        FROM source_supplier__cast_type AS dim_supplier
        LEFT JOIN {{ ref('stg_dim_city')}} as dim_city
            ON dim_supplier.postal_city_key = dim_city.city_key
    )

SELECT
    dim_supplier.supplier_key
    , dim_supplier.supplier_name
    , dim_supplier.supplier_category_key
    , COALESCE(dim_supplier_category.supplier_category_name, "Invalid" ) AS supplier_category_name 
    , dim_supplier.delivery_method_key
    , COALESCE(dim_delivery_method.delivery_method_name, "Invalid" ) AS delivery_method_name 
    , dim_supplier.delivery_city_key
    , COALESCE(dim_city.city_name, "Invalid" ) AS delivery_city_name 
    , dim_supplier.postal_city_key
    , dim_supplier.postal_city_name
FROM source_supplier__join_postal_city AS dim_supplier
LEFT JOIN {{ ref('stg_dim_supplier_category') }} AS dim_supplier_category
    ON dim_supplier.supplier_category_key = dim_supplier_category.supplier_category_key
LEFT JOIN {{ ref('stg_dim_delivery_method')}} AS dim_delivery_method
    ON dim_supplier.delivery_method_key = dim_delivery_method.delivery_method_key
LEFT JOIN {{ ref('stg_dim_city')}} AS dim_city
    ON dim_supplier.delivery_city_key = dim_city.city_key
    