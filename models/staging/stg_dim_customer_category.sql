
WITH source_customer_category AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customer_categories`
),
    source_customer_category__renamed_columns AS (
        SELECT
             customer_category_ID AS customer_category_key
            ,  customer_category_Name AS customer_category_name
        FROM source_customer_category

    ),
    source_customer_category__cast_type AS (
        SELECT
            CAST( customer_category_key AS INT ) AS customer_category_key
            , CAST( customer_category_name AS STRING ) AS customer_category_name
        FROM source_customer_category__renamed_columns
    )

SELECT
    customer_category_key
    , customer_category_name
from source_customer_category__cast_type