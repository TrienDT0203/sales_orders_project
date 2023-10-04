/*
lastEditedBy: TrienDT
updatedAt: 2023/10/02
}
*/

WITH source_data_supplier AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__supplier_categories`
),
    source_data_supplier__renamed_columns AS (
        SELECT
             Supplier_Category_ID AS supplier_category_key
            ,  Supplier_Category_Name AS supplier_category_name
        FROM source_data_supplier

    ),
    source_data_supplier__cast_type AS (
        SELECT
            CAST( supplier_category_key AS INT ) AS supplier_category_key
            , CAST( supplier_category_name AS STRING ) AS supplier_category_name
        FROM source_data_supplier__renamed_columns

    ),
    source_data_supplier__union AS (
        SELECT *
        FROM source_data_supplier__cast_type

        UNION  All
        SELECT
            0 AS supplier_category_key
            , "Undefined" AS supplier_category_name

        UNION  All
        SELECT
            -1 AS supplier_category_key
            , "Error/ Invalid" AS supplier_category_name

    )

SELECT
    supplier_category_key
    , supplier_category_name
from source_data_supplier__union