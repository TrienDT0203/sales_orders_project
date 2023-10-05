/*
lastEditedBy: TrienDT
updatedAt: 2023/10/02
}
*/

WITH source_data_delivery_method AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
),
    source_data_delivery_method__renamed_columns AS (
        SELECT
             Delivery_Method_ID AS delivery_method_key
            ,  Delivery_Method_Name AS delivery_method_name
        FROM source_data_delivery_method

    ),
     source_data_delivery_method__cast_type AS (
        SELECT
            CAST( delivery_method_key AS INT ) AS delivery_method_key
            , CAST( delivery_method_name AS STRING ) AS delivery_method_name
        FROM source_data_delivery_method__renamed_columns

    )

SELECT
    delivery_method_key
    , delivery_method_name
from source_data_delivery_method__cast_type