/*
lastEditedBy: TrienDT
updatedAt: 2023/10/05
}
*/

WITH source_colors AS (
    SELECT *
    FROM `vit-lam-data`.wide_world_importers.warehouse__colors
),
    source_colors__renamed_columns AS (
        SELECT
             color_id AS color_key
            ,  color_name AS color_name
        FROM source_colors

    ),
     source_colors__cast_type AS (
        SELECT
            CAST( color_key AS INT ) AS color_key
            , CAST( color_name AS STRING ) AS color_name
        FROM source_colors__renamed_columns

    ),
    source_colors__union AS (
        SELECT *
        FROM source_colors__cast_type

        UNION  All
        SELECT
            0 AS color_key
            , "Undefined" AS color_name

        UNION  All
        SELECT
            -1 AS color_key
            , "Error/ Invalid" AS color_name

    )

SELECT
    color_key
    , color_name
from source_colors__union