
WITH source_buying_group AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
),
    source_buying_group__renamed_columns AS (
        SELECT
             buying_group_ID AS buying_group_key
            ,  buying_group_Name AS buying_group_name
        FROM source_buying_group

    ),
    source_buying_group__cast_type AS (
        SELECT
            CAST( buying_group_key AS INT ) AS buying_group_key
            , CAST( buying_group_name AS STRING ) AS buying_group_name
        FROM source_buying_group__renamed_columns
    )

SELECT
    buying_group_key
    , buying_group_name
from source_buying_group__cast_type