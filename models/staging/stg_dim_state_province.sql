WITH source_state_provinces AS (
    SELECT *
    FROM `vit-lam-data`.wide_world_importers.application__state_provinces
)
     ,
     source_state_provinces__renamed_columns AS (
        SELECT
            State_Province_ID AS state_province_key
            , State_Province_Name AS state_province_name
            , Country_ID AS country_key
        FROM source_state_provinces

    ),
     source_state_provinces__cast_type AS (
        SELECT
            CAST( state_province_key AS int )  state_province_key
            , CAST( state_province_name AS string )  state_province_name
            , CAST( country_key AS int ) country_key
        FROM source_state_provinces__renamed_columns

    ),
    source_state_provinces__join_country_name AS (
        SELECT 
            state_provinces.*
            , dim_countries.country_name
        FROM source_state_provinces__cast_type AS state_provinces
        JOIN {{ ref('stg_dim_countries')}}  AS dim_countries
            ON state_provinces.country_key = dim_countries.country_key
    )
     ,
     source_state_provinces__union AS (
        SELECT *
        FROM source_state_provinces__join_country_name

        UNION  All
        SELECT
            0 AS state_province_key
            , "Undefined" AS state_province_name
            , 0 AS country_key
            , "Undefined" AS country_name

        UNION  All
        SELECT
            -1 AS state_province_key
            , "Error/ Invalid" AS state_province_name
            , -1 AS country_key
            , "Error/ Invalid" AS country_name

    )

SELECT
    state_province_key
    , state_province_name
    , country_key
    ,  country_name
from source_state_provinces__union