WITH source_cities AS (
    SELECT *
    FROM `vit-lam-data`.wide_world_importers.application__cities
)
     ,source_cities__renamed_columns AS (
        SELECT
            city_id AS city_key
            , city_name AS city_name
            , state_province_id AS state_province_key
            FROM source_cities

    ),
     source_cities__cast_type AS (
        SELECT
            CAST( city_key AS int )   city_key
            , CAST( city_name AS string )  city_name
            , CAST( state_province_key AS int ) state_province_key
            FROM source_cities__renamed_columns

    ),
    source_cities__join__state_province_name AS ( 
        SELECT 
            dim_cities.*
            , dim_state_province.state_province_name
        FROM source_cities__cast_type AS dim_cities
        JOIN {{ ref('stg_dim_state_province')}} AS dim_state_province
            ON dim_cities.state_province_key = dim_state_province.state_province_key
    ),
    source_cities__union AS (
        SELECT *
        FROM source_cities__join__state_province_name

        UNION  All
        SELECT
            0 AS city_key
            , "Undefined" AS city_name
            , 0 AS state_province_key
            , "Undefined" AS state_province_name
            
        UNION  All
        SELECT
            -1 AS city_key
            , "Error/ Invalid" AS city_name
            , -1 AS state_province_key
            , "Error/ Invalid" AS state_province_name
            
    )

SELECT
    city_key
    ,  city_name
    ,  state_province_key
    ,  state_province_name
from source_cities__union