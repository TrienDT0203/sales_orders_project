WITH source_city AS (
    SELECT *
    FROM `vit-lam-data`.wide_world_importers.application__cities
)
     ,source_city__renamed_columns AS (
        SELECT
            city_id AS city_key
            , city_name AS city_name
            , state_province_id AS state_province_key
            FROM source_city

    ),
     source_city__cast_type AS (
        SELECT
            CAST( city_key AS int )   city_key
            , CAST( city_name AS string )  city_name
            , CAST( state_province_key AS int ) state_province_key
            FROM source_city__renamed_columns

    )

SELECT
      dim_city.city_key
    , dim_city.city_name
    , dim_city.state_province_key
    , COALESCE(dim_state_province.state_province_name, "Invalid") AS state_province_name
from source_city__cast_type dim_city
LEFT JOIN {{ ref('stg_dim_state_province')}} AS dim_state_province
    ON dim_city.state_province_key = dim_state_province.state_province_key