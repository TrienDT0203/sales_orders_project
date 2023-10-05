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

    )

SELECT 
    state_provinces.state_province_key
    , state_provinces.state_province_name    
    , state_provinces.country_key
    , COALESCE(dim_countries.country_name, "Invalid" ) AS country_name
FROM source_state_provinces__cast_type AS state_provinces
LEFT JOIN {{ ref('stg_dim_countries')}}  AS dim_countries
    ON state_provinces.country_key = dim_countries.country_key