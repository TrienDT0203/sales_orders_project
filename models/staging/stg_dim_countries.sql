WITH source_countries AS (
    SELECT *
    FROM `vit-lam-data`.wide_world_importers.application__countries
)
     ,source_countries__renamed_columns AS (
        SELECT
            country_id AS country_key
            , country_name AS country_name
            , formal_name AS formal_name
            , continent AS continent
            , region AS region
        FROM source_countries

    ),
     source_countries__cast_type AS (
        SELECT
            CAST( country_key AS int )   country_key
            , CAST( country_name AS string )  country_name
            , CAST( formal_name AS string ) formal_name
            , CAST( continent AS string ) continent
            , CAST( region AS string ) region
        FROM source_countries__renamed_columns

    ),
    source_countries__union AS (
        SELECT *
        FROM source_countries__cast_type

        UNION  All
        SELECT
            0 AS country_key
            , "Undefined" AS country_name
            , "Undefined" AS formal_name
            , "Undefined" AS continent
            , "Undefined" AS region

        UNION  All
        SELECT
            -1 AS country_key
            , "Error/ Invalid" AS country_name
            , "Error/ Invalid" AS formal_name
            , "Error/ Invalid" AS continent
            , "Error/ Invalid" AS region

    )

SELECT
    country_key
    ,  country_name
    ,  formal_name
    ,  continent
    ,  region
from source_countries__union