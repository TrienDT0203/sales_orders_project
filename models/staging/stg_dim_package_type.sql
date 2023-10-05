WITH source_package_types AS (
    SELECT *
    FROM `vit-lam-data`.wide_world_importers.warehouse__package_types
)
     ,source_package_types__renamed_columns AS (
        SELECT
            Package_Type_ID AS package_type_key
            , Package_Type_Name AS package_type_name
            FROM source_package_types

    ),
     source_package_types__cast_type AS (
        SELECT
            CAST( package_type_key AS int )  package_type_key
            , CAST( package_type_name AS string )  package_type_name
            FROM source_package_types__renamed_columns

    )

SELECT
    package_type_key
    , package_type_name
FROM source_package_types__cast_type