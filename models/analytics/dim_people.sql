WITH source_data_people AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__people`
),
    source_data_people__renamed_columns AS (
        SELECT
            Person_ID as person_key
            , Full_Name as person_name
            , Preferred_Name as preferred_name
            , Search_Name as search_name
            , Is_Employee as is_employee
            , Is_Salesperson as is_sales_person
        FROM source_data_people
    ),
    source_data_people__cast_type AS (
        SELECT
            CAST( person_key as int ) person_key
            , CAST( person_name as string ) person_name
            , CAST( preferred_name as string ) preferred_name
            , CAST( search_name as string ) search_name
            , CAST( is_employee as string ) is_employee
            , CAST( is_sales_person as string ) is_sales_person
        FROM source_data_people__renamed_columns
    ),
    source_data_people__transform_data AS (
        SELECT
            person_key
            , person_name
            , preferred_name
            , search_name
            ,  CASE
                    WHEN is_employee = 'false' THEN "Not employee"
                    WHEN is_employee = 'true' THEN "Employee"
                    ELSE 'Error/ Invalid'
                END AS is_employee
            ,  CASE
                    WHEN is_sales_person = 'false' THEN "Not Salesperson"
                    WHEN is_sales_person = 'true' THEN "Salesperson"
                    ELSE 'Error/ Invalid'
                END AS is_sales_person
        FROM source_data_people__cast_type
    ),
    source_data_people__union_table AS (
        SELECT *
        FROM source_data_people__transform_data

        UNION ALL
        SELECT
            0 as person_key
            , "Undefined" as person_name
            , "Undefined" as preferred_name
            , "Undefined" as search_name
            , "Undefined" as is_employee
            , "Undefined" as is_sales_person

        UNION ALL
        SELECT
            -1 as person_key
            , "Error/ Invalid" as person_name
            , "Error/ Invalid" as preferred_name
            , "Error/ Invalid" as search_name
            , "Error/ Invalid" as is_employee
            , "Error/ Invalid" as is_sales_person
    )
    
SELECT
    person_key
    , person_name
    , preferred_name
    , search_name
    , is_employee
    , is_sales_person
FROM source_data_people__union_table