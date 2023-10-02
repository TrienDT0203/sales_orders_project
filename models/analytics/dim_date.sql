WITH source_date_table AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2030-12-31', INTERVAL 1 DAY)) d
    ),
    source_data_table__renamed_columns AS (
        SELECT
            d AS date_key,
            EXTRACT(YEAR FROM d) AS year,
            EXTRACT(MONTH FROM d) AS month,
            FORMAT_DATE('%B', d) as month_name,
            EXTRACT(DAY FROM d) AS date,
            FORMAT_DATE('%A', d) AS date_of_week,
            FORMAT_DATE('%a', d) AS date_of_week_short,
            CASE
                WHEN FORMAT_DATE('%a', d) in ('Mon', 'Tue','Wed','Thu','Fri') THEN 'Weekday'
                WHEN FORMAT_DATE('%a', d) in ('Sat', 'Sun') THEN 'Weekend'
                ELSE 'Error/ Invalid'
                END AS is_weekday_or_weekend,
        FROM source_date_table
    ),
    source_data_table__cast_type AS (
        SELECT CAST(date_key as date) as date_key
            ,   CAST(year as int) as year
            ,   CAST(month as int) as month
            ,   CAST(month_name as string) as month_name
            ,   CAST(date as int) as date
            ,   CAST(date_of_week as string) as date_of_week
            ,   CAST(date_of_week_short as string) as date_of_week_short
            ,   CAST(is_weekday_or_weekend as string) as is_weekday_or_weekend
        FROM source_data_table__renamed_columns
    )


SELECT date_key 
    , year
    , month
    , month_name
    , date 
    , date_of_week
    , date_of_week_short
    , is_weekday_or_weekend
FROM source_data_table__renamed_columns

