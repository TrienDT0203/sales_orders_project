WITH source_sales_order_line AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
),
  source_sales_order_line_rename_columns AS (
    SELECT 
        order_line_id AS order_line_key
      , order_id AS order_key
      , stock_item_id AS stock_item_key
      , description AS description
      , package_type_id AS package_type_key
      , picked_quantity AS picked_quantity
      , quantity AS quantity
      , unit_price AS unit_price
      , tax_rate AS tax_rate
    FROM source_sales_order_line  
  ),
  source_sales_order_line_cast_type AS (
    SELECT 
        CAST(order_line_key AS  INT ) order_line_key
      , CAST(order_key AS  INT ) order_key
      , CAST(stock_item_key AS  INT ) stock_item_key
      , CAST(description AS  STRING ) description
      , CAST(package_type_key AS  INT ) package_type_key
      , CAST(picked_quantity AS  INT ) picked_quantity
      , CAST(quantity AS  INT ) quantity
      , CAST(unit_price AS FLOAT64 ) unit_price
      , CAST(tax_rate AS FLOAT64) tax_rate
    FROM source_sales_order_line_rename_columns  
  ),
  source_sales_order_line_join_fact_sales_order AS (
    SELECT 
        order_line.order_line_key
      , order_line.order_key
      , order_line.stock_item_key
      , order_header.customer_key
      , order_header.salesperson_person_key
      , order_header.picked_by_persion_key
      , order_header.contact_person_key
      , order_header.expected_delivery_date
      , order_line.description
      , order_line.package_type_key
      , order_header.order_date
      , order_line.picked_quantity
      , order_line.quantity
      , order_line.unit_price
      , order_line.tax_rate
    FROM source_sales_order_line_cast_type AS order_line  
    JOIN {{ ref('stg_fact_sales_order')}}  AS order_header
        ON order_line.order_key = order_header.order_key
  )
SELECT 
    order_line_key
    , order_key
    , stock_item_key
    , customer_key
    , salesperson_person_key
    , picked_by_persion_key
    , contact_person_key
    , expected_delivery_date
    , description
    , package_type_key
    , order_date
    , picked_quantity
    , quantity
    , unit_price
    , tax_rate
    , quantity * unit_price * (1+tax_rate/100) as cost
FROM source_sales_order_line_join_fact_sales_order