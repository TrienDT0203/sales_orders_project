WITH source_sales_order AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
),
  source_sales_order__rename_columns AS (
    SELECT 
        order_id AS order_key
      , customer_id AS customer_key
      , salesperson_person_id AS salesperson_person_key
      , picked_by_person_id AS picked_by_persion_key
      , contact_person_id AS contact_person_key
      , order_date AS order_date
      , expected_delivery_date AS expected_delivery_date
      , customer_purchase_order_number AS customer_purchase_order_number
      , picking_completed_when AS picking_completed_when
    FROM source_sales_order  
  ),
  source_sales_order__cast_type AS (
    SELECT 
        CAST(order_key AS INT ) order_key  
      , CAST(customer_key AS INT ) customer_key 
      , CAST(salesperson_person_key AS INT ) salesperson_person_key 
      , CAST(picked_by_persion_key AS INT ) picked_by_persion_key 
      , CAST(contact_person_key AS INT ) contact_person_key 
      , CAST(order_date AS DATE ) order_date 
      , CAST(expected_delivery_date AS DATE ) expected_delivery_date 
      , CAST(customer_purchase_order_number AS INT ) customer_purchase_order_number 
      , CAST(picking_completed_when AS DATETIME ) picking_completed_when 
    FROM source_sales_order__rename_columns  
  )

SELECT 
    order_key
    , COALESCE(customer_key, 0) customer_key
    , COALESCE(salesperson_person_key, 0) salesperson_person_key
    , COALESCE(picked_by_persion_key, 0) picked_by_persion_key
    , COALESCE(contact_person_key, 0) contact_person_key
    , order_date
    , expected_delivery_date
    , customer_purchase_order_number
    , picking_completed_when
FROM source_sales_order__cast_type