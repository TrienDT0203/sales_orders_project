WITH source_customer AS (
    SELECT *
    FROM `vit-lam-data`.wide_world_importers.sales__customers
),
    source_customer__rename_columns AS (
        SELECT
            Customer_ID AS customer_key
            , Customer_Name AS customer_name
            , Bill_To_Customer_ID AS bill_to_customer_key
            , Is_Statement_Sent AS is_statement_sent
            , Is_On_Credit_Hold AS is_on_credit_hold
            , Credit_Limit AS credit_limit
            , Standard_Discount_Percentage AS standard_discount_percentage 
            , Payment_Days AS payment_days
            , Account_Opened_Date AS account_opened_date
            , phone_number AS phone_number
            , delivery_address_line_1 AS delivery_address_line_1 
            , delivery_address_line_2 AS delivery_address_line_2 
            , Customer_Category_ID AS customer_category_key 
            , Buying_Group_ID AS buying_group_key
            , Delivery_Method_ID AS delivery_method_key
            , Delivery_City_ID AS delivery_city_key
            , Primary_Contact_Person_ID AS primary_contact_person_key
        FROM source_customer
    ),
    source_customer__cast_type AS (
        SELECT
              CAST(customer_key AS INT) customer_key
            , CAST(customer_name AS STRING) customer_name
            , CAST(bill_to_customer_key AS INT) bill_to_customer_key
            , CAST(is_statement_sent AS STRING) is_statement_sent
            , CAST(is_on_credit_hold AS STRING) is_on_credit_hold
            , CAST(credit_limit AS FLOAT64) credit_limit
            , CAST(standard_discount_percentage AS FLOAT64) standard_discount_percentage 
            , CAST(payment_days AS INT) payment_days
            , CAST(account_opened_date AS DATE) account_opened_date
            , CAST(phone_number AS STRING) phone_number
            , CAST(delivery_address_line_1 AS STRING) delivery_address_line_1 
            , CAST(delivery_address_line_2 AS STRING) delivery_address_line_2 
            , CAST(customer_category_key AS INT) customer_category_key 
            , CAST(buying_group_key AS INT) buying_group_key
            , CAST(delivery_method_key AS INT) delivery_method_key
            , CAST(delivery_city_key AS INT) delivery_city_key
            , CAST(primary_contact_person_key AS INT) primary_contact_person_key
        FROM source_customer__rename_columns
    ),
    source_sub_dim_customer AS (
        SELECT
              CAST(customer_key AS INT) customer_key
            , CAST(customer_name AS STRING) customer_name
        FROM source_customer__cast_type
    )

SELECT 
    dim_customer.customer_key
    , dim_customer.customer_name
    , dim_customer.bill_to_customer_key
    , COALESCE(sub_dim_customer.customer_name, "Invalid") AS bill_to_customer_name
    , CASE 
        WHEN dim_customer.is_statement_sent = 'true' then 'Sent'
        WHEN  dim_customer.is_statement_sent = 'false' then 'Not Sent'
        ELSE "Invalid"
        END AS is_statement_sent
    , CASE 
        WHEN dim_customer.is_on_credit_hold = 'true' then 'On credit hold'
        WHEN  dim_customer.is_on_credit_hold = 'false' then 'Not on credit hold'
        ELSE "Invalid"
        END AS is_on_credit_hold
    , dim_customer.credit_limit
    , dim_customer.standard_discount_percentage
    , dim_customer.payment_days
    , dim_customer.account_opened_date
    , dim_customer.phone_number
    , dim_customer.delivery_address_line_1
    , dim_customer.delivery_address_line_2
    , dim_customer.customer_category_key
    , COALESCE(dim_customer_category.customer_category_name, "Invalid") AS customer_category_name
    , dim_customer.buying_group_key
    , COALESCE(dim_buying_group.buying_group_name, "Invalid") AS buying_group_name
    , dim_customer.delivery_method_key
    , COALESCE(dim_delivery_method.delivery_method_name, "Invalid") AS delivery_method_name
    , dim_customer.delivery_city_key
    , COALESCE(dim_city.city_name,"Invalid")  delivery_city_name
    , COALESCE(dim_city.state_province_key,0)  delivery_state_province_key
    , COALESCE(dim_city.state_province_name,"Invalid")  delivery_state_province_name
    , COALESCE(dim_city.country_key,0)  country_key
    , COALESCE(dim_city.country_name,"Invalid")  delivery_country_name
    , dim_customer.primary_contact_person_key
    , COALESCE(dim_people.person_name,"Invalid")  primary_contact_person_name
    , COALESCE(dim_people.preferred_name,"Invalid")  preferred_name
    , COALESCE(dim_people.search_name,"Invalid")  search_name
    , COALESCE(dim_people.is_employee,"Invalid")  is_employee
    , COALESCE(dim_people.is_sales_person,"Invalid")  is_sales_person
FROM source_customer__cast_type dim_customer
LEFT JOIN source_sub_dim_customer sub_dim_customer
    ON dim_customer.bill_to_customer_key = sub_dim_customer.customer_key
LEFT JOIN `data-warehouse-400809`.`sales_order_staging`.`stg_dim_customer_category` dim_customer_category
    ON dim_customer.customer_category_key = dim_customer_category.customer_category_key
LEFT JOIN `data-warehouse-400809`.`sales_order_staging`.`stg_dim_buying_group` dim_buying_group
    ON dim_customer.buying_group_key = dim_buying_group.buying_group_key
LEFT JOIN `data-warehouse-400809`.`sales_order_staging`.`stg_dim_delivery_method` dim_delivery_method
    ON dim_customer.delivery_method_key = dim_delivery_method.delivery_method_key
LEFT JOIN `data-warehouse-400809`.`sales_order_staging`.`stg_dim_city` dim_delivery_city
    ON dim_customer.delivery_city_key = dim_city.city_key
LEFT JOIN `data-warehouse-400809`.`sales_order`.`dim_people` dim_people
    ON dim_customer.primary_contact_person_key = dim_people.person_key