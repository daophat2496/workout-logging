{{ config(materialized='table') }}

SELECT 
    {{ generate_hash_key_column(["'shoe'", 'g.id']) }} AS dw_hash_key_id
    , {{ generate_surrogate_key_column(["'shoe'", 'g.id']) }} AS dw_surrogate_key_id
    , 'shoe' AS equipment_type
    , g.id AS equipment_id
    , g.name AS equipment_name
    , g.model_name AS equipment_model_name
    , g.brand_name AS equipment_brand_name
    , g.distance AS equipment_distance_in_meters
    , g.distance / 1000 AS equipment_distance_in_kilometers
FROM {{ source("raw_data", "gears") }} g