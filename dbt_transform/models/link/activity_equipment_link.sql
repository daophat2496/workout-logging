{{
    config(
        materialized='table'
    )
}}

SELECT DISTINCT
    {{ generate_hash_key_column(['ad.sport_type', 'ad.id']) }} AS dw_activity_hub_hash_key_id
    , {{ generate_hash_key_column(["'shoe'", 'g.id']) }} AS dw_equipment_hub_hash_key_id
FROM {{ source("raw_data", "activity_details") }} ad
JOIN {{ source("raw_data", "gears") }} g
    ON ad.gear_id = g.id
WHERE ad.sport_type = 'Run'
-- UNION ALL