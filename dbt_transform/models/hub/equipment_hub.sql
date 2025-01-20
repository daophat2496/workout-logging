{{
    config(
        materialized='table'
    )
}}

-- depends_on: {{ ref('activity_running_sat') }}

SELECT DISTINCT
    esh.dw_hash_key_id
    , esh.dw_surrogate_key_id
    , esh.equipment_id
    , esh.equipment_type
FROM {{ ref("equipment_shoe_sat") }} esh
-- UNION ALL