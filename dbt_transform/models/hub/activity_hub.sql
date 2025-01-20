{{
    config(
        materialized='table'
    )
}}

-- depends_on: {{ ref('activity_running_sat') }}

SELECT DISTINCT
    atcs.dw_hash_key_id
    , atcs.dw_surrogate_key_id
    , atcs.activity_id
    , atcs.activity_sport_type
FROM {{ ref("activity_running_sat") }} atcs
-- UNION ALL