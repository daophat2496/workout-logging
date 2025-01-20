{{ config(materialized='table') }}

SELECT 
    {{ generate_hash_key_column(['ad.sport_type', 'ad.id']) }} AS dw_hash_key_id
    , {{ generate_surrogate_key_column(['ad.sport_type', 'ad.id']) }} AS dw_surrogate_key_id
    , ad.id AS activity_id
    , ad.sport_type AS activity_sport_type
    , ad.name AS activity_name
    , ad.distance AS activity_distance
    , ad.moving_time AS activity_time_in_seconds
    , ad.elapsed_time AS activity_elapsed_time_in_seconds
    , ad.start_date AS activity_start_datetime
    , ad.start_date_local AS activity_start_local_datetime
    , ad.utc_offset AS activity_utc_offset
    , ad.average_speed AS activity_average_speed
    , 'm/s' AS activity_average_speed_unit
    , ad.moving_time / 60 / ad.distance * 1000 AS activity_average_pace
    , 'min/km' AS activity_average_pace_unit
FROM {{ source("raw_data", "activity_details") }} ad 
WHERE ad.sport_type = 'Run'