{{config(materialized='view') }}

SELECT 
    id as meeting_type_id
    , type as meeting_type
    , opdateringsdato AS updated_at

FROM {{source('raw', 'meeting_type') }}