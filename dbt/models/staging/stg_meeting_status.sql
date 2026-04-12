{{config(materialized='view') }}

SELECT 
    id as meeting_status_id
    , status as meeting_status
    , opdateringsdato AS updated_at

FROM {{source('raw', 'meeting_status') }}