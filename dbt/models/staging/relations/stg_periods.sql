{{config(materialized='view') }}

SELECT 
    id as period_id
    , startdato as start_date
    , slutdato as end_date
    , type as period_type
    , kode as period_code
    , titel as period_title
    , opdateringsdato as updated_at
FROM {{source('raw', 'periods') }} M
