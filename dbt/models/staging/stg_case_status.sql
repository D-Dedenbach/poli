{{config(materialized='view') }}

SELECT 
    id
    , status AS case_status
    , opdateringsdato AS updated_at

FROM {{ source('raw', 'case_status') }}