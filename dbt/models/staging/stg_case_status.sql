{{config(materialized='view') }}

SELECT 
    id AS case_status_id
    , status AS case_status
    , opdateringsdato AS updated_at

FROM {{ source('raw', 'case_status') }}