{{config(materialized='view') }}

SELECT 
    id AS case_type_id
    , type as case_type
    , opdateringsdato as updated_at

FROM {{ source('raw', 'case_type') }}