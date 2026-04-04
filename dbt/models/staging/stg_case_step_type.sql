{{config(materialized='view') }}

SELECT id
    , type as case_step_type
    , opdateringsdato as updated_at

FROM {{ source('raw', 'case_step_type') }}