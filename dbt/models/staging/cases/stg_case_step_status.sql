{{config(materialized='view') }}

SELECT id as case_step_status_id
    , status as case_step_status
    , opdateringsdato as updated_at

FROM {{source('raw', 'case_step_status')}}