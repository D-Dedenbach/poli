{{ config(materialized='view') }}

SELECT  
    CS.id AS case_step_id
    , CS.dato as date
    , CS.folketingstidende as folketing_hansard
    , CS.folketingstidendesidenummer as folketing_hansard_page_nr
    , CS.opdateringsdato as updated_at
    , CS.sagid as case_id
    , CS.statusid as case_step_status_id
    , CSS.case_step_status
    , CS.titel as title
    , CS.typeid as case_step_type_id
    , CST.case_step_type


FROM {{source('raw', 'case_step')}} CS
INNER JOIN {{ ref('stg_case_step_type') }} CST ON CS.typeid = CST.case_step_type_id
INNER JOIN {{ ref('stg_case_step_status') }} CSS ON CS.statusid = CSS.case_step_status_id