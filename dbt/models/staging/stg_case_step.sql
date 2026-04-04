{{ config(materialized='view') }}

SELECT  
    id,
    dato as date,
    folketingstidende as folketing_hansard,
    folketingstidendesidenummer as folketing_hansard_page_nr,
    opdateringsdato as updated_at,
    sagid as case_id,
    statusid as status_id,
    titel as title,
    typeid as case_step_type_id


FROM {{source('raw', 'case_step')}}