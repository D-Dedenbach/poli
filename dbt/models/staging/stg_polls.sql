{{config(materialized='view') }}

SELECT id AS poll_id
       , nummer AS poll_number
       , konklusion AS conclusion
       , vedtaget AS adopted
       , m_deid AS meeting_id
       , typeid AS poll_type_id
       , opdateringsdato AS updated_at
       , sagstrinid AS case_step_id
       , kommentar AS comment


FROM {{source('raw', 'votes')}}