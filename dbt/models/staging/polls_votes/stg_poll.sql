{{config(materialized='view') }}

SELECT P.id AS poll_id
       , P.nummer AS poll_number
       , P.konklusion AS conclusion
       , P.vedtaget AS adopted
       , P.m_deid AS meeting_id
       , P.typeid AS poll_type_id
       , PT.poll_type
       , P.opdateringsdato AS updated_at
       , P.sagstrinid AS case_step_id
       , P.kommentar AS comment


FROM {{source('raw', 'votes')}} P
INNER JOIN {{ ref('stg_poll_type') }} PT ON P.typeid = PT.poll_type_id