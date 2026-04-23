{{config(materialized='view') }}

SELECT 
    M.id as meeting_id
    , M.titel as title
    , M.lokale as room
    , M.nummer as meeting_nr
    , M.dagsordenurl as url_to_agenda
    , M.starttidsbem_rkning as starting_time_note
    , M.offentlighedskode as is_public
    , M.dato as meeting_date
    , M.statusid as meeting_status_id
    , MS.meeting_status
    , M.typeid as meeting_type_id
    , MT.meeting_type
    , M.periodeid as period_id
    , M.opdateringsdato AS updated_at

FROM {{source('raw', 'meeting') }} M
INNER JOIN {{ ref('stg_meeting_status') }} MS ON M.statusid = MS.meeting_status_id
INNER JOIN {{ ref('stg_meeting_type') }} MT ON M.typeid = MT.meeting_type_id