{{config(materialized='view') }}

SELECT 
    id as meeting_id
    , titel as title
    , lokale as room
    , nummer as meeting_nr
    , dagsordenurl as url_to_agenda
    , starttidsbem_rkning as starting_time_note
    , offentlighedskode as is_public
    , dato as meeting_date
    , statusid as meeting_status_id
    , typeid as meeting_type_id
    , periodeid as period_id
    , opdateringsdato AS updated_at

FROM {{source('raw', 'meeting') }}