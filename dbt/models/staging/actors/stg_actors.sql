{{ config(materialized='view') }}

SELECT
    A.id as actor_id
    , A.typeid as actor_type_id
    , T.actor_type
    , A.gruppenavnkort as group_name_short
    , TRIM(A.navn) as actor_name
    , A.fornavn as first_name
    , A.efternavn as last_name
    , A.periodeid as period_id
    , A.opdateringsdato as updated_at
    , A.startdato as start_date
    , A.slutdato as end_date
    , A.biografi as biography
    
FROM {{ source('raw', 'actors') }} A
INNER JOIN {{ ref('stg_actor_types') }} T ON A.typeid = T.actor_type_id