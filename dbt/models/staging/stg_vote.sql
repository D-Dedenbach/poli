{{config(materialized='view',
       tags=['staging'],
       schema = 'staging')}}

SELECT 
    V.id AS vote_id
    , V.typeid AS vote_type_id
    , VT.vote_type
    , V.afstemningid AS poll_id
    , V.akt_rid AS actor_id
    , V.opdateringsdato AS updated_at
FROM {{ source('raw', 'member_votes') }} V
INNER JOIN {{ ref('stg_vote_type') }} VT ON V.typeid = VT.vote_type_id