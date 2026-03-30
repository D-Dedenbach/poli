{{config(materialized='view',
       tags=['staging'],
       schema = 'staging')}}

SELECT 
    id AS vote_id
    , typeid
    , afstemningid AS poll_id
    , akt_rid AS actor_id
    , opdateringsdato AS updated_at
FROM {{ source('raw', 'member_votes') }}