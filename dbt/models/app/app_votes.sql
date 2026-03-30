{{config(
    materialized='table',
    schema='app',
    tags=['app'],
    description='This table contains the votes cast by actors in the Danish parliament. It is derived from the raw member_votes data and includes relevant transformations for analysis.'
)}}

SELECT 
    V.vote_id
    , V.poll_id
    , V.actor_id
    , A.actor_name
    , A.actor_type_id
    , V.updated_at

FROM {{ ref('stg_votes') }} V
INNER JOIN {{ ref('stg_actors') }} A ON V.actor_id = A.id