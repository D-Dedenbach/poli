{{ config(materialized='view') }}

SELECT 
    id AS role_id
    , rolle AS role
    , opdateringsdato AS updated_at


FROM {{ source('raw', 'actor_actor_roles')}}