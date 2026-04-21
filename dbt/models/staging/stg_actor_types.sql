{{ config(materialized='view') }}

SELECT
    id as actor_type_id
    , opdateringsdato as updated_at
    , type as actor_type
FROM {{ source('raw', 'actor_types') }}