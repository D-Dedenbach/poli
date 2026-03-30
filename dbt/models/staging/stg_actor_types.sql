{{ config(materialized='view') }}

SELECT
    id,
    opdateringsdato as updated_at,
    type
FROM {{ source('raw', 'actor_types') }}