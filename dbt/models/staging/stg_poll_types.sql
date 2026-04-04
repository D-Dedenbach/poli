{{config(materialized='view')}}

SELECT id
    , type as poll_type
    , opdateringsdato as updated_at

FROM {{ source('raw', 'vote_types') }}