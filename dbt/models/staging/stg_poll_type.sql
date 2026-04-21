{{config(materialized='view')}}

SELECT id as poll_type_id
    , type as poll_type
    , opdateringsdato as updated_at

FROM {{ source('raw', 'vote_types') }}