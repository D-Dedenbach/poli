{{config(materialized='view')}}

SELECT id as vote_type_id
    , type as vote_type
    , opdateringsdato as updated_at

FROM {{ source('raw', 'member_vote_types') }}