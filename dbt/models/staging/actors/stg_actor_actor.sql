SELECT 
    AA.id AS actor_actor_id
    , AA.fraakt_rid AS from_actor_id
    , AA.tilakt_rid AS to_actor_id
    , AA.startdato AS start_date
    , AA.slutdato AS end_date
    , AA.rolleid AS role_id
    , AAR.role
    , AA.opdateringsdato AS updated_at


FROM {{ source('raw', 'actor_actor') }} AA
INNER JOIN {{ ref('stg_actor_actor_role') }} AAR ON AA.rolleid = AAR.role_id