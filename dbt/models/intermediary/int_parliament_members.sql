{{ config(materialized='view') }}

WITH ft_periods AS 
(
SELECT A.actor_id
    , A.actor_name
    , A.period_id
    , A.start_date AS ft_period_start_date
    , A.end_date AS ft_period_end_date
    , P.start_date
    , P.end_date
    , P.period_title

FROM {{ ref('stg_actors') }} A
INNER JOIN {{ref('stg_periods')}} P ON A.period_id = P.period_id

WHERE actor_type = 'Parlamentarisk forsamling' AND actor_name = 'Folketinget'
),

party_memberships AS
(
SELECT AA.actor_actor_id AS party_relation_id
    , AA.from_actor_id
    , AA.to_actor_id
    , A.actor_name
    , A.group_name_short
    , AA.start_date AS party_membership_start_date
    , AA.end_date AS party_membership_end_date
FROM {{ ref('stg_actor_actor') }} AA
INNER JOIN {{ ref('stg_actors') }} A ON AA.to_actor_id = A.actor_id

WHERE AA.role = 'medlem' 
    AND A.actor_type = 'Folketingsgruppe'
)


SELECT AA.actor_actor_id
    , AA.from_actor_id
    , AA.to_actor_id
    , PM.party_relation_id
    , MAX(FT.ft_period_start_date, COALESCE(PM.party_membership_start_date, now())) AS record_validity_start_date
    , MIN(FT.ft_period_end_date, COALESCE(PM.party_membership_end_date, now())) AS record_validity_end_date
    , AA.role
    , A.actor_name AS member_name
    , FT.start_date
    , FT.end_date
    , FT.ft_period_start_date
    , FT.ft_period_end_date
    , PM.party_membership_start_date
    , PM.party_membership_end_date
    , FT.period_title
    , PM.group_name_short
FROM {{ ref('stg_actor_actor') }} AA
INNER JOIN {{ ref('stg_actors') }} A ON AA.from_actor_id = A.actor_id
INNER JOIN ft_periods FT ON AA.to_actor_id = FT.actor_id
INNER JOIN party_memberships PM ON A.actor_id = PM.from_actor_id

-- The party membership record must be active at the time of the parliamentary period the actor status is mentioned.
-- This is bad design as there is no safeguard for all party memberships starting after the parliamentary period start or ending before parliamentary year end
-- In theory this can lead to periods of time where in my data, members of parliament would not have a valid party membership
-- This would become apparent with null joins on vote data
WHERE PM.party_membership_start_date BETWEEN AA.start_date AND COALESCE(AA.end_date, now())

