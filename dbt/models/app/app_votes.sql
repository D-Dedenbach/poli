{{config(
    materialized='table',
    tags=['app'],
    description='This table contains the votes cast by actors in the Danish parliament. It is derived from the raw member_votes data and includes relevant transformations for analysis.'
)}}

SELECT 
    V.vote_id
    , V.poll_id
    , P.poll_number
    , V.actor_id
    , A.actor_name
    , ATY.actor_type
    , V.updated_at
    , P.adopted
    , P.conclusion
    , PT.poll_type
    , CS.date
    , CST.case_step_type
    , CS.title AS case_step_title
    , CSS.case_step_status
    , CSTAT.case_status
    , CC.case_category
    , C.case_title
    , C.case_title_short
    , C.case_reasoning



FROM {{ ref('stg_votes') }} V
INNER JOIN {{ ref('stg_actors') }} A ON V.actor_id = A.id
INNER JOIN {{ ref('stg_actor_types')}} ATY ON A.actor_type_id = ATY.id
INNER JOIN {{ ref('stg_polls') }} P ON V.poll_id = P.poll_id
INNER JOIN {{ ref('stg_poll_types') }} PT ON P.poll_type_id = PT.id
INNER JOIN {{ ref('stg_case_step') }} CS ON P.case_step_id = CS.id
INNER JOIN {{ ref('stg_case_step_type')}} CST ON CS.case_step_type_id = CST.id
INNER JOIN {{ ref('stg_case_step_status') }} CSS ON CS.status_id = CSS.id
INNER JOIN {{ ref('stg_case')}} C ON CS.case_id = C.id
INNER JOIN {{ ref('stg_case_type') }} CT ON C.case_type_id = CT.id
INNER JOIN {{ ref('stg_case_status') }} CSTAT ON C.case_status_id = CSTAT.id
INNER JOIN {{ ref('stg_case_category')}} CC ON C.category_id = CC.id