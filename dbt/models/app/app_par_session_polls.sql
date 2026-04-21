{{config(
    materialized='view',
    tags=['app'],
    description='This view restricts the scope of polls to meetings in the parliamentary chamber (i.e. full parliament, not workgroups) and cases connected to that'
)}}

SELECT 
    P.poll_id
    , C.id AS case_id
    , P.conclusion
    , P.adopted
    , M.meeting_date
    , M.title as meeting_title
    , PT.poll_type
    , CS.title as case_step_title
    , CSS.case_step_status
    , CST.case_step_type
    , CSTAT.case_status
    , CC.case_category
    , CT.case_type
    , C.case_title
    , C.case_title_short
    , C.case_reasoning

FROM {{ref('stg_poll')}} P
INNER JOIN {{ref('stg_case_step')}} CS ON P.case_step_id = CS.id
INNER JOIN {{ref("stg_case")}} C ON CS.case_id = C.case_id
INNER JOIN {{ref('stg_meeting')}} M ON P.meeting_id = M.meeting_id
INNER JOIN {{ref('stg_poll_type')}} PT ON P.poll_type_id = PT.id
INNER JOIN {{ ref('stg_case_step_type')}} CST ON CS.case_step_type_id = CST.id
INNER JOIN {{ ref('stg_case_step_status') }} CSS ON CS.status_id = CSS.id
INNER JOIN {{ ref('stg_case_type') }} CT ON C.case_type_id = CT.id
INNER JOIN {{ ref('stg_case_status') }} CSTAT ON C.case_status_id = CSTAT.id
INNER JOIN {{ ref('stg_case_category')}} CC ON C.category_id = CC.id

WHERE 1=1 
    -- meeting type 1 means meeting in plenum, this excludes for instance committee meetings
    AND M.meeting_type_id = 1