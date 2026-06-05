SELECT CS.case_step_id
    , CS.case_id
    , CS.case_step_status
    , CS.case_step_type
    , CS.title AS case_step_title
    , C.case_title
    , C.case_title_short
    , C.decision
    , C.case_reasoning
    , C.case_category
    , C.case_type
    , C.case_status
    , P.poll_id
    , P.adopted
    , P.poll_type
    , M.title
    , M.meeting_date


FROM {{ ref('stg_case_step') }} CS
INNER JOIN {{ ref('stg_case') }} C ON CS.case_id = C.case_id
INNER JOIN {{ ref('stg_poll') }} P ON CS.case_step_id = P.case_step_id
INNER JOIN {{ ref('stg_meeting') }} M ON P.meeting_id = M.meeting_id

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17