SELECT PV.poll_id
    , PV.party_abbr
    , PV.vote_type
    , PV.poll_type
    , PV.meeting_date
    , PV.title AS meeting_title
    , PV.adopted
    , C.case_step_title
    , C.case_step_status
    , C.case_step_type
    , C.case_title
    , C.case_title_short
    , C.decision
    , C.case_category
    , C.case_reasoning
    , C.case_status


FROM {{ ref('int_poll_votes') }} PV
INNER JOIN {{ ref('int_case_info') }} C ON PV.case_step_id = C.case_step_id
