SELECT V.poll_id
    , V.poll_type
    , STRFTIME(V.meeting_date, '%d-%m-%Y') AS meeting_date
    , V.title AS meeting_title
    , CASE WHEN V.adopted = True THEN 'Vedtaget' ELSE 'Forkastet' END AS adopted
    , C.case_step_title
    , C.case_step_status
    , C.case_step_type
    , C.case_title
    , C.case_title_short
    , C.decision
    , C.case_category
    , C.case_reasoning
    , C.case_status
    , PV.for_against_votes
    , PV.absent_votes
    , PV.total_for_votes
    , PV.total_against_votes
    , PV.total_absent_votes
    , PV.total_abstain_votes
    , COALESCE(PV.total_for_votes, 0) / ( COALESCE(PV.total_for_votes, 0) + COALESCE(PV.total_against_votes, 0)) AS for_against_proportionality
    

FROM {{ ref('int_case_info') }} C
INNER JOIN {{ ref('int_votes_per_party') }} V ON C.case_step_id = V.case_step_id
INNER JOIN {{ ref('int_poll_votes') }} PV ON V.poll_id = PV.poll_id