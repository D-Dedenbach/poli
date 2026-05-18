SELECT PV.poll_id
    , PV.poll_type
    , STRFTIME(PV.meeting_date, '%d-%m-%Y') AS meeting_date
    , PV.title AS meeting_title
    , CASE WHEN PV.adopted = True Then 'Vedtaget' ELSE 'Forkastet' END AS adopted
    , C.case_step_title
    , C.case_step_status
    , C.case_step_type
    , C.case_title
    , C.case_title_short
    , C.decision
    , C.case_category
    , C.case_reasoning
    , C.case_status
    , FORV.type_votes AS for_votes
    , AGAINSTV.type_votes AS against_votes
    , ABSENTV.type_votes AS absent_votes
    , ABSTAINV.type_votes AS abstain_votes
    , COALESCE(FORV.total_type_votes, 0) / ( COALESCE(FORV.total_type_votes, 0) + COALESCE(AGAINSTV.total_type_votes, 0)) AS for_against_proportionality


FROM {{ ref('int_case_info') }} C
INNER JOIN {{ ref('int_poll_votes') }} PV ON C.case_step_id = PV.case_step_id
LEFT JOIN {{ ref('int_poll_votes') }} FORV ON PV.poll_id = FORV.poll_id AND FORV.vote_type = 'For'
LEFT JOIN {{ ref('int_poll_votes') }} AGAINSTV ON AGAINSTV.poll_id = PV.poll_id AND AGAINSTV.vote_type = 'Imod'
LEFT JOIN {{ ref('int_poll_votes') }} ABSENTV ON ABSENTV.poll_id = PV.poll_id AND ABSENTV.vote_type = 'Fravær'
LEFT JOIN {{ ref('int_poll_votes') }} ABSTAINV ON ABSTAINV.poll_id = PV.poll_id AND ABSTAINV.vote_type = 'Hverken for eller imod'

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19