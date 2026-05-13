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
    , JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'party_abbr', PV.party_abbr,
                'vote_type', PV.vote_type,
                'vote_count', PV.vote_count
            )
     ) AS votes

FROM {{ ref('int_poll_votes') }} PV
INNER JOIN {{ ref('int_case_info') }} C ON PV.case_step_id = C.case_step_id

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
