SELECT C.poll_id
    , C.poll_type
    , meeting_date
    , C.title AS meeting_title
    , CASE WHEN C.adopted = True THEN 'Vedtaget' ELSE 'Forkastet' END AS adopted
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
    , JSON_ARRAY(
        
        JSON_OBJECT('party_abbr', '', 
                    'vote_type', 'for', 
                    'votes', PV.total_for_votes,
                    'fill', '#C3DDC5'),
        JSON_OBJECT('party_abbr', '', 
                    'vote_type', 'against', 
                    'votes', PV.total_against_votes,
                    'fill', '#F1CCCC')
    ) AS total_for_against_array
    , COALESCE(PV.total_for_votes, 0) / ( COALESCE(PV.total_for_votes, 0) + COALESCE(PV.total_against_votes, 0)) AS for_against_proportionality
    

FROM {{ ref('int_case_info') }} C
INNER JOIN {{ ref('int_poll_votes') }} PV ON C.poll_id = PV.poll_id