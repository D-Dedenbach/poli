WITH poll_meeting as 
(
SELECT P.poll_id
    , P.case_step_id
    , V.vote_id
    , V.vote_type
    , V.actor_id
    , P.adopted
    , P.poll_type
    , M.meeting_date
    , M.period_id
    , M.title

FROM {{ ref('stg_poll') }} P
INNER JOIN {{ ref('stg_meeting') }} M ON P.meeting_id = M.meeting_id
INNER JOIN {{ ref('stg_vote') }} V ON V.poll_id = P.poll_id
)

SELECT PM.*
    , MEM.party_abbr
    , MEM.party_name



FROM poll_meeting PM
INNER JOIN {{ ref('int_parliament_members') }} MEM ON PM.actor_id = MEM.member_id 
                                                    AND PM.meeting_date BETWEEN MEM.record_validity_start_date AND MEM.record_validity_end_date
