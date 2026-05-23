import reflex as rx
import httpx
import json

from typing import TypedDict, List, cast
from datetime import datetime

class PartyVoteItem(TypedDict):
    party_abbr: str
    votes: int


class PollData(TypedDict):
    poll_id: int
    poll_type: str
    meeting_date: datetime
    adopted: bool
    case_title_short: str
    case_category: str
    for_votes: List[PartyVoteItem]
    against_votes: List[PartyVoteItem]
    absent_votes: List[PartyVoteItem]
    abstain_votes: List[PartyVoteItem]
    for_against_proportionality: float
    total_for_votes: int
    total_against_votes: int


def calc_pie_angle(proportionality: float, type: str = 'start', buffer: float = 3.0, clockwise: bool = False) -> float:
    """
    args:
        - proportionality: percent of total of counter-clockwise vals
        - type: start / end point of pie
        - buffer: degrees of buffer applied on each contact point
        - clockwise: if false, renders left side. If true, right side
    """
    if type not in ('start', 'end'):
        raise ValueError(f"type must be 'start' or 'end', got {type!r}")
    
    default_start_val = 90.0
    angle_shift = (proportionality - 0.5) * 180.0

    if clockwise == False:
        default_end_val = 270.0
        buffer = -1 * buffer * proportionality
    else:
        default_end_val = -90.0
        buffer = buffer * (1 - proportionality)
        

    if type == 'start':
        return default_start_val - angle_shift - buffer
    elif type == 'end':
        return default_end_val + angle_shift + buffer


class PollOutcomeState(rx.State):
    data: list[PollData] = []
    is_loading: bool = False
    error_message: str = ""

    @rx.event
    async def load_polls(self):
        self.is_loading = True
        self.error_message = ""
    
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get("http://localhost:5000/polls/latest")   
                response.raise_for_status()
                payload = response.json()

            self.data = [
                {
                    "poll_id": int(item['poll_id']),
                    "poll_type": item["poll_type"],
                    "meeting_date": datetime.strptime(item['meeting_date'], "%d-%m-%Y"),
                    "adopted": item["adopted"],
                    "case_title_short": item["case_title_short"],
                    "case_category": item["case_category"],
                    "for_votes": cast(List[PartyVoteItem], json.loads(item["for_votes"]) if item["for_votes"] else []),
                    "against_votes": cast(List[PartyVoteItem], json.loads(item["against_votes"]) if item["against_votes"] else []),
                    "absent_votes": cast(List[PartyVoteItem], json.loads(item["abstain_votes"]) if item["abstain_votes"] else []),
                    "abstain_votes": cast(List[PartyVoteItem], json.loads(item["abstain_votes"]) if item["abstain_votes"] else []),
                    "for_against_proportionality": float(item["for_against_proportionality"]),
                    "total_for_votes": int(item['total_for_votes']),
                    "total_against_votes": int(item['total_against_votes']),

                }
                for item in payload
            ]    
        
        except Exception as e:
            self.error_message = f"Could not load poll data: {e}"
            print(self.error_message)
        
        finally:
            self.is_loading = False
            print(f"data: {self.data}")

    @rx.var
    def enriched_data(self) -> List[dict]:
        return [
            {
            **item,
            'l_start_angle': calc_pie_angle(item.get('for_against_proportionality', 0.5), type='start', buffer=2.0),
            'l_end_angle': calc_pie_angle(item.get('for_against_proportionality', 0.5), type='end', buffer=2.0),
            'r_start_angle': calc_pie_angle(item.get('for_against_proportionality', 0.5), type='start', clockwise=True, buffer=2.0),
            'r_end_angle': calc_pie_angle(item.get('for_against_proportionality', 0.5), type='end', clockwise=True, buffer=2.0),
            }
            for item in self.data
        ]
