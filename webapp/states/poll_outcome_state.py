import reflex as rx
import httpx
import json

from typing import TypedDict, List, cast
from datetime import datetime

# Colors to display parties and vote types. Defined directly here to avoid having to change database entries whenever changing colors.
DISPLAY_COLORS = {
    "FG": "#e1f000",
    "ALT": "#2b8738",
    "EL": "#e6801a",
    "IA": "#c20e1a",
    "N": "#cb5400",
    "SF": "#e07ea8",
    "RV": "#733280",
    "JF": "#e81931",
    "S": "#a82721",
    "SIU": "#ffffff",
    "ANDRE": "#808080",
    "M": "#b48cd2",
    "V": "#254264",
    "SP": "#00678a",
    "KD": "#8A2BE2",
    "KF": "#96b226",
    "LA": "#3fb2be",
    "DD": "#7896d2",
    "DF": "#eac73e",
    "NB": "#1E1E1E",
    "BP": "#75d6c6",
    "for": "#243E26",
    "against": "#7F3333"
}

class PieSlice(TypedDict):
    party_abbr: str
    vote_type: str
    votes: int
    fill: str


class PollData(TypedDict):
    poll_id: int
    poll_type: str
    meeting_date: datetime
    adopted: bool
    case_title_short: str
    case_category: str
    for_against_votes: List[PieSlice]
    absent_votes: List[PieSlice]
    for_against_proportionality: float
    total_for_votes: int
    total_against_votes: int
    total_absent_votes: int
    total_abstain_votes: int
    total_for_against_array: List[PieSlice]

class EnrichedPollData(TypedDict):
    poll_id: int
    poll_type: str
    meeting_date: datetime
    adopted: bool
    case_title_short: str
    case_category: str
    for_against_votes: List[PieSlice]
    absent_votes: List[PieSlice]
    for_against_proportionality: float
    total_for_votes: int
    total_against_votes: int
    total_absent_votes: int
    total_abstain_votes: int
    total_for_against_array: List[PieSlice]
    for_start_angle: int
    against_end_angle: int


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
                    "meeting_date": item["meeting_date"],
                    "adopted": item["adopted"],
                    "case_title_short": item["case_title_short"],
                    "case_category": item["case_category"],
                    "for_against_votes": cast(List[PieSlice], json.loads(item["for_against_votes"]) if item["for_against_votes"] else []),
                    "absent_votes": cast(List[PieSlice], json.loads(item["absent_votes"]) if item["absent_votes"] else []),
                    "for_against_proportionality": float(item["for_against_proportionality"]),
                    "total_for_votes": int(item['total_for_votes']),
                    "total_against_votes": int(item['total_against_votes']),
                    "total_absent_votes": int(item["total_absent_votes"]),
                    "total_abstain_votes": int(item['total_abstain_votes']),
                    "total_for_against_array": cast(List[PieSlice], json.loads(item["total_for_against_array"]) if item["total_for_against_array"] else [])
                }
                for item in payload
            ]    
        
        except Exception as e:
            self.error_message = f"Could not load poll data: {e}"
            print(self.error_message)
        
        finally:
            self.is_loading = False

    @rx.var
    def enriched_data(self) -> List[EnrichedPollData]:
        def apply_colors(slices: List[PieSlice], key: str) -> List[PieSlice]:
            return [
                {**s, "fill": DISPLAY_COLORS.get(s[key], "#CCCCCC")}
                for s in slices
            ]

        return [
            {
                **item,
                'for_start_angle': calc_pie_angle(item.get('for_against_proportionality', 0.5), type='start', buffer=0.0),
                'against_end_angle': calc_pie_angle(item.get('for_against_proportionality', 0.5), type='start', buffer=0.0) + 360,
                'for_against_votes': apply_colors(item['for_against_votes'], key="party_abbr"),
                'absent_votes': apply_colors(item['absent_votes'], key="party_abbr"),
                'total_for_against_array': apply_colors(item['total_for_against_array'], key="vote_type"),
            }
            for item in self.data
        ]