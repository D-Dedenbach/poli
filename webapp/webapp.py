import reflex as rx 
import httpx
import json

from typing import List
from datetime import datetime

from webapp.webapp_utils import calc_pie_angle



class State(rx.State):
    data: List[dict] = []
    loading: bool = False
    error: str = ""

    @rx.event
    def fetch_latest_polls(self):
        print("fetch_latest_polls called!")  # Debug
        self.loading = True
        self.error = ""
        try:
            with httpx.Client() as client:
                print("Making request to FastAPI...")  # Debug
                response = client.get("http://localhost:5000/polls/latest")
                print(f"Response status: {response.status_code}")  # Debug
                if response.status_code == 200:
                    data = response.json()

                    # Transform json arrays into lists
                    for d in data:
                        d['for_votes'] = json.loads(d['for_votes']) if d['for_votes'] else []
                        d['against_votes'] = json.loads(d['against_votes']) if d['against_votes'] else []
                        d['absent_votes'] = json.loads(d['absent_votes']) if d['absent_votes'] else []
                        d['abstain_votes'] = json.loads(d['abstain_votes']) if d['abstain_votes'] else []
                        d['l_start_angle'] = calc_pie_angle(d.get('for_against_proportionality', 0.5), type='start', buffer=2.0)
                        d['l_end_angle'] = calc_pie_angle(d.get('for_against_proportionality', 0.5), type='end', buffer=2.0)
                        d['r_start_angle'] = calc_pie_angle(d.get('for_against_proportionality', 0.5), type='start', clockwise=True, buffer=2.0)
                        d['r_end_angle'] = calc_pie_angle(d.get('for_against_proportionality', 0.5), type='end', clockwise=True, buffer=2.0)

                    self.data = data if isinstance(data, list) else data.get("polls", [])
                else:
                    self.error = f"API Error: {response.status_code}"
        except Exception as e:
            print(f"Error fetching polls: {e}")  # Debug
            self.error = f"Failed to fetch polls: {str(e)}"
        finally:
            self.loading = False
    

def poll_card(poll_dict: dict):
    def custom_label(content: dict):
        return f"{content['party_abbr']}: {content['votes']}"
    

    return rx.card(
        rx.vstack(
            rx.hstack(
    
                rx.badge(poll_dict["case_category"],
                        color_scheme="gray"    ),
                # Badge to mark poll type
                rx.badge(poll_dict["poll_type"],
                        color_scheme="blue"),
                # Badge to mark whether the poll as adopted
                rx.badge(poll_dict["adopted"],
                        color_scheme=rx.match(
                            poll_dict["adopted"],
                            ("Vedtaget", "green"),
                            ("Forkastet", "red"),
                            "gray"
                     ))
            ),
            rx.hstack(
                rx.text(poll_dict["meeting_date"]),
                rx.spacer(),
                rx.text(poll_dict['case_title_short'])

            ),
            rx.recharts.pie_chart(
                rx.recharts.pie(
                    data = poll_dict['for_votes'],
                    data_key = 'votes',
                    name_key = 'party_abbr',
                    inner_radius="60%",
                    fill="#5F774A",
                    label=True,
                    label_line=True,
                    stroke="#000000",
                    start_angle=poll_dict['l_start_angle'],
                    end_angle=poll_dict['l_end_angle'],

                ),

                rx.recharts.pie(
                    data = poll_dict['against_votes'],
                    data_key='votes',
                    name_key='party_abbr',
                    inner_radius='60%',
                    fill='#E53935',
                    label=True,
                    label_line=True,
                    stroke='#000000',
                    start_angle=poll_dict['r_start_angle'],
                    end_angle=poll_dict['r_end_angle'],


                ),
                rx.recharts.graphing_tooltip(),
                width="100%",
                height=300,

            )
        )

    )

def index():
    return rx.vstack(
        rx.heading("Meetings", font_size="2em"),
        rx.grid(
            rx.foreach(
                State.data,
                poll_card
            ),
            columns="1",
            spacing="4"

        ),
        spacing="4",
    )

app = rx.App()
app.add_page(index, on_load=State.fetch_latest_polls)