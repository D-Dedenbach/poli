import reflex as rx 
import httpx
import json

from typing import List
from datetime import datetime

from webapp.states.poll_outcome_state import PollOutcomeState, EnrichedPollData
from webapp.components.poll_outcome_donut import for_against_chart_container, absent_chart_container
    

def poll_card(poll_dict: EnrichedPollData) -> rx.Component:    

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
            rx.hstack(
                for_against_chart_container(poll_dict, tcss_width="3/6"),
                absent_chart_container(poll_dict, tcss_width="3/6"),
                rx.spacer(),
                width="100%"
            )
           


        )

    )

def index():
    return rx.vstack(
        rx.heading("Meetings", font_size="2em"),
        rx.grid(
            rx.foreach(
                PollOutcomeState.enriched_data,
                poll_card
            ),
            columns="1",
            spacing="4"

        ),
        spacing="4",
    )

app = rx.App()
app.add_page(index, on_load=PollOutcomeState.load_polls)