"""Welcome to Reflex! This file outlines the steps to create a basic app."""

import reflex as rx

from rxconfig import config


class State(rx.State):
    """The app state."""
    votes: list[dict] = []

    def load_votes(self):
        import requests
        response = requests.get("http://localhost:5000/votes/15777")
        data = response.json()
        self.votes = data["votes"]

def vote_card(vote: dict) -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.heading(vote["poll_id"]),
            rx.text(vote["updated_at"]),
            rx.badge(vote["actor_id"]),
            spacing="2",
        ),
        border_left="4px solid blue",
        padding="1rem",
        margin_bottom="1rem",
    )

def index() -> rx.Component:

    return rx.vstack(
        rx.heading("Parliamentary Votes"),
        rx.button("Load Votes", on_click=State.load_votes),
        rx.vstack(
            rx.foreach(
                State.votes,
                vote_card
            ),
            spacing="4",
        ),
    )


app = rx.App()
app.add_page(index)
