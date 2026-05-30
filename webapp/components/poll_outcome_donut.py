import reflex as rx
from webapp.states.poll_outcome_state import EnrichedPollData, PieSlice

def pie_cell(slice: PieSlice) -> rx.Component:
    return rx.recharts.cell(
        fill=slice["color"],
        stroke="#000000",
        stroke_width=2,
    )


def for_against_chart_container(data: EnrichedPollData, tcss_width : str = "full") ->  rx.Component:
    return rx.el.div(  
        rx.el.div(
            rx.el.span(
                "resultat",
                class_name="text-xs font-bold text-gray-400 uppercase tracking-widest",
            ),
            rx.el.span(
                rx.el.span(f"{data['total_for_votes']}", class_name="text-green-700 text-2xl font-extrabold tracking-widest"),
                rx.el.span(":", class_name="mx-2"),
                rx.el.span(f"{data['total_against_votes']}", class_name="text-red-600 text-2xl font-extrabold tracking-widest"),
                class_name="flex flex-row items-center"
            ),
            rx.el.hr(class_name="w-2/5 my-1 border-gray-700"),
            rx.el.span(
                "Stemte Blankt",
                class_name="text-xs font-bold text-gray-400 uppercase tracking-widest",
            ),
            rx.el.span(
                f"{data['total_abstain_votes']}",
                class_name="text-gray-500 text-2xl font-extrabold tracking-widest"
            ),
            class_name="absolute inset-0 flex flex-col items-center justify-center pointer-events-none z-20"
        ),
        rx.recharts.pie_chart(
            rx.recharts.pie(
                rx.foreach(data["total_for_against_array"], pie_cell),
                data=data["total_for_against_array"],
                data_key="votes",
                name_key="party_abbr",
                inner_radius="0%",
                outer_radius="85%",
                label=False,
                fill_opacity=0.8,
                start_angle=data['for_start_angle'],
                end_angle=data['against_end_angle'],
                cx="50%",
                cy="50%",

            ),
            rx.recharts.pie(
                rx.foreach(data["for_against_votes"], pie_cell),
                data = data['for_against_votes'],
                data_key = 'votes',
                name_key = 'party_abbr',
                inner_radius="70%",
                outer_radius="80%",
                label=True,
                label_line=False,
                start_angle=data['for_start_angle'],
                end_angle=data['against_end_angle'],
                cx="50%",
                cy="50%",
            ),


            rx.recharts.graphing_tooltip(),
            width="100%",
            height=300,
        ),
        class_name=f"relative w-{tcss_width} mb-8",
    ),


def absent_chart_container(data: EnrichedPollData, tcss_width : str ="3/6") ->  rx.Component:
    return rx.el.div(  
        rx.el.div(
            rx.el.span(
                "resultat",
                class_name="text-xs font-bold text-gray-400 uppercase tracking-widest",
            ),
            rx.el.span(f"{data['total_absent_votes']}",
                class_name="flex flex-row items-center"
            ),
            class_name="absolute inset-0 flex flex-col items-center justify-center pointer-events-none z-10"
        ),
        rx.recharts.pie_chart(
            rx.recharts.pie(
                data = data['absent_votes'],
                data_key = 'votes',
                name_key = 'party_abbr',
                inner_radius="60%",
                outer_radius="80%",
                fill="#9E9E9E",
                label=False,
                label_line=False,
                stroke="#000000",
                start_angle=90,
                end_angle=450,
                cx="50%",
                cy="50%",
            ),
        
            rx.recharts.graphing_tooltip(),
            width="100%",
            height=300,
        ),
        class_name=f"relative w-{tcss_width} mb-8",
    ),