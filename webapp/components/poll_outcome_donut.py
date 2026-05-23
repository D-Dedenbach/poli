import reflex as rx

def pie_chart_container(data) ->  rx.Component:
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
            class_name="absolute inset-0 flex flex-col items-center justify-center pointer-events-none z-10"
        ),
        rx.recharts.pie_chart(
            rx.recharts.pie(
                data = data['for_votes'],
                data_key = 'votes',
                name_key = 'party_abbr',
                inner_radius="50%",
                outer_radius="90%",
                fill="#5F774A",
                label=True,
                label_line=True,
                stroke="#000000",
                start_angle=data['l_start_angle'],
                end_angle=data['l_end_angle'],
                cx="50%",
                cy="50%",
                min_angle=15
            ),
            rx.recharts.pie(
                data = data['against_votes'],
                data_key='votes',
                name_key='party_abbr',
                inner_radius='50%',
                outer_radius="90%",
                fill='#E53935',
                label=True,
                label_line=True,
                stroke='#000000',
                start_angle=data['r_start_angle'],
                end_angle=data['r_end_angle'],
                cx="50%",
                cy="50%",
                min_angle=15


            ),
        
            rx.recharts.graphing_tooltip(),
            width="100%",
            height=300,
        ),
        class_name="relative w-full mb-8",
    ),