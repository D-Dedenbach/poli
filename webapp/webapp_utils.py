
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



