
def get_i3time(time, frac, i3time = None):
    """
    Sets or creates a I3Time with the value of a datetime object and the tenth of a nanosecond.

    Args:
        time (datetime.datetime): The time
        frac (int): Tenth of nanoseconds (DAQ units)
        i3time (dataclasses.I3Time): If you want to set an existing time, pass the time here

    Returns:
        dataclasses.I3Time: Returns the I3Time. If you passed a `i3time` it is the same object.
    """
    from icecube import dataclasses

    if i3time is None:
        i3time = dataclasses.I3Time()

    i3time.set_utc_cal_date(time.year,
                            time.month,
                            time.day,
                            time.hour,
                            time.minute,
                            time.second,
                            1e-1 * frac)

    return i3time

def get_db_time(i3time):
    """
    Returns a `datetime` object without microseconds.

    Args:
        i3time (dataclasses.I3Time): The I3Time.

    Returns:
        datetime.datetime: No microseconds.
    """

    return i3time.date_time.replace(microsecond = 0)

def get_db_frac(i3time):
    """
    Returns the frac for the DB of a given I3Time.

    Args:
        i3time (dataclasses.I3Time): The I3Time.

    Returns:
        int: Tenth of nanoseconds since the last second.
    """

    return int(i3time.utc_nano_sec * 10)


