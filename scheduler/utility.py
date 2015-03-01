import datetime

def update_time(timestamp_dt, time_obj):
    return timestamp_dt.replace(
        hour=time_obj.hour, 
        minute=time_obj.minute, 
        second=time_obj.second, 
        microsecond=0)
