
import datetime


def now():
    return str(datetime.datetime.now())


def timebox():
    date = now()
    date = date[8:10] + "." + date[11:13]
    return date
