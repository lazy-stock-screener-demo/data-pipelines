from datetime import datetime, date, timedelta
import re


def transformFromDatetime2Date(now):
    if isinstance(now, datetime):
        now = now.date()
    return now


def getYear():
    return datetime.today().year


def getYearOrRange(year_from_now_start, year_from_now_end=0):
    if not year_from_now_end:
        return str(int(getYear()) - year_from_now_start)
    else:
        return [
            str(int(getYear()) - ele)
            for ele in range(year_from_now_start, year_from_now_end + 1)
        ]


def getDate():
    # print(datetime.today())
    # print(datetime.today() - timedelta(1))
    return transformFromDatetime2Date(datetime.today() - timedelta(1)).strftime(
        "%Y%m%d"
    )


def getSeasons():
    # https://stackoverflow.com/questions/16139306/determine-season-given-timestamp-in-python-using-datetime
    now = transformFromDatetime2Date(datetime.today())
    this_year = now.year
    seasons = [
        ("QTR1", (date(this_year, 1, 1), date(this_year, 3, 31))),
        ("QTR2", (date(this_year, 4, 1), date(this_year, 6, 30))),
        ("QTR3", (date(this_year, 7, 1), date(this_year, 9, 30))),
        ("QTR4", (date(this_year, 10, 1), date(this_year, 12, 31))),
    ]
    return next(season for season, (start, end) in seasons if start <= now <= end)


def extract_pars(url):
    # idea is from https://stackoverflow.com/questions/6260777/python-regex-to-parse-string-and-return-tuple
    match = re.compile(
        r"""
        (?P<base>^.*\/data\/?)
        (?P<cik>\d*)
        \/(?P<submission>\d*)
        \/\w+
        """,
        re.VERBOSE,
    ).match(url)
    base = match.group("base")
    cik = match.group("cik")
    submission = match.group("submission")
    return (base, cik, submission)


def decorator_config(func):
    def enhanced_func(*args, **kwargs):
        build_pars_func = args[0]
        data_pars = args[1]
        ticker = None
        if len(args) > 2:
            ticker = args[2]
        enhanced_data_pars = dict(**data_pars, ticker=ticker)
        pars = build_pars_func(data_pars)
        return func(pars)

    return enhanced_func