"""Download BTS data from offical website."""

import datetime
import argparse
import month_download


def _replace_day(date):
    return date.replace(day=15)


def gen_date(start_date=None, end_date=None):
    """Like the 'range()' in Python, it doesn’t include the stop date in the result"""
    if start_date is None:
        start_date = datetime.date(year=2018, month=1, day=15)
    else:
        start_date = _replace_day(start_date)
    if end_date is None:
        end_date = datetime.date.today().replace(year=2018)
    else:
        end_date = _replace_day(end_date)

    date_step = datetime.timedelta(days=30)
    date = start_date
    while date.replace(day=28) < end_date:
        # the Feb usually has 28 days
        yield date
        date += date_step


def main(bucket, start_date=None, end_date=None):
    for date in gen_date(start_date, end_date):
        month_download.main(bucket, date=date)


def _parse_month(arg: str):
    info = arg.split('-')
    return datetime.date(year=int(info[0]), month=int(info[1]), day=15)


if __name__ == "__main__":
    # download()
    argparser = argparse.ArgumentParser(description='batch download data from a start month'
                                                    'to a end month')
    argparser.add_argument('--start', type=str,
                           help='format: year-month, e.g. 2018-01')
    argparser.add_argument(
        '--end', type=str, help='format: year-month, e.g. 2018-12')
    argparser.add_argument(
        '--bucket', type=str, help='your bucketID, e.g. eoseoseos')
    _args = argparser.parse_args()
    main(_args.bucket,
         _parse_month(_args.start),
         _parse_month(_args.end),
         )
