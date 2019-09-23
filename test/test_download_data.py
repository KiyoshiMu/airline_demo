"""A test for download_data"""
import tempfile
import os
import datetime
import pytest
from airline_demo import download_data

def test_gen_date():
    dates = list(download_data.gen_date(datetime.date(2018, 1, 1), datetime.date(2019, 1, 1)))
    assert len(dates) == 12 and dates[0].year == 2018 and dates[0].month == 1 and \
           dates[-1].year == 2018 and dates[-1].month == 12

@pytest.mark.skip
def test_batch_download():
    dst = tempfile.mkdtemp()
    ret = []
    for fp in download_data.batch_download(datetime.date(2018, 1, 1), 
                                    datetime.date(2018, 2, 1), dst):
        ret.append(fp)
    assert os.stat(ret[0]).st_size > 10000
