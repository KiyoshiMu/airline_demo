"""A test for download_data"""
import tempfile
import os
import datetime
import pytest
from airline_demo.preparation import batch_download, month_download

def test_gen_date():
    dates = list(batch_download.gen_date(datetime.date(2018, 1, 1), datetime.date(2019, 1, 1)))
    assert len(dates) == 12 and dates[0].year == 2018 and dates[0].month == 1 and \
           dates[-1].year == 2018 and dates[-1].month == 12

def test_batch_download():
    dst = tempfile.mkdtemp()
    fp = month_download.download(datetime.date(2018, 1, 1), dst)
    assert os.stat(fp).st_size > 10000

@pytest.mark.skip
def test_unzip():
    zip_p = os.path.join('data/201809.zip')
    csv_p = month_download._unzip(zip_p, '.')
    assert csv_p.endswith('.csv')