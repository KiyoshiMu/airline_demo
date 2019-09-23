import os
import tempfile
import shutil
import pytest
from airline_demo import gloud_func

def test_unzip():
    zip_p = os.path.join(os.getcwd(), '201809.zip')
    csv_p = gloud_func._unzip(zip_p, '.')
    assert csv_p.endswith('.csv')