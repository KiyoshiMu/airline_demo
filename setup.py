from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = ['tensorflow-transform==0.14.0', 'tensorflow==1.15.2']

setup(
    name='flight',
    version='0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    include_package_data=True,
    description='Flights'
)