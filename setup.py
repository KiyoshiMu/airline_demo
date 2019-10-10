from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = ['tensorflow-transform', 'apache-beam[gcp]']

setup(
    name='flight',
    version='0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    include_package_data=True,
    description='Flights'
)