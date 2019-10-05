import setuptools

REQUIRED_PACKAGES = [
    'tensorflow-data-validation',
    ]
setuptools.setup(
    name='merge_event',
    version='0.0.1',
    description='Data Science on GCP flights analysis pipelines',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    )
