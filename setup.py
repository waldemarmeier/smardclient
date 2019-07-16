from setuptools import setup

setup(
    name='smardclient',
    version='16.07.2019',
    description='totally inofficial client for smard data platform',
    url='tbd',
    author='Waldemar Meier',
    author_email='infor@waldemarmeier.com',
    license='unlicense',
    packages=['smardclient'],
    zip_safe=False,
    install_requires=[
        "requests>=2.22.0"
    ]
)