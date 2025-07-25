"""
Setup file for sql_data_generator package.
"""

from setuptools import setup, find_packages

setup(
    name="sql_data_generator",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "faker",
        "python-dateutil",
        "inflection"
    ]
) 