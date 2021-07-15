#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "dbt-ibm_dbt"
package_version = "0.0.1"
description = """The ibm_dbt adapter plugin for dbt (data build tool) using pyodbc"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Kay",
    author_email="",
    url="",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include\ibm-dbt\macros\*.sql',
            'include\ibm-dbt\dbt_project.yml',
        ]
    },
    install_requires=[
        "dbt",
        "pyodbc"
    ]
)

