from setuptools import setup, find_packages, find_namespace_packages

setup(
    name='serverless_otel',
    packages=find_namespace_packages(
        where='libs',
        include=['serverless_otel*']
    ),
    package_dir={"": "libs"},
    version='0.0.1',
)
