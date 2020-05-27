from setuptools import setup, find_packages
## Python 2 compat
from io import open

# Get the long description from the README file
with open("README.md", encoding='utf-8') as f:
    long_description = f.read()

test_deps = [
    "pytest>=3.1",
    "pytest-cov",
    "pyspark",
    "nbformat",
    "nbconvert",
    "ipykernel",
    "jupyter_client",
    "moto",
    "dill"
]

extras = {
    "testing": test_deps
}

setup(
    name="dscitools",
    version="0.1.2",
    description="Tools for data science with Jupyter/Pandas/Spark",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/dzeber/dscitools",
    author="David Zeber",
    author_email="dzeber@mozilla.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Framework :: IPython",
        "Framework :: Jupyter",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering :: Information Analysis"
    ],
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "numpy",
        "pandas",
        "six",
        "boto3"
    ],
    tests_require=test_deps,
    extras_require=extras
)
