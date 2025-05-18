"""
Setup script for the Blizzard 2.1 IRS 990 XML Processing System.
"""

from setuptools import setup, find_packages

setup(
    name="blizzard",
    version="2.1.0",
    description="Production-ready IRS 990 XML Processing System",
    author="Blizzard Team",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "psycopg2",
        "lxml",
        "pandas",
        "pyyaml",
        "python-dotenv",
    ],
    entry_points={
        "console_scripts": [
            "blizzard=cli.blizzard_cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
