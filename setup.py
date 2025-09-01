from setuptools import setup, find_packages

setup(
    name="mdm-framework",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pydantic>=2.0.0",
        "sqlalchemy>=2.0.0",
        "psycopg2-binary>=2.9.0",
        "neo4j>=5.0.0",
        "pandas>=2.0.0",
        "pyyaml>=6.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "isort>=5.0.0",
            "mypy>=1.0.0",
            "flake8>=6.0.0",
        ]
    },
    python_requires=">=3.9",
    description="A flexible MDM framework with ETL capabilities",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
)
