from setuptools import setup, find_packages

setup(
    name="cmapss_pipeline",
    version="0.4.0",
    packages=find_packages(),
    install_requires=[],
    author="Jesús Vizcaíno",
    description="ETL Medallion pipeline para C-MAPSS",
    python_requires=">=3.8",
)
