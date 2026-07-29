from setuptools import setup


setup(
    name="trishula-csv-analyzer",
    version="0.3.0",
    description="Local Snowflake CSV session and funnel analyzer",
    python_requires=">=3.9",
    py_modules=[
        "benchmark",
        "cli",
        "converter",
        "errors",
        "event_parser",
        "generate_mock_data",
        "insights",
        "server",
        "visualizer",
    ],
    install_requires=[
        "duckdb>=0.9.0",
        "pandas>=2.0.0",
        "pyarrow>=12.0.0",
        "rich>=13.0.0",
        "fastapi>=0.100.0",
        "uvicorn>=0.20.0",
        "python-multipart>=0.0.6",
    ],
    extras_require={"test": ["pytest>=8.0"]},
    entry_points={
        "console_scripts": [
            "trishula=cli:main",
            "trishula-web=server:main",
        ]
    },
)
