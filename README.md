# MDM Framework

A flexible Master Data Management (MDM) framework with ETL capabilities.

## Features

- ETL Pipeline with multiple connectors (SQL, Graph DB, Text files)
- Data mapping configuration through JSON
- Generic data cleansing framework
- Match & Merge with binary decision tree
- Golden record management with history tracking

## Installation

```bash
pip install -e ".[dev]"
```

## Project Structure

```
mdm-framework/
├── src/
│   ├── etl/
│   │   ├── connectors/
│   │   └── pipeline.py
│   ├── models/
│   │   ├── landing/
│   │   ├── staging/
│   │   └── master/
│   ├── mapping/
│   ├── cleansing/
│   ├── matching/
│   └── merging/
├── tests/
└── config/
```

## Usage

[Documentation pending]

## Development

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

2. Install development dependencies:
```bash
pip install -e ".[dev]"
```

3. Run tests:
```bash
pytest tests/
```
