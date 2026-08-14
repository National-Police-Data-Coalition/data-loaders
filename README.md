# NPDI Data Loader

This repository contains a script (`load.py`) designed to process and load data from JSONL files into a Neo4j database. The script provides robust error handling and logging capabilities.

## Features

- **Data Models**: Handles data related to officers, complaints, units, and other entities.
- **Neo4j Integration**: Uses the Neo4j Python driver (`neomodel`) to interact with the database.
- **Error Handling**: Handles invalid or outdated data with detailed logging.

## Prerequisites

1. **Python Dependencies**: Install required libraries using:
   ```bash
   pip install -r requirements.txt
   ```
   Ensure `dotenv`, `deepdiff`, `neomodel`, and `argparse` are installed.

2. **Neo4j Database**: A running instance of Neo4j with appropriate credentials.

3. **Environment Configuration**: Provide a `.env` file with the following variables:
   ```
   GRAPH_USER=<username>
   GRAPH_PASSWORD=<password>
   GRAPH_NM_URI=<neo4j_bolt_uri>
   ```

4. **Data Format**: The input JSONL file should contain structured data compatible with supported models (e.g., officers, units, complaints).

## Usage

### Loading the Infrastructure Nodes

The NPDI Database Leverages Infrastructure nodes to support faster searching and traversal of the database. When building your database for the first time, you will need to add these nodes.

```bash
python -m loader.load_infra <city_csv_file> [-l <logging_level>]
```

> [!NOTE]
> The NPDI Leverages the United States Cities Database provided by Pareto Software to construct some of the required infrasstructure nodes. You can find this data at SimpleMaps.com.
> https://simplemaps.com/data/us-cities


### Command-line Execution

To run the script, use the following command:

```bash
python -m loader.cli load <input_file> [-l <logging_level>]
```

- `<input_file>`: Path to the JSONL file to process.
- `-l <logging_level>` (optional): Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
- `--batch-size <size>` (optional): Number of records to process in each batch (default is 500).
- `--concurrency <num>` (optional): Number of concurrent tasks to run (default is 4).
- `--stop-on-error` (optional): Stop processing on the first error encountered.

### Example

```bash
python -m loader.cli load datasets/input.jsonl -l DEBUG
```

### Output

- Logs are written to the console and include detailed information about processed data, errors, and updates.
- Missing references or failed connections are saved to a timestamped file named `<timestamp>_missing_log.txt`.

## Deterministic Keys

Some ingest handlers need a stable key so related records from separate JSONL rows can resolve the same graph node. These keys should be deterministic, based on canonical source identifiers, and generated through a shared helper rather than rebuilt inline in each handler.

Complaint keys are generated with SHA-256 in `loader.ingest.complaint_key.build_complaint_key`:

```text
sha256("complaint\x1f<source_uid>\x1f<record_id>")
```

The `\x1f` unit separator keeps the key input unambiguous even when identifiers contain ordinary punctuation. The stored value is the full lowercase SHA-256 hex digest. Complaint ingestion writes this digest to `Complaint.complaint_key`, and allegation ingestion uses the same helper to resolve the complaint from `source_uid` and `complaint_id`.

When adding deterministic keys for other entity types, prefer the same pattern:

- Include an entity namespace as the first part, such as `complaint`, `officer`, or `unit`.
- Join canonical parts with `\x1f`.
- Store the full SHA-256 hex digest unless there is a documented reason to use a different format.
- Centralize the implementation in a helper so creators and resolvers use identical logic.

## Functions

### Key Functions

- `load_complaint(data)`: Loads complaint data into Neo4j.
- `load_officer(data)`: Processes and updates officer information.
- `load_unit(data)`: Adds or updates unit data.
- `detect_diff(item, incoming_data)`: Compares existing and new data using `DeepDiff`.



## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Acknowledgments

- [Neo4j](https://neo4j.com) for the graph database platform.
- [DeepDiff](https://zepworks.com/deepdiff/) for data comparison utilities.
