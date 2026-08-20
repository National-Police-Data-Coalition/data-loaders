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

## Tests

Run the unit tests with Python's built-in test runner:

```bash
python3 -m unittest discover -s tests -v
```

The current unit tests do not require a Neo4j instance. They focus on pure helper behavior, such as the Cypher fragments used by the ingest pipeline.

Optional Docker-backed integration tests use the same Neo4j image as the application environment:

```bash
RUN_NEO4J_TESTS=1 python3 -m unittest tests.test_integration_neo4j -v
```

By default, the integration test starts `neo4j:5.23-community`. Override with `NEO4J_TEST_IMAGE` only when intentionally testing another Neo4j image.

## Deterministic UIDs

Ingest handlers use deterministic UIDs when a graph node or change record must be stable across repeated loads. The UID should be derived from durable natural identifiers in the source data, generated in shared helpers, and reused anywhere the same logical entity needs to be resolved again.

General node and change UIDs are generated in `loader.utils.deterministic_uid`:

- `deterministic_node_uid(...)` creates node UIDs from a namespaced list of natural-key parts.
- `det_change_uid(...)` creates `Change.uid` values from the target node UID, source UID, scraped timestamp, and URL.
- `canonical_change_timestamp(...)` normalizes timestamps to UTC before hashing.
- `canonical_change_url(...)` centralizes URL handling for change identity.

Both helpers serialize a versioned payload and store the full lowercase SHA-256 hex digest. The namespace/version prefix keeps node UIDs and change UIDs from colliding even when the natural-key values are similar.


> [!NOTE]
> Change UIDs should always be computed from the actual UID written to the target node. In practice, an ingest handler should resolve an existing target UID when one exists, otherwise compute and assign a deterministic node UID first, then compute `change_uid` from that target UID.

### Deterministic Keys
Certain entity types require deterministic keys for matching and deduplication. For example, complaints need a stable key so that their children, (allegations, penalties, and investigations) can be efficiently matched to the correct complaint node even when those children are ingested in a separate batch or file. 

Deterministic keys must be generated from durable identifiers in the source data, and they must be stored in the graph for later resolution.

For example, the `Complaint` model has a `complaint_key` property that is used to match allegations to complaints. The complaint key is generated from the source UID and the complaint ID, which are both durable identifiers.

#### Separators

Deterministic keys are generated by concatenating the namespace and durable identifiers with a unit separator (`\x1f`) and hashing the result with SHA-256. We use a binary unit separator to avoid ambiguity when identifiers contain ordinary punctuation. The following example shows how to generate a complaint key:

```text
sha256("complaint\x1f<source_uid>\x1f<record_id>")
```

When adding deterministic UIDs for other entity types:

- Include an entity namespace as the first part, such as `complaint`, `officer`, `unit`, or `employment`.
- Use only durable identifiers that are strong enough to identify one logical entity.
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
