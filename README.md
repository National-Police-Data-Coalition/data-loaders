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
python load_infra.py <city_csv_file> [-l <logging_level>]
```

> [!NOTE]
> The NPDI Leverages the United States Cities Database provided by Pareto Software to construct some of the required infrasstructure nodes. You can find this data at SimpleMaps.com.
> https://simplemaps.com/data/us-cities


### Command-line Execution

To run the script, use the following command:

```bash
python load.py <input_file> [-l <logging_level>]
```

- `<input_file>`: Path to the JSONL file to process.
- `-l <logging_level>` (optional): Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).

### Example

```bash
python load.py data/input.jsonl -l DEBUG
```

### Output

- Logs are written to the console and include detailed information about processed data, errors, and updates.
- Missing references or failed connections are saved to a timestamped file named `<timestamp>_missing_log.txt`.

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
