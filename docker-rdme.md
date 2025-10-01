# Using the Docker Image

This project includes a Dockerfile to build a Docker image for the JSONL loader script. Follow these steps to build and run the Docker container.

1. Build the image (from your project root, where your Dockerfile lives)

```bash
docker build -t jsonl-loader .
```

2. Run the container

This step runs the container as a one-off process, passing in your local JSONL file and the database connection details.


```bash
docker run \
  -e DATABASE_URL="bolt://neo4j:Vm*i.a3ip9B.6Q@host.docker.internal:7687" \
  -v "$(pwd)/datasets/source/input.jsonl:/datasets/input.jsonl:ro" \
  jsonl-loader /datasets/input.jsonl -w 8 -l INFO

```

Explanation of the flags:
- --rm → remove the container automatically when it exits.

- -e DATABASE_URL=... → tells your app how to connect to Neo4j.

- -v "$(pwd)/datasets/source/input.jsonl:/datasets/input.jsonl:ro" → mounts your local JSONL file into the container at /datasets/input.jsonl. The format is -v <local path>:<container path>:ro (read-only).

- jsonl-loader → the image you just built.

- /datasets/input.jsonl -w 8 -l INFO → arguments passed through to your script.