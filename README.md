# BlackSail Logs

BlackSail Logs is a powerful structured logging system that provides schema-based log management with multiple storage backend support and seamless integration with popular logging frameworks.

## Features

- **Schema Management**
  - YAML-based schema definitions
  - Dynamic schema loading with file watching
  - Support for basic and complex data types
  - Field validation and indexing options

- **Multiple Storage Backends**
  - PostgreSQL
  - SQLite
  - ClickHouse
  - MySQL

- **RESTful API**
  - Schema CRUD operations
  - Log insertion (single and batch)
  - Log querying and counting
  - Schema listing and retrieval

- **Zap Integration**
  - Custom hook for structured logging
  - Batch log processing
  - Efficient field indexing

## Requirements

- Go 1.21 or later
- PostgreSQL 13+ (for PostgreSQL backend)
- ClickHouse 22+ (for ClickHouse backend)
- Docker and docker-compose (for containerized deployment)

## Installation

```bash
go get github.com/yourusername/blacksail-logs
```

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/yourusername/blacksail-logs.git
cd blacksail-logs
```

2. Start the required services using Docker:
```bash
docker-compose up -d
```

3. Initialize the database:
```bash
make init-db
```

4. Create a schema definition (example in `examples/app_logs.yaml`):
```yaml
name: app_logs
version: 1
fields:
  - name: timestamp
    type: datetime
    index: true
  - name: level
    type: string
    index: true
  - name: message
    type: string
  - name: metadata
    type: object
```

5. Run the example application:
```bash
go run examples/main.go
```

## Configuration

Configuration is managed through `configs/config.yaml`:

```yaml
storage:
  type: postgres  # or sqlite, clickhouse, mysql
  dsn: "host=localhost port=5432 user=postgres password=postgres dbname=logs sslmode=disable"

api:
  host: "localhost"
  port: 8080

schema:
  path: "./examples"
  watch: true
```

## API Endpoints

- `GET /api/v1/schemas` - List all schemas
- `POST /api/v1/schemas` - Create a new schema
- `GET /api/v1/schemas/{name}` - Get schema details
- `PUT /api/v1/schemas/{name}` - Update schema
- `DELETE /api/v1/schemas/{name}` - Delete schema
- `POST /api/v1/logs` - Insert logs
- `GET /api/v1/logs` - Query logs
- `GET /api/v1/logs/count` - Count logs

## Development

1. Install development tools:
```bash
make install-tools
```

2. Run tests:
```bash
make test
```

3. Run linter:
```bash
make lint
```

4. Build:
```bash
make build
```

## Docker Support

Build the Docker image:
```bash
docker build -t blacksail-logs .
```

Run using docker-compose:
```bash
docker-compose up -d
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 