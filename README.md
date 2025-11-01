# Data Scribe

Data Scribe is a command-line tool that automatically generates a data catalog from your database schema. It inspects your database, extracts table and column information, and uses a Large Language Model (LLM) to generate business-friendly descriptions for each column, outputting a clean, easy-to-read Markdown file.

This tool is designed to help data teams document their data assets efficiently, making it easier for analysts, data scientists, and other stakeholders to discover and understand the data available to them.

## Features

- **Automated Data Cataloging**: Automatically generates a data catalog from your database schema.
- **AI-Powered Descriptions**: Uses an LLM to generate meaningful, business-focused descriptions for your database columns.
- **Extensible Architecture**: Easily extendable to support different databases and LLM providers through a simple, interface-based plugin system.
- **Configuration-Driven**: Simple YAML-based configuration for managing database connections and LLM settings.
- **Markdown Output**: Generates a clean and readable Markdown file, perfect for documentation and sharing.

## How It Works

1.  **Configuration**: You provide database connection details and LLM provider settings in a `config.yaml` file.
2.  **Connection**: Data Scribe connects to the specified database using the appropriate connector.
3.  **Schema Extraction**: It inspects the database schema to retrieve a list of tables and their corresponding columns.
4.  **AI Enrichment**: For each column, it constructs a prompt and queries the configured LLM to generate a concise, business-level description.
5.  **Catalog Generation**: It compiles the schema information and the AI-generated descriptions into a structured data catalog.
6.  **Output**: The final data catalog is written to a Markdown file.

## Project Structure

```
data-scribe/
├── data-scribe/
│   ├── app.py                  # Main CLI application (Typer)
│   ├── components/
│   │   ├── db_connectors/      # Database connector implementations
│   │   └── llm_clients/        # LLM client implementations
│   ├── core/
│   │   ├── catalog_generator.py # Core logic for catalog generation
│   │   ├── factory.py          # Factory for creating connectors and clients
│   │   └── interfaces.py       # Abstract base classes for connectors and clients
│   └── utils/
│       ├── config.py           # Handles environment variable loading
│       ├── logger.py           # Logging configuration
│       ├── writers.py          # Output writers (e.g., Markdown)
│       └── utils.py            # Utility functions
├── config.yaml                 # Configuration file for DBs and LLMs
├── .env                        # Environment variables (e.g., API keys)
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.8+
- An OpenAI API key (or another supported LLM provider)

### Configuration

1.  **Create a `.env` file** in the root of the project and add your OpenAI API key:
    ```
    OPENAI_API_KEY="your-api-key-here"
    ```

2.  **Configure your database connection** in `config.yaml`. The project comes with a sample configuration for a local SQLite database.

    ```yaml
    # config.yaml

    default:
      db: dev_sqlite
      llm: openai_dev

    db_connections:
      dev_sqlite:
        type: "sqlite"
        path: "test.db" # Path to your SQLite database

      # Example for PostgreSQL
      # prod_postgres:
      #   type: "postgres"
      #   host: "prod.db.example.com"
      #   user: "admin"
      #   password: "${PROD_DB_PASSWORD}" # Example of using an env var

    llm_providers:
      openai_dev:
        provider: "openai"
        model: "gpt-3.5-turbo"
    ```

### Running the Application

Execute the `scan_db` command to generate your data catalog:

```bash
python -m data_scribe.main db
```

You can also specify different profiles or an output file:

```bash
python -m data-scribe.main db --db prod_postgres --llm openai_dev --output my_catalog.md
```

## Extensibility

Data Scribe is designed to be easily extensible. To add support for a new database or LLM provider, you need to:

1.  **Create a new connector/client class** in the `components` directory that implements the corresponding interface (`BaseConnector` or `BaseLLMClient`).
2.  **Register your new class** in the appropriate registry (`DB_CONNECTOR_REGISTRY` or `LLM_CLIENT_REGISTRY`) in `data-scribe/core/factory.py`.

That's it! The factory will then be able to instantiate your new component based on the configuration in `config.yaml`.
