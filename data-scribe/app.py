import typer
import yaml
import sys

from data_scribe.core.factory import get_db_connector, get_llm_client
from data_scribe.core.catalog_generator import CatalogGenerator
from data_scribe.utils.writers import MarkdownWriter
from data_scribe.utils.utils import load_config
from data_scribe.utils.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# Create a Typer application instance
app = typer.Typer()


@app.command(name="db")
def scan_db(
    db_profile: str = typer.Option(None, "--db"),
    llm_profile: str = typer.Option(None, "--llm"),
    config_path: str = typer.Option("config.yaml", "--config"),
    output_filename: str = typer.Option("dbt_catalog.md", "--output"),
):
    """
    Scans a database schema, generates a data catalog using an LLM, and writes it to a Markdown file.
    """
    try:
        # Load configuration from the specified YAML file
        logger.info(f"Loading configuration from '{config_path}'...")
        config = load_config(config_path)
        logger.info("Configuration loaded successfully.")

    except FileNotFoundError:
        logger.error(f"Error: Configuration file not found at '{config_path}'.")
        raise typer.Exit(code=1)
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise typer.Exit(code=1)

    # Determine the database and LLM profiles to use
    db_profile_name = db_profile or config.get("default", {}).get("db")
    llm_profile_name = llm_profile or config.get("default", {}).get("llm")

    # Ensure that profiles are available
    if not db_profile_name or not llm_profile_name:
        logger.error(
            "Error: Missing database or LLM profile. Please specify with --db/--llm or set a default in config.yaml."
        )
        raise typer.Exit(code=1)

    logger.info(f"Using database profile: {db_profile_name}")
    logger.info(f"Using LLM profile: {llm_profile_name}")

    db_connector = None  # Initialize db_connector to None
    try:
        # Get database and LLM parameters from the configuration
        db_params = config["db_connections"][db_profile_name]
        llm_params = config["llm_providers"][llm_profile_name]
        db_type = db_params.pop("type")
        llm_provider = llm_params.pop("provider")

        # Initialize the database connector and LLM client
        logger.info(f"Initializing '{db_type}' connector...")
        db_connector = get_db_connector(db_type, db_params)
        logger.info("Database connector initialized successfully.")

        logger.info(f"Initializing '{llm_provider}' client...")
        llm_client = get_llm_client(llm_provider, llm_params)
        logger.info("LLM client initialized successfully.")

        # Generate the data catalog
        logger.info("Generating data catalog...")
        catalog_generator = CatalogGenerator(db_connector, llm_client)
        catalog_data = catalog_generator.generate_catalog(db_profile_name)
        logger.info("Data catalog generated successfully.")

        # Write the catalog to a Markdown file
        logger.info(f"Writing catalog to '{output_filename}'...")
        writer = MarkdownWriter()
        writer.write(catalog_data, output_filename, db_profile_name)
        logger.info(
            f"Successfully created data catalog at '{output_filename}'."
        )

    except KeyError as e:
        logger.error(f"Configuration Error: Missing required key: {e}")
        raise typer.Exit(code=1)
    except ValueError as e:
        logger.error(f"Value Error: {e}")
        raise typer.Exit(code=1)
    except ConnectionError as e:
        logger.error(f"Database Connection Error: {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        raise typer.Exit(code=1)
    finally:
        # Ensure the database connection is closed
        if db_connector:
            logger.info("Closing database connection.")
            db_connector.close()
