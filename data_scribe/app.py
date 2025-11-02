import typer
import yaml
import sys
import functools

from data_scribe.core.factory import get_db_connector, get_llm_client
from data_scribe.core.catalog_generator import CatalogGenerator
from data_scribe.core.dbt_catalog_generator import DbtCatalogGenerator
from data_scribe.utils.writers import MarkdownWriter, DbtMarkdownWriter
from data_scribe.utils.utils import load_config
from data_scribe.utils.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# Create a Typer application instance
app = typer.Typer()


def load_and_validate_config(config_path: str):
    try:
        logger.info(f"Loading configuration from '{config_path}'...")
        config = load_config(config_path)
        logger.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found at '{config_path}'.")
        raise typer.Exit(code=1)
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise typer.Exit(code=1)


def init_llm(config, llm_profile_name: str):
    try:
        llm_params = config["llm_providers"][llm_profile_name]
        llm_provider = llm_params.pop("provider")
        logger.info(f"Initializing LLM provider '{llm_provider}'...")
        llm_client = get_llm_client(llm_provider, llm_params)
        logger.info("LLM client initialized successfully.")
        return llm_client
    except KeyError as e:
        logger.error(f"Missing LLM configuration key: {e}")
        raise typer.Exit(code=1)


def handle_exceptions(func):
    """데코레이터: try/except 반복 제거"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (KeyError, ValueError, ConnectionError) as e:
            logger.error(f"{type(e).__name__}: {e}")
            raise typer.Exit(code=1)
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            raise typer.Exit(code=1)

    return wrapper


@app.command(name="db")
@handle_exceptions
def scan_db(
    db_profile: str = typer.Option(None, "--db"),
    llm_profile: str = typer.Option(None, "--llm"),
    config_path: str = typer.Option("config.yaml", "--config"),
    output_filename: str = typer.Option("db_catalog.md", "--output"),
):
    """
    Scans a database schema, generates a data catalog using an LLM, and writes it to a Markdown file.
    """
    config = load_and_validate_config(config_path)

    db_profile_name = db_profile or config.get("default", {}).get("db")
    llm_profile_name = llm_profile or config.get("default", {}).get("llm")
    if not db_profile_name or not llm_profile_name:
        logger.error(
            "Missing profiles: specify --db/--llm or set defaults in config.yaml."
        )
        raise typer.Exit(code=1)

    db_params = config["db_connections"][db_profile_name]
    db_type = db_params.pop("type")
    db_connector = get_db_connector(db_type, db_params)
    llm_client = init_llm(config, llm_profile_name)

    logger.info("Generating data catalog...")
    catalog = CatalogGenerator(db_connector, llm_client).generate_catalog(
        db_profile_name
    )
    MarkdownWriter().write(catalog, output_filename, db_profile_name)
    logger.info(f"Catalog written to '{output_filename}'.")
    db_connector.close()


@app.command(name="dbt")
@handle_exceptions
def scan_dbt(
    dbt_project_dir: str = typer.Option(..., "--project-dir"),
    llm_profile: str = typer.Option(None, "--llm"),
    config_path: str = typer.Option("config.yaml", "--config"),
    output_filename: str = typer.Option("dbt_catalog.md", "--output"),
):
    config = load_and_validate_config(config_path)
    llm_profile_name = llm_profile or config.get("default", {}).get("llm")
    llm_client = init_llm(config, llm_profile_name)

    logger.info("Generating dbt catalog...")
    catalog = DbtCatalogGenerator(llm_client).generate_catalog(dbt_project_dir)
    DbtMarkdownWriter().write(catalog, output_filename, dbt_project_dir)
    logger.info(f"DBT catalog written to '{output_filename}'.")
