"""
This module defines the `DbWorkflow` class, which encapsulates the end-to-end
logic for the `schema-scribe db` command.

The workflow is responsible for orchestrating the process of connecting to a
database, inspecting its schema, generating a data catalog (with AI-powered
descriptions), and writing the result to a specified output format.
"""

from typing import Optional
import typer

from schema_scribe.core.factory import get_db_connector, get_writer
from schema_scribe.core.catalog_generator import CatalogGenerator
from schema_scribe.core.workflow_helpers import load_config_from_path, init_llm
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbWorkflow:
    """
    Manages the end-to-end workflow for the `schema-scribe db` command.

    This class acts as the central orchestrator for database scanning tasks.
    It is instantiated with CLI parameters, loads the necessary configuration,
    and uses factory functions to dynamically initialize the required components
    (database connector, LLM client, and writer).

    The lifecycle of the workflow is managed by the `run` method.
    """

    def __init__(
        self,
        config_path: str,
        db_profile: Optional[str],
        llm_profile: Optional[str],
        output_profile: Optional[str],
    ):
        """
        Initializes the DbWorkflow with configuration from the CLI.

        This constructor stores the profile names and loads the main `config.yaml`
        file, making the configuration accessible for the `run` method.

        Args:
            config_path: The path to the configuration file (e.g., 'config.yaml').
            db_profile: The name of the database profile to use from the config.
            llm_profile: The name of the LLM profile to use from the config.
            output_profile: The name of the output profile to use from the config.
        """
        self.config_path = config_path
        self.db_profile_name = db_profile
        self.llm_profile_name = llm_profile
        self.output_profile_name = output_profile
        self.config = load_config_from_path(self.config_path)

    def run(self):
        """
        Executes the main database scanning and documentation workflow.

        This method orchestrates the following steps:
        1.  Determines the correct database and LLM profiles to use, prioritizing
            CLI arguments over defaults in the config file.
        2.  Initializes the database connector and LLM client using the factory.
        3.  Instantiates the `CatalogGenerator` and runs it to produce the
            structured data catalog.
        4.  Initializes a writer and uses it to save the catalog if an output
            profile is specified.
        5.  Ensures the database connection is always closed, even if errors occur,
            by using a `finally` block.
        """
        # Determine which database and LLM profiles to use.
        db_profile_name = self.db_profile_name or self.config.get(
            "default", {}
        ).get("db")
        llm_profile_name = self.llm_profile_name or self.config.get(
            "default", {}
        ).get("llm")

        if not db_profile_name or not llm_profile_name:
            logger.error(
                "Missing profiles. Please specify --db and --llm, or set defaults in config.yaml."
            )
            raise typer.Exit(code=1)

        # Instantiate the database connector and LLM client.
        db_params = self.config["db_connections"][db_profile_name]
        db_type = db_params.pop("type")
        db_connector = get_db_connector(db_type, db_params)

        llm_client = init_llm(self.config, llm_profile_name)

        try:
            # Generate the data catalog.
            logger.info("Generating data catalog for the database...")
            catalog = CatalogGenerator(
                db_connector, llm_client
            ).generate_catalog(db_profile_name)

            # Write the catalog to the specified output, if provided.
            if not self.output_profile_name:
                logger.info(
                    "Catalog generated. No --output profile specified, so not writing to a file."
                )
                return

            # Initialize the writer based on the output profile.
            writer_params = self.config["output_profiles"][
                self.output_profile_name
            ]
            writer_type = writer_params.pop("type")
            writer = get_writer(writer_type)

            # Prepare arguments for the writer.
            writer_kwargs = {
                "db_profile_name": db_profile_name,
                "db_connector": db_connector,
                **writer_params,
            }
            writer.write(catalog, **writer_kwargs)
            logger.info(
                f"Catalog written successfully using output profile: '{self.output_profile_name}'."
            )
        except (KeyError, ValueError, IOError) as e:
            logger.error(
                f"Failed to write catalog using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)
        finally:
            # Ensure the database connection is always closed.
            db_connector.close()
