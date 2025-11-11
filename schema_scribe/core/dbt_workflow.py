"""
This module defines the `DbtWorkflow` class, which encapsulates the complex,
multi-mode logic for the `schema-scribe dbt` command.

The workflow handles parsing a dbt project, generating a data catalog, and then
performing one of several actions based on user input:
- Updating dbt `schema.yml` files with AI-generated content.
- Running CI checks for documentation completeness or drift.
- Writing a full catalog to an external format (e.g., Markdown).
"""

from typing import Optional
import typer

from schema_scribe.core.factory import get_writer, get_db_connector
from schema_scribe.core.dbt_catalog_generator import DbtCatalogGenerator
from schema_scribe.components.writers.dbt_yaml_writer import DbtYamlWriter
from schema_scribe.core.workflow_helpers import (
    load_config_from_path,
    init_llm,
)
from schema_scribe.core.exceptions import CIError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbtWorkflow:
    """
    Manages the end-to-end workflow for the `schema-scribe dbt` command.

    This class acts as a state machine, orchestrating the dbt documentation
    process based on the combination of CLI flags provided by the user. It is
    responsible for loading configuration, initializing components, generating
    the dbt catalog, and routing the result to the correct handler based on the
    selected mode (e.g., `--update`, `--check`, `--drift`, or `--output`).
    """

    def __init__(
        self,
        dbt_project_dir: str,
        db_profile: str | None,
        llm_profile: Optional[str],
        config_path: str,
        output_profile: Optional[str],
        update_yaml: bool,
        check: bool,
        interactive: bool,
        drift: bool,
    ):
        """
        Initializes the DbtWorkflow with parameters from the CLI.

        Args:
            dbt_project_dir: The path to the target dbt project directory.
            db_profile: The DB profile, required only for `--drift` mode to
                        connect to the live database.
            llm_profile: The LLM profile for generating descriptions.
            config_path: The path to the main `config.yaml` file.
            output_profile: The output profile, used when generating a catalog
                            file (e.g., Markdown).
            update_yaml: Flag for `--update` mode to modify `schema.yml` files.
            check: Flag for `--check` mode to validate documentation coverage.
            interactive: Flag for `--interactive` mode to confirm changes.
            drift: Flag for `--drift` mode to detect inconsistencies with the DB.
        """
        self.dbt_project_dir = dbt_project_dir
        self.db_profile_name = db_profile
        self.llm_profile_name = llm_profile
        self.config_path = config_path
        self.output_profile_name = output_profile
        self.update_yaml = update_yaml
        self.check = check
        self.interactive = interactive
        self.drift = drift
        self.config = load_config_from_path(self.config_path)

    def run(self):
        """
        Executes the dbt scanning and documentation workflow based on CLI flags.

        The method follows a clear execution path:
        1.  **Initialization**: Sets up the LLM client. If in `--drift` mode, it
            also initializes a database connector to compare against the live
            database schema.
        2.  **Catalog Generation**: Invokes `DbtCatalogGenerator` to parse the
            dbt project and create a structured representation of its models,
            sources, and columns.
        3.  **Action Dispatch**: Based on the CLI flags, it dispatches the
            catalog to the appropriate handler:
            -   For `--check`, `--drift`, `--update`, or `--interactive`, it uses
                `DbtYamlWriter` to process `schema.yml` files. In CI modes
                (`--check`, `--drift`), it will raise a `CIError` on failure.
            -   For `--output`, it uses a writer from the factory to generate a
                standalone catalog file (e.g., Markdown, Confluence).
            -   If no action flag is provided, it simply logs a message.
        """
        # 1. Initialize components based on the execution mode.
        llm_profile_name = self.llm_profile_name or self.config.get(
            "default", {}
        ).get("llm")
        llm_client = init_llm(self.config, llm_profile_name)

        db_connector = None
        if self.drift:
            if not self.db_profile_name:
                logger.error("Drift mode requires a --db profile")
                raise typer.Exit(code=1)
            try:
                logger.info(
                    f"Initializing DB connection '{self.db_profile_name}' for drift check..."
                )
                db_params = self.config["db_connections"][self.db_profile_name]
                db_type = db_params.pop("type")
                db_connector = get_db_connector(db_type, db_params)
            except Exception as e:
                logger.error(
                    f"Failed to connect to database for drift check: {e}"
                )
                raise typer.Exit(code=1)

        # 2. Generate the dbt project catalog.
        logger.info(
            f"Generating dbt catalog for project: {self.dbt_project_dir}"
        )
        catalog_gen = DbtCatalogGenerator(
            llm_client=llm_client, db_connector=db_connector
        )
        catalog = catalog_gen.generate_catalog(
            dbt_project_dir=self.dbt_project_dir, run_drift_check=self.drift
        )

        # 3. Determine the action mode and dispatch to the correct handler.
        action_mode = None
        if self.drift:
            action_mode = "drift"
        elif self.check:
            action_mode = "check"
        elif self.interactive:
            action_mode = "interactive"
        elif self.update_yaml:
            action_mode = "update"

        if action_mode:
            self._handle_yaml_update(action_mode, catalog)
        elif self.output_profile_name:
            self._handle_file_output(catalog)
        else:
            logger.info(
                "Catalog generated. No output specified (--output, --update, or --check)."
            )

        if db_connector:
            db_connector.close()

    def _handle_yaml_update(self, mode: str, catalog: dict):
        """Handles modes that interact with dbt schema.yml files."""
        logger.info(f"Running in --{mode} mode...")
        writer = DbtYamlWriter(dbt_project_dir=self.dbt_project_dir, mode=mode)
        updates_needed = writer.write(catalog)

        if mode in ["check", "drift"]:
            if updates_needed:
                log_msg = (
                    "documentation is outdated"
                    if self.check
                    else "documentation drift was detected"
                )
                logger.error(f"CI CHECK FAILED: {log_msg}.")
                raise CIError(f"CI CHECK FAILED: {log_msg}.")
            else:
                log_msg = "is up-to-date" if self.check else "has no drift"
                logger.info(
                    f"CI CHECK PASSED: All dbt documentation {log_msg}."
                )
        else:
            logger.info(f"dbt schema.yml {mode} process complete.")

    def _handle_file_output(self, catalog: dict):
        """Handles writing the catalog to a file using an output profile."""
        try:
            logger.info(f"Using output profile: '{self.output_profile_name}'")
            writer_params = self.config["output_profiles"][
                self.output_profile_name
            ]
            writer_type = writer_params.pop("type")
            writer = get_writer(writer_type)

            writer_kwargs = {
                "project_name": self.dbt_project_dir,
                **writer_params,
            }
            writer.write(catalog, **writer_kwargs)
            logger.info(
                f"dbt catalog written successfully using profile: '{self.output_profile_name}'."
            )
        except (KeyError, ValueError, IOError) as e:
            logger.error(
                f"Failed to write catalog using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)
