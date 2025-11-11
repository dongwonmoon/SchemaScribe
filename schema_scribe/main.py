"""
This module is the main entry point for the Schema Scribe CLI application.

This script is the target for the `schema-scribe` console script entry point
defined in `pyproject.toml`. When executed, it invokes the Typer CLI application
defined in the `app` module.
"""

from schema_scribe.app import app

if __name__ == "__main__":
    # If this script is run directly, execute the Typer application.
    app()
