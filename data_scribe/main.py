"""
This module is the main entry point for the Data Scribe application.

When executed as the main script, it invokes the Typer CLI application defined in the `app` module.
This allows the application to be run from the command line using `python -m data_scribe.main`.
"""

from data_scribe.app import app

if __name__ == "__main__":
    # If this script is run directly, execute the Typer application.
    app()
