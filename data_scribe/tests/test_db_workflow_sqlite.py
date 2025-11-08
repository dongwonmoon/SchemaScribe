import os
from data_scribe.core.db_workflow import DbWorkflow
from data_scribe.components.db_connectors.sqlite_connector import SQLiteConnector


def setup_module(module):
    db_path = "test_sqlite_ci.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = SQLiteConnector()
    conn.connect({"path": db_path})
    conn.cursor.execute("CREATE TABLE users (id INTEGER, email TEXT)")
    conn.cursor.execute("INSERT INTO users VALUES (1, 'ci@test.com')")
    conn.connection.commit()
    conn.close()


def test_sqlite_db_workflow(tmp_path):
    config_path = tmp_path / "test_config.yml"
    output_file = tmp_path / "ci_catalog.md"

    config_content = """
default:
  db: sqlite_ci
  llm: ollama_ci
llm_providers:
  ollama_ci:
    provider: "ollama"
    model: "llama3"
    host: "http://localhost:11434"
db_connections:
  sqlite_ci:
    type: "sqlite"
    path: "test_sqlite_ci.db"
output_profiles:
  markdown_ci:
    type: "markdown"
    output_filename: "{}"
""".format(
        str(output_file)
    )

    with open(config_path, "w") as f:
        f.write(config_content)

    workflow = DbWorkflow(
        config_path=str(config_path),
        db_profile="sqlite_ci",
        llm_profile="ollama_ci",
        output_profile="markdown_ci",
    )
    workflow.run()

    assert output_file.exists()
    content = output_file.read_text()
    assert "users" in content
    assert "email" in content
    assert "AI-Generated" in content


def teardown_module(module):
    if os.path.exists("test_sqlite_ci.db"):
        os.remove("test_sqlite_ci.db")
