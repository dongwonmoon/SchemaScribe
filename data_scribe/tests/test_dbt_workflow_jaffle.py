import os
import shutil
from data_scribe.core.dbt_workflow import DbtWorkflow
from ruamel.yaml import YAML


def test_dbt_update_workflow(tmp_path):
    jaffle_path = tmp_path / "jaffle_shop"
    shutil.copytree("./jaffle_shop", jaffle_path, dirs_exist_ok=True)

    os.system(f"cd {jaffle_path} && dbt compile")
    config_path = tmp_path / "test_config.yml"
    config_content = """
default:
  llm: ollama_ci
llm_providers:
  ollama_ci:
    provider: "ollama"
    model: "llama3"
    host: "http://localhost:11434"
"""
    with open(config_path, "w") as f:
        f.write(config_content)

    workflow = DbtWorkflow(
        dbt_project_dir=str(jaffle_path),
        llm_profile="ollama_ci",
        config_path=str(config_path),
        output_profile=None,
        update_yaml=True,
        check=False,
    )
    workflow.run()

    schema_file = jaffle_path / "models/schema.yml"
    assert schema_file.exists()

    yaml = YAML()
    with open(schema_file, "r") as f:
        data = yaml.load(f)

    customer_model = next(m for m in data["models"] if m["name"] == "customers")
    first_name_col = next(
        c for c in customer_model["columns"] if c["name"] == "first_name"
    )

    assert "description" in first_name_col
    assert len(first_name_col["description"]) > 5
