import os
import dagster as dg
from dagster_airbyte import AirbyteWorkspace, build_airbyte_assets_definitions, DagsterAirbyteTranslator
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

# 1. Initialize your dbt project
dbt_project = DbtProject(
    project_dir=os.getenv("DBT_PROJECT_DIR"), 
    profiles_dir=os.getenv("DBT_PROFILES_DIR")
)
dbt_project.prepare_if_dev()

# 2. Define a custom translator to prefix your Airbyte asset keys with "raw_data"
class CustomAirbyteTranslator(DagsterAirbyteTranslator):
    def get_asset_spec(self, props):
        default_spec = super().get_asset_spec(props)
        return default_spec.replace_attributes(
            key=default_spec.key.with_prefix("raw_data")
        )

# 3. Initialize your Airbyte Workspace Resource
airbyte_workspace = AirbyteWorkspace(
    rest_api_base_url="http://localhost:8000/api/public/v1",
    configuration_api_base_url="http://localhost:8000/api/v1",
    workspace_id=os.getenv("AIRBYTE_WORKSPACE_ID"),
    username="airbyte",
    password=os.getenv("AIRBYTE_PASSWORD"),
)

# 4. Automatically discover and build all Airbyte assets for your workspace
airbyte_assets = build_airbyte_assets_definitions(
    workspace=airbyte_workspace,
    dagster_airbyte_translator=CustomAirbyteTranslator()
)

# 5. Define your dbt assets
@dbt_assets(manifest=dbt_project.manifest_path)
def my_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# 6. Bundle everything up for Dagster definitions
defs = dg.Definitions(
    assets=[*airbyte_assets, my_dbt_assets],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project.project_dir,
            profiles_dir=dbt_project.profiles_dir,
        ),
        "airbyte": airbyte_workspace,
    },
)