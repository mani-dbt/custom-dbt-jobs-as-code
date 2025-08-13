import re
from io import StringIO
from typing import Any, Dict, Optional

from loguru import logger
from ruamel.yaml import YAML

from dbt_jobs_as_code.schemas.job import JobDefinition

def normalize_job_name_for_identifier(job_name: str) -> str:
    """Convert job name to a valid identifier format.
    Returns:
        Normalized name: lowercase with spaces replaced by underscores
    """
    # Convert to lowercase and replace spaces with underscores
    normalized = job_name.lower().replace(" ", "_")
    
    # Replace any non-alphanumeric characters (except underscores) with underscores
    normalized = re.sub(r'[^a-z0-9_]', '_', normalized)
    
    # Remove consecutive underscores
    normalized = re.sub(r'_+', '_', normalized)
    
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')

    return normalized

def apply_templated_fields(
    job_dict: Dict[Any, Any], template_config: Dict[str, str]
) -> Dict[Any, Any]:
    """Apply templated fields to a job dictionary based on a template configuration.

    Args:
        job_dict: The job dictionary to modify
        template_config: A dictionary mapping field paths to template values

    Returns:
        The modified job dictionary
    """

    def set_nested_value(d: Dict[Any, Any], path: str, value: str):
        parts = path.split(".")
        for part in parts[:-1]:
            if part not in d:
                d[part] = {}
            d = d[part]
        d[parts[-1]] = value

    result = job_dict.copy()
    for field_path, template_value in template_config.items():
        set_nested_value(result, field_path, template_value)

    return result


def export_jobs_yml(
    jobs: list[JobDefinition], include_linked_id: bool = False, template_file: Optional[str] = None
):
    """Export a list of job definitions to YML

    Args:
        jobs: List of job definitions to export
        include_linked_id: Whether to include the linked ID in the export
        template_file: Path to a YAML file containing field templates to apply
    """
    yaml = YAML()
    template_config = {}
    if template_file:
        with open(template_file, "r") as f:
            # Replace curly braces with custom delimiters in template file content
            content = f.read()
            content = escape_curly_braces(content)
            template_config = yaml.load(content)

    export_yml = {"jobs": {}}
    user_keys = set()
    duplicate_jobs = []

    for id, cloud_job in enumerate(jobs):
        base_key = normalize_job_name_for_identifier(cloud_job.name)
        yaml_key = base_key

        counter = 1
        while yaml_key in user_keys:
            yaml_key = f"{base_key}_{counter}"
            counter += 1

        # Track if we had to add a counter due to duplicate job names
        if counter > 1:
            duplicate_jobs.append({
                'name': cloud_job.name,
                'identifier': yaml_key,
                'base_key': base_key
            })

        user_keys.add(yaml_key)

#        yaml_key = cloud_job.identifier if cloud_job.identifier else f"import_{id + 1}"

        job_dict = cloud_job.to_load_format(include_linked_id)

        if template_config:
            job_dict = apply_templated_fields(job_dict, template_config)

        export_yml["jobs"][yaml_key] = job_dict

    # Check if there were any duplicate job names and raise an error if so
    if duplicate_jobs:
        error_message = "❌ Duplicate job names detected. Job names must be unique to avoid identifier conflicts. Please rename jobs to have unique names \n"
        for dup in duplicate_jobs:
            error_message += f"  - Job '{dup['name']}' \n"
        logger.error(error_message.strip())
        raise ValueError("Duplicate job names detected. Please rename jobs to have unique names.")

    print(
        "# yaml-language-server: $schema=https://raw.githubusercontent.com/dbt-labs/dbt-jobs-as-code/main/src/dbt_jobs_as_code/schemas/load_job_schema.json"
    )
    print("")

    yaml.width = 4096
    yaml.block_seq_indent = 2
    # Convert back to standard template syntax before output
    stream = StringIO()
    yaml.dump(export_yml, stream)
    yaml_str = stream.getvalue()
    yaml_str = unescape_curly_braces(yaml_str)
    print(yaml_str, end="")


def escape_curly_braces(yaml_str: str) -> str:
    """Escape curly braces in YAML string.

    We escape and unsescape curly braces because we want to use `{{ }}` in the template file
    but we want to avoid that the YAML parser interprets them as a dictionary.

    Args:
        yaml_str: YAML string to escape

    Returns:
        Escaped YAML string with curly braces replaced
    """
    return yaml_str.replace("{", "<[<").replace("}", ">]>")


def unescape_curly_braces(yaml_str: str) -> str:
    return yaml_str.replace("<[<", "{").replace(">]>", "}")
