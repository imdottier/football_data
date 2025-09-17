import os
import logging
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from databricks.sdk.service.catalog import VolumeType
from utils.logging_utils import init_logging

# Loads DATABRICKS_HOST and DATABRICKS_TOKEN from your .env file
load_dotenv() 

# --- CONFIGURATION ---
LOCAL_DATABASE_PATH = "delta_temp/gold.db"
VOLUME_PATH = "/Volumes/workspace/gold/gold_data_files/" 

def get_volume_components(volume_path: str):
    """Parses a Volume path into its constituent parts."""
    parts = volume_path.strip('/').split('/')
    if parts[0] != 'Volumes' or len(parts) < 4:
        raise ValueError("Invalid Volume path. Must be /Volumes/<catalog>/<schema>/<volume_name>/...")
    return parts[1], parts[2], parts[3] # catalog, schema, volume_name


def ensure_schema_exists(client: WorkspaceClient, catalog_name: str, schema_name: str):
    """Ensures the specified schema exists in the catalog."""
    logging.info(f"Ensuring schema '{catalog_name}.{schema_name}' exists...")
    try:
        client.schemas.create(name=schema_name, catalog_name=catalog_name)
        logging.info(f"Schema '{catalog_name}.{schema_name}' created successfully.")
    except Exception as e:
        if 'already exists' in str(e).lower():
            logging.info(f"Schema '{catalog_name}.{schema_name}' already exists.")
        else:
            logging.error(f"Failed to create or verify schema '{catalog_name}.{schema_name}'.")
            raise e


def ensure_volume_exists(client: WorkspaceClient, catalog_name: str, schema_name: str, volume_name: str):
    """Ensures the specified Volume exists in the schema."""
    logging.info(f"Ensuring Volume '{volume_name}' exists in '{catalog_name}.{schema_name}'...")
    try:
        client.volumes.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=volume_name,
            volume_type=VolumeType.MANAGED
        )
        logging.info(f"Volume '{volume_name}' created successfully.")
    except Exception as e:
        if 'already exists' in str(e).lower():
            logging.info(f"Volume '{volume_name}' already exists.")
        else:
            logging.error(f"Failed to create or verify Volume '{volume_name}'.")
            raise e


def upload_directory_to_volume(local_base_dir, volume_base_path, clean_first=True):
    """
    Recursively uploads a directory to a UC Volume, ensuring the schema and volume exist first.
    If clean_first=True, deletes the target directory in the Volume before uploading.
    """
    try:
        client = WorkspaceClient()
        catalog, schema, volume = get_volume_components(volume_base_path)

        # Ensure catalog objects exist
        ensure_schema_exists(client, catalog, schema)
        ensure_volume_exists(client, catalog, schema, volume)

        logging.info("Pre-flight checks passed. Starting SDK upload.")
        logging.info(f"Source directory: '{local_base_dir}'")
        logging.info(f"Target Volume path: '{volume_base_path}'")

        # Final destination path (root under the volume)
        dir_name = os.path.basename(os.path.normpath(local_base_dir))
        target_volume_path = os.path.join(volume_base_path, dir_name).replace("\\", "/")

        # --- Cleanup step ---
        if clean_first:
            logging.info(f"Cleaning remote directory (if exists): {target_volume_path}")
            try:
                client.files.delete(file_path=target_volume_path)
                logging.info("Remote directory successfully deleted.")
            except Exception as e:
                if "not found" in str(e).lower() or "RESOURCE_DOES_NOT_EXIST" in str(e):
                    logging.info("Remote directory does not exist yet, skipping cleanup.")
                else:
                    raise

        # --- Upload step ---
        for root, dirs, files in os.walk(local_base_dir):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_path, local_base_dir)
                volume_dest_path = os.path.join(target_volume_path, relative_path).replace("\\", "/")

                logging.debug(f"Uploading: {relative_path}")
                with open(local_path, "rb") as f:
                    client.files.upload(
                        file_path=volume_dest_path,
                        contents=f,         # not file_bytes
                        overwrite=True
                    )

        logging.info("Recursive upload to Volume complete.")
        return target_volume_path

    except Exception as e:
        logging.critical(f"Upload process failed critically. Error: {e}", exc_info=True)
        return None



# --- EXECUTION ---
if __name__ == "__main__":
    init_logging()

    logging.info("--- Starting Databricks Upload Script ---")
    uploaded_path = upload_directory_to_volume(LOCAL_DATABASE_PATH, VOLUME_PATH)
    
    if uploaded_path:
        logging.info("--- Upload Process Successful ---")
        logging.info("The notebook script can now be run to register the tables.")
    else:
        logging.error("--- Upload Process Failed ---")
