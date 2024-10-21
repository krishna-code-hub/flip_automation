import os
import yaml
import argparse

def load_config(config_path='config/folder_structure.yaml'):
    """Load the folder structure configuration from a YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def create_folder_structure(config):
    """Create the folder structure based on the configuration."""
    project_name = config.get('project_name', 'ETL_Framework')
    folders = config.get('folders', [])

    for folder in folders:
        folder_path = os.path.join(project_name, folder)
        os.makedirs(folder_path, exist_ok=True)
    print(f"Project folders created successfully under '{project_name}'.")

def create_sample_files(config):
    """Create the sample files based on the configuration."""
    project_name = config.get('project_name', 'ETL_Framework')
    sample_files = config.get('sample_files', [])
    
    for sample_file in sample_files:
        path = sample_file['path']
        content = sample_file['content']
        file_path = os.path.join(project_name, path)
        with open(file_path, 'w') as f:
            f.write(content)
    print(f"Sample files created successfully under '{project_name}'.")

if __name__ == "__main__":
    # Load configuration
    parser = argparse.ArgumentParser(description="Initialize project folders and sample notebooks.")
    parser.add_argument('--config', type=str, required=True, help="Path to the folder structure configuration YAML file.")
    args = parser.parse_args()

    # Load configuration from the specified path
    config_path = args.config
    config = load_config(config_path)

    # Create folders and sample files
    create_folder_structure(config)
    #create_sample_files(config)
