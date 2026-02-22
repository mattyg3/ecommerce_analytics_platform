import shutil
from pathlib import Path

def delete_folder(filepath):
    # Delete the folder and everything inside
    if filepath.exists() and filepath.is_dir():
        shutil.rmtree(filepath)
        print(f"✅ Deleted folder: {filepath}")
    else:
        print(f"Folder does not exist: {filepath}")

def find_repo_root(start_path: Path = None) -> Path:
    """
    Walks up from start_path to find the repository root (contains .git)
    """
    path = start_path or Path(__file__).resolve()
    for parent in [path] + list(path.parents):
        if (parent / ".git").exists():
            return parent
    raise FileNotFoundError("Cannot find repository root (no .git folder found).")

BASE_DIR = find_repo_root()

CLICKSTREAM_DIR = Path("/home/surff/spark_data/clickstream/raw") #WSL path
delete_folder(CLICKSTREAM_DIR)

ORDERS_DIR = Path("/home/surff/spark_data/orders/raw") #WSL path
delete_folder(ORDERS_DIR)

CLICKSTREAM_CHECKPOINT = Path("/home/surff/spark_data/checkpoints/clickstream_ingest") #WSL path
delete_folder(CLICKSTREAM_CHECKPOINT)

LANDING_OUTPUT = BASE_DIR / "data" / "landing"
delete_folder(LANDING_OUTPUT)

BRONZE = BASE_DIR / "data" / "bronze"
delete_folder(BRONZE)