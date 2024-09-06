import os

def determine_sqlite_file_path(storage_base_path: str, dataset_id: str, segment_id: str) -> str:
    return os.path.join(storage_base_path, dataset_id, segment_id, f'{segment_id}.sqlite')
