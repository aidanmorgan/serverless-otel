import os


def determine_filesystem_path(storage_base_path:str, dataset_id: str, segment_id: str, key: str) -> str:
    data_file: str = os.path.join(storage_base_path, dataset_id, segment_id, f'{key}')

    if not os.path.exists(data_file):
        with open(data_file, mode='a'):
            pass

    return data_file
