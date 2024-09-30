import json
from collections import defaultdict
from typing import Dict, List, Union, Any

def analyze_json_duplicates(json_file: str, key_paths: Dict[str, List[Union[str, List[str]]]]):
    """
    Analyze a JSON file for duplicates based on specified keys.

    Parameters:
    - json_file (str): The path to the JSON file.
    - key_paths (dict): A dictionary specifying the keys to analyze.
      The keys of this dictionary are the names of the counters (e.g., 'doi', 'url').
      The values are lists that represent the path to the value in the JSON data.
      Each element in the list can be a string (key) or a list (for iterating over a list of items).

    Example of key_paths:
    {
        'doi': ['urls', ['doi']],
        'url': ['urls', ['paperlink']],
        'sciencealert': ['sciencealert']
    }
    """
    # Load the JSON data
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Initialize counters
    counters = {key: defaultdict(list) for key in key_paths.keys()}

    # Handle both list and dict data structures
    if isinstance(data, dict):
        data_items = data.items()
    elif isinstance(data, list):
        data_items = enumerate(data)
    else:
        raise ValueError("Unsupported JSON data structure. Must be a list or a dictionary.")

    # Iterate over each item in the data
    for item_id, item_data in data_items:
        # For each key we want to analyze
        for counter_name, path in key_paths.items():
            values = extract_values_from_path(item_data, path)
            for value in values:
                if value:
                    counters[counter_name][value].append(item_id)

    # Find duplicates
    duplicates = {
        key: {value: ids for value, ids in counter.items() if len(ids) > 1}
        for key, counter in counters.items()
    }

    # Calculate totals
    totals = {
        f"total_{key}_duplicates": sum(len(ids) for ids in duplicate_counter.values())
        for key, duplicate_counter in duplicates.items()
    }

    # Print the results
    for key, duplicate_counter in duplicates.items():
        print(f"\nDuplicate {key.upper()}s with Item IDs:")
        for value, ids in duplicate_counter.items():
            print(f"{value}: {len(ids)} occurrences, found in items: {ids}")

    # Print total duplicates
    for key, total in totals.items():
        print(f"\nTotal duplicates for {key.split('_')[1].upper()}s: {total}")

def extract_values_from_path(data_item: Any, path: List[Union[str, List[str]]]) -> List[Any]:
    """
    Extract values from a data item based on the provided path.

    Parameters:
    - data_item (Any): The data item (dict) to extract values from.
    - path (List[Union[str, List[str]]]): The path to navigate through the data item.

    Returns:
    - List[Any]: A list of extracted values.
    """
    values = [data_item]
    for key in path:
        if isinstance(key, list):
            # We need to iterate over a list in the data
            new_values = []
            for value in values:
                if isinstance(value, list):
                    for item in value:
                        for sub_key in key:
                            extracted_value = item.get(sub_key)
                            new_values.append(extracted_value)
                elif isinstance(value, dict):
                    for sub_key in key:
                        extracted_value = value.get(sub_key)
                        new_values.append(extracted_value)
            values = new_values
        else:
            # Navigate to the next key in all current values
            new_values = []
            for value in values:
                if isinstance(value, dict):
                    new_value = value.get(key)
                    if new_value is not None:
                        new_values.append(new_value)
            values = new_values
    return values

# Example usage:

# For the first data structure (from 'urldict.json')
key_paths_1 = {
    'doi': ['urls', ['doi']],
    'url': ['urls', ['paperlink']],
    'sciencealert': ['sciencealert']
}

analyze_json_duplicates('urldict.json', key_paths_1)

# For the second data structure (from 'urldictcleandoistwo.json')
urldict_keys = {
    'doi': ['paperlinks', ['doi']],
    'url': ['paperlinks', ['paperlink']],
    'sciencealert': ['url']
}

analyze_json_duplicates('urldictcleandoistwo.json', urldict_keys)
