import json

# Path to the JSON file
input_file_path = 'cleaned_data.json'
output_file_path = 'transformed_cleaned_data.json'

# Open and read the JSON file
with open(input_file_path, 'r') as file:
    data = json.load(file)

# Transform the data to use 'index' as the key
transformed_data = {entry['index']: entry for entry in data}

# Save the transformed data to a new JSON file
with open(output_file_path, 'w') as file:
    json.dump(transformed_data, file, indent=4)

print(f"Transformed data saved to {output_file_path}")
