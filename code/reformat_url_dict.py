import json

# Function to reformat the JSON structure
def reformat_json(data):
    reformatted_data = []
    index = 1  # Start the index at 1

    for key, value in data.items():
        value["index"] = index  # Add the index field
        reformatted_data.append(value)  # Append the reformatted value to the list
        index += 1  # Increment the index

    return reformatted_data

# Load the data from the JSON file
with open('JSON/urldict.json', 'r') as infile:
    data = json.load(infile)

# Reformat the data
reformatted_data = reformat_json(data)

# Save the reformatted data to a new JSON file
with open('JSON/reformatted_urldict.json', 'w') as outfile:
    json.dump(reformatted_data, outfile, indent=4)

print("Reformatted JSON has been saved as 'reformatted_urldict.json'.")
