#!/usr/bin/python
import pandas as pd

# File paths
new_file = r'changes\_new_records.csv'
deleted_file = r'changes\_deleted_records.csv'
modified_file = r'changes\_modified_records.csv'

# Load the files
new_records = pd.read_csv(new_file)
deleted_records = pd.read_csv(deleted_file)
modified_records = pd.read_csv(modified_file)

# Standardize column names and order for all files
standard_header = ['LatD', 'LatM', 'LatS', 'NS', 'LonD', 'LonM', 'LonS', 'EW', 'City', 'State']
new_records = new_records[standard_header]
deleted_records = deleted_records[standard_header]
modified_records = modified_records[standard_header]

# Combine _new_records.csv and _deleted_records.csv for comparison
combined = pd.concat([new_records, deleted_records]).drop_duplicates()

# Remove rows in _modified_records.csv that exist in combined DataFrame
filtered_modified = modified_records.merge(combined, how='left', indicator=True)
filtered_modified = filtered_modified[filtered_modified['_merge'] == 'left_only'].drop(columns=['_merge'])

# Save the cleaned _modified_records.csv
filtered_modified.to_csv(r'changes\_modified_records.csv', index=False)

print("Processed _modified_records.csv and removed duplicates from _new_records.csv and _deleted_records.csv.")
