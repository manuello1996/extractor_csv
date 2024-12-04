#!/usr/bin/python
import pandas as pd

# File paths
deleted_file = r'changes\_deleted_records.csv'
new_file = r'changes\_new_records.csv'
modified_file = r'changes\_modified_records.csv'

# Standard header in the correct order for new_records
standard_header = ['LatD', 'LatM', 'LatS', 'NS', 'LonD', 'LonM', 'LonS', 'EW', 'City', 'State']

# Function to preprocess the file and remove extra leading commas
def preprocess_file(file_path):
    # Open the file and read its contents
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    # Remove leading commas from each line
    cleaned_lines = [line.lstrip(',') for line in lines]
    
    # Write the cleaned data back to the file
    with open(file_path, 'w') as file:
        file.writelines(cleaned_lines)

# Function to replace the header and standardize
def standardize_header(file_path):
    # Preprocess the file to clean leading commas
    preprocess_file(file_path)
    
    # Read the cleaned file
    df = pd.read_csv(file_path, skiprows=1, header=None)  # Skip the first line as it's being replaced
    
    # Assign the standard header
    df = df.iloc[:, :len(standard_header)]  # Trim extra columns if necessary
    df.columns = standard_header
    
    # Count the number of non-empty fields in each row
    df['filled_fields'] = df.notna().sum(axis=1)

    # Filter rows where more than 2 fields are filled
    df = df[df['filled_fields'] > 2].drop(columns=['filled_fields'])

    # Save the standardized file
    df.to_csv(file_path, index=False, header=standard_header)

# Function to rearrange columns in _new_records.csv
def rearrange_new_records(file_path):
    # Preprocess the file to clean leading commas
    standardize_header(file_path)
    
    # Read the cleaned file
    df = pd.read_csv(file_path, skiprows=1, header=None)  # Skip the first line as it's being replaced
    
    # Assign a temporary header to handle current structure
    temp_header = ['City', 'State', 'LatD', 'LatM', 'LatS', 'NS', 'LonD', 'LonM', 'LonS', 'EW']
    df.columns = temp_header[:len(df.columns)]
    
    # Rearrange columns to match the standard order
    df = df.reindex(columns=['LatD', 'LatM', 'LatS', 'NS', 'LonD', 'LonM', 'LonS', 'EW', 'City', 'State'])
    
    # Save the rearranged file
    df.to_csv(file_path, index=False, header=standard_header)

     # Count the number of non-empty fields in each row
    df['filled_fields'] = df.notna().sum(axis=1)

    # Filter rows where more than 2 fields are filled
    df = df[df['filled_fields'] > 2].drop(columns=['filled_fields'])

    # Save the standardized file
    df.to_csv(file_path, index=False, header=standard_header)

# Process files
standardize_header(deleted_file)  # Standardize _deleted_records.csv
rearrange_new_records(new_file)   # Rearrange columns in _new_records.csv
standardize_header(modified_file) # Standardize _modified_records.csv

print("Processing completed: headers standardized and columns rearranged in _new_records.csv.")
