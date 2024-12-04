#!/usr/bin/python
import pandas as pd
import os

# Function to load CSV
def load_csv(file_path):
    return pd.read_csv(file_path)

# Function to clean column names by stripping leading/trailing spaces
def clean_column_names(df):
    # Remove leading/trailing spaces from column names
    df.columns = df.columns.str.strip()
    return df

# Function to compare two CSVs
def compare_csvs(old_df, new_df):
    # Clean the column names in both DataFrames
    old_df = clean_column_names(old_df)
    new_df = clean_column_names(new_df)
    
   
    # Ensure that the columns we want to compare exist in both DataFrames
    comparison_columns = ['LatD', 'LatM', 'LatS', 'NS', 'LonD', 'LonM', 'LonS', 'EW']
    
    missing_columns_old = [col for col in comparison_columns if col not in old_df.columns]
    missing_columns_new = [col for col in comparison_columns if col not in new_df.columns]
    
    if missing_columns_old:
        print(f"Warning: Missing columns in old DataFrame: {missing_columns_old}")
    if missing_columns_new:
        print(f"Warning: Missing columns in new DataFrame: {missing_columns_new}")
    
    # Merge on unique columns (e.g., "City", "State" as unique identifiers)
    merged_df = pd.merge(old_df, new_df, on=["City", "State"], how='outer', indicator=True)
    
    # Detect changes
    new_records = merged_df[merged_df['_merge'] == 'right_only']
    deleted_records = merged_df[merged_df['_merge'] == 'left_only']
    # modified_records = merged_df[merged_df['_merge'] == 'both']
    
    # Check for changes in the location fields, if all comparison columns exist
    if all(col in merged_df.columns for col in comparison_columns):
        modified_records = modified_records[modified_records[comparison_columns].apply(tuple, axis=1) != 
                                            modified_records[comparison_columns].apply(tuple, axis=1)]
    else:
        print("Comparison columns are missing in the merged DataFrame.")
        modified_records = pd.DataFrame()  # Return an empty DataFrame for modified records if columns are missing

    return new_records, deleted_records

def find_modified_records(old_df, new_df):
    # Merge the old and new dataframes based on City and State
    merged_df = pd.merge(old_df, new_df, on=["City", "State"], how='outer', suffixes=('_old', '_new'), indicator=True)

    # Check for rows where any coordinate data has changed, or new records with no existing coordinates
    modified_records = merged_df[
        (merged_df['LatD_old'] != merged_df['LatD_new']) |
        (merged_df['LatM_old'] != merged_df['LatM_new']) |
        (merged_df['LatS_old'] != merged_df['LatS_new']) |
        (merged_df['NS_old'] != merged_df['NS_new']) |
        (merged_df['LonD_old'] != merged_df['LonD_new']) |
        (merged_df['LonM_old'] != merged_df['LonM_new']) |
        (merged_df['LonS_old'] != merged_df['LonS_new']) |
        (merged_df['EW_old'] != merged_df['EW_new']) |
        # Include new records (right_only) even if coordinates are missing
        ((merged_df['_merge'] == 'right_only') & 
         ((merged_df['LatD_new'].notna()) | merged_df['LatD_new'].isna())) &
         ((merged_df['LonD_new'].notna()) | merged_df['LonD_new'].isna())
    ]
    
    # Select relevant columns and rename them to match the original format
    modified_records = modified_records[['LatD_new', 'LatM_new', 'LatS_new', 'NS_new', 'LonD_new', 'LonM_new', 'LonS_new', 'EW_new', 'City', 'State']]
    
    # Rename columns to match original file format
    modified_records.columns = ['LatD', 'LatM', 'LatS', 'NS', 'LonD', 'LonM', 'LonS', 'EW', 'City', 'State']
    
    return modified_records

# Function to save the results
def save_changes(new_records, deleted_records, modified_records, result_folder):
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    
    # Save new records
    if not new_records.empty:
        new_records.to_csv(os.path.join(result_folder, '_new_records.csv'), index=False)
    
    # Save deleted records
    if not deleted_records.empty:
        deleted_records.to_csv(os.path.join(result_folder, '_deleted_records.csv'), index=False)
    
    # Save modified records
    if not modified_records.empty:
        modified_records.to_csv(os.path.join(result_folder, '_modified_records.csv'), index=False)

# Main function to compare old and new CSV files
def main(old_file, new_file, result_folder):
    # Load CSVs
    old_df = load_csv(old_file)
    new_df = load_csv(new_file)
    
    # Compare CSVs
    new_records, deleted_records = compare_csvs(old_df, new_df)
    modified_records = find_modified_records(old_df, new_df)
    
    # Save the results
    save_changes(new_records, deleted_records, modified_records, result_folder)

if __name__ == "__main__":
    # Set file paths
    old_file = '_city.csv'  # Path to the previous CSV file
    new_file = '_new_city.csv'       # Path to the new CSV file
    result_folder = 'changes' # Folder to save the result files
    
    main(old_file, new_file, result_folder)
    import clean
    import final