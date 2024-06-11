import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os

def load_data(file_path):
    """
    Load the dataset from a CSV file.
    """
    return pd.read_csv(file_path)

def clean_data(df):
    """
    Clean the data: convert numeric fields and handle missing values.
    """
    # Rimuovi '_dim' dai nomi delle colonne
    df.columns = [strip_dim(col) for col in df.columns]
    
    # Handle missing values (here, simply filling with the mean for numeric columns)
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].fillna(df[col].mean())
    
    return df

def check_columns_exist(df, columns, file_path):
    """
    Check if the specified columns exist in the DataFrame.
    """
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"The following columns are not in the dataset {file_path}: {', '.join(missing_columns)}")

def descriptive_statistics(df, numeric_columns, file=None):
    """
    Compute descriptive statistics for the specified numeric columns.
    """
    stats = df[numeric_columns].describe()
    if file:
        file.write("Descriptive statistics:\n")
        file.write(str(stats) + "\n\n")

def text_field_statistics(df, text_columns, file, file_name):
    """
    Compute frequency counts for the specified text columns and visualize them.
    """
    for col in text_columns:
        counts = df[col].value_counts()
        stats_text = f"Value counts for {col}:\n{counts}\n"
        file.write(stats_text + "\n")
        
        # Visualize the frequency counts
        plt.figure(figsize=(10, 6))
        sns.countplot(y=col, data=df, order=counts.index)
        plt.title(f'Value Counts for {col}')
        plt.xlabel('Frequency')
        plt.ylabel(col)
        plt.savefig(f"../datasets/stats/png/{file_name}_{col}.png")
        plt.close()  # Close the plot to free memory

def visualize_data(df, numeric_columns, file_name):
    """
    Create plots to visualize the data.
    """
    for col in numeric_columns:
        plt.figure(figsize=(10, 6))
        sns.histplot(df[col], kde=True)
        plt.title(f'Distribution of {col}')
        plt.xlabel(col)
        plt.ylabel('Frequency')
        plt.savefig(f"../datasets/stats/png/{file_name}_{col}.png")
        plt.close()  # Close the plot to free memory

def strip_dim(string):
    """
    Strip the string removing everything after '_dim', including '_dim'.
    """
    dim_index = string.find('_dim')
    if dim_index != -1:
        string = string[:dim_index]
    return string

def main():
    # Get the parameters passed from main.py
    file_path = sys.argv[1]
    file_name = os.path.basename(file_path)
    name, ext = os.path.splitext(file_name)
    file_name = name.replace('.', '_')

    columns = sys.argv[2].split(',')

    # Load and clean the data
    df = load_data(file_path)
    df = clean_data(df)

    # Check if the specified columns exist in the DataFrame
    try:
        check_columns_exist(df, columns, file_path)
    except ValueError as e:
        print(e)
        sys.exit(1)

    # Separate numeric and text columns
    numeric_columns = [col for col in columns if pd.api.types.is_numeric_dtype(df[col])]
    text_columns = [col for col in columns if not pd.api.types.is_numeric_dtype(df[col])]

    # Open the output file once for all functions
    output_file_path = f"../datasets/stats/{file_name}_stats.txt"
    with open(output_file_path, 'w') as output_file:
        # Perform statistical analysis and visualization
        if numeric_columns:
            descriptive_statistics(df, numeric_columns, output_file)
            visualize_data(df, numeric_columns, file_name)
        
        if text_columns:
            text_field_statistics(df, text_columns, output_file, file_name)
    print("analysis saved successfully! in: ", output_file_path)
if __name__ == "__main__":
    main()
