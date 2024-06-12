import pandas as pd
import numpy as np
import csv


def get_columns_from_csv(file_path):
    """
    Read the first row of the CSV file to get the column names.
    """
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        first_row = next(reader)
    return first_row

def map_old_to_new_columns(old_columns, new_columns):
    """
    Map old column names to new column names based on substring matching.
    """
    column_mapping = {}
    for old_col in old_columns:
        for new_col in new_columns:
            if old_col.strip().replace("'", "") in new_col.strip().replace("'", ""):
                column_mapping[old_col] = new_col
                break
    return column_mapping


# Funzione per calcolare la Discernibility Metric (DM)
def discernibility_metric(df, qi_columns):
    equivalence_classes = df.groupby(qi_columns).size()
    dm = sum(equivalence_classes ** 2)
    return dm


# Funzione per calcolare la Normalized Certainty Penalty (NCP)
def normalized_certainty_penalty(original_df, anonymized_df, qi_columns, mapped_columns):
    ncp = 0
    for col, mapped_col in zip(qi_columns, mapped_columns):
        if original_df[col].dtype == 'object':
            unique_values = original_df[col].unique()
            ncp += sum((anonymized_df[mapped_col].value_counts() / len(anonymized_df)) ** 2) / len(unique_values)
        else:
            col_range = original_df[col].max() - original_df[col].min()
            ncp += (anonymized_df[mapped_col].max() - anonymized_df[mapped_col].min()) / col_range
    ncp /= len(qi_columns)
    return ncp

# Funzione per confrontare statistiche descrittive
def compare_statistics(original_df, anonymized_df):
    stats_original = original_df.describe(include='all')
    stats_anonymized = anonymized_df.describe(include='all')
    return stats_original, stats_anonymized

# Lettura dei dataset originali e anonimizzati da file CSV
original_df = pd.read_csv('../datasets/hospital.csv')

# Quasi-identificatori
qi_columns = ['dateandTime', 'age', 'height', 'gender', 'race', 'weight', 'zipCode']  # Modifica con i nomi delle colonne appropriate

# Calcolo delle metriche
print("Utility metrics for k=2 and k=10:")
for i in range(2):
    k = 2 if i == 0 else 10
    print("k =", k)
    file_anon_path = "../datasets/anonymous_table_k_" + str(k) + ".csv"
    new_columns = get_columns_from_csv(file_anon_path)
    column_mapping = map_old_to_new_columns(qi_columns, new_columns)

    # Ottieni solo le colonne mappate
    mapped_columns = [column_mapping[old_col] for old_col in qi_columns if old_col in column_mapping]
    mapped_columns_str = ','.join(mapped_columns)
#   print("Mapped columns:", mapped_columns_str)
    # Caricamento del dataset anonimizzato
    anonymized_df = pd.read_csv(file_anon_path)
    
    dm = discernibility_metric(anonymized_df, mapped_columns)
    ncp = normalized_certainty_penalty(original_df, anonymized_df, qi_columns, mapped_columns)
    stats_original, stats_anonymized = compare_statistics(original_df, anonymized_df)

    print("Discernibility Metric (DM):", dm)
    print("Normalized Certainty Penalty (NCP):", ncp)
   # print("\nDescriptive Statistics - Original Dataset\n", stats_original)
    print("\nDescriptive Statistics - Anonymized Dataset:\n", stats_anonymized)
