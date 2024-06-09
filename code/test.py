import os
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import csv


def remove_csv_extension_from_array(array):
    return [file.replace('.csv', '') if file.endswith('.csv') else file for file in array]



#concateno i percorsi di ogni file csv richiesto
def dimTables_path(files, Qis):
    path=""
    for qi in range(Qis):
       path+="../datasets/"+files[qi]+" "
    return path

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
                column_mapping[old_col] = old_col
                break
    return column_mapping



def execute(k, execution_times, treshold):
    k_str = str(k)
    t_str = str(treshold)
    for i in QIs:
        i_str = str(i)
        print("Starting with " + i_str + " QIs and k= " + k_str + " ...")
        start = datetime.timestamp(datetime.now())
        #os.system("python3 main.py -d ../datasets/adults.csv -D dimension_tables_" + i_str + "qi.json -k " + k_str)
        os.system(f"python3 incognito.py -d {file_path} -D {dimTables_path(dimensionsTable, i)} -k {k_str} -t {t_str}")
        #print(dimTables_path(dimensionsTable, i))
        stop = datetime.timestamp(datetime.now())
        execution_time = stop - start
        print()
        print("Execution time with " + i_str + " QIs and k= " + k_str + ": " + str(round(execution_time,3)))
        print()
        execution_times.append(execution_time)


def plot_with_k(k, treshold):
    execution_times = list()
    execute(k, execution_times, treshold)
    if len(execution_times) == len(QIs):
        plt.plot(QIs, execution_times, label=f'k={k}')
        plt.xlabel("Number of qi")
        plt.ylabel("Time [s]")
        plt.title(f"Execution Time")
        plt.grid()
        plt.legend() 
        plt.savefig('results.png')


if __name__ == "__main__":
    dimensionsTable = ['gender.csv', 'race.csv', 'age.csv','dateandTime.csv', 'zipCode.csv','height.csv', 'weight.csv']
    quis = remove_csv_extension_from_array(dimensionsTable)
    file_path="../datasets/hospital.csv" #path of the original table
    
    for i in range(2):
        if i==0:
            k=2
        else:
            k=10
        print("k =",k)
        file_anon_path="../datasets/anonymous_table_k_"+str(k)+".csv" #path of the anonymized table
        
        if(k==2):
            print("how many tuples do you want to generate (example 1000)?")
            tuples = input()
            while not tuples.isdigit() or int(tuples) <= int(k) :
                print("Invalid input. Please enter a numeric value positive >", k)
                tuples = input()

            os.system(f"python3 ../datasets/data-generation2.py --num_tuples {tuples}")#generate the data
            os.system("python3 ../datasets/dimensionsTable_to_csv.py")#create the csv files (this case only for date_time.csv)
            print("QIs: ",quis)
            print("how many qis do you want to use? (maximum 7)")

            qisNum = input()
            while not qisNum.isdigit() or int(qisNum) < 2:
                print("Invalid input. Please enter a numeric value positive => 2")   
                qisNum = input()


            columns = input("Enter the columns to analyze, separated by commas: ").replace(" ", "")
            os.system(f"python3 analysis.py {file_path} {columns}") #analysis of the original table

    

        print("insert the threshold t  <k (default 0)")#treshold for the suppression of the k-anonymity
        treshold = input()
        while not treshold.isdigit() or int(treshold) >= int(k):
            print("Invalid input. Please enter a numeric value less than k.")
            treshold = input()
        QIs = range(2, int(qisNum)+1)#range of QIs
        plot_with_k(k, int(treshold))
  
        # Get old and new column names, because the anon table columns are different from the original table
        old_columns = columns.split(',')
        new_columns = get_columns_from_csv(file_anon_path)
        column_mapping = map_old_to_new_columns(old_columns, new_columns)

        # Ottieni solo le colonne mappate
        mapped_columns = [column_mapping[old_col] for old_col in old_columns if old_col in column_mapping]
        mapped_columns_str = ','.join(mapped_columns)
        #print("Mapped columns:", mapped_columns_str)
        
        #analysis of the anonymized table
        os.system(f"python3 analysis.py {file_anon_path} {mapped_columns_str}")

