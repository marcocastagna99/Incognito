#from pysqlite3 import dbapi2 as sqlite3
import sqlite3
from os import path
from sympy import subsets
import argparse
import queue
import pandas as pd
import csv
from statistics import mean

#tutto cio che non è commentato funziona
#devo continuare dalla parte commentata    progress: 48%
#per ora va il parsing e crea le tabelle sql relative alle dimensioni, inizializza C1/E1 (nodi e archi di un solo qi)
#continuare incognito

def prepareTableForKAnonymization(dataset):
    with open(dataset, "r") as dataset_table:
        table_name = path.basename(dataset).split(".")[0]
        print("Working on dataset " + table_name)

        # first line contains attribute names
        attribute_names = dataset_table.readline().split(",")
        for attr in attribute_names:
            table_attribute = attr.strip() + " TEXT"  #per dare il tipo di dato al campo sql 
            #attributes.append(table_attribute.replace("-", "_"))
            attributes.append(table_attribute)
        cursor.execute("CREATE TABLE IF NOT EXISTS " + table_name + "(" + ','.join(attributes) + ")")  #creo la tabella
        print("Attributes found: " + str([attr.strip() for attr in attribute_names]))
        # insert records into the SQL table
        for line in dataset_table: #itero le linee
            values = line.split(",")
            new_values = list()
            for value in values: #itero i valori di una linea
                value = value.strip()
                if value.__contains__("-"):
                    value = value.replace("-", "_")
                new_values.append(value)

            # a line could be a "\n" => new_values ===== [''] => len(new_values) == 1
            if len(new_values) == 1:
                continue #ignoro "\n"
            cursor.execute("INSERT INTO " + table_name + ' values ({})'.format(new_values)  #inserisco i valori new values, elimino []
                           .replace("[", "").replace("]", ""))
            connection.commit()

#IMPORTANT each columns of the csv file rappresent different level of generalization
#the first line of the file is made with values that rappresent leves of the tree
def csv_to_dict(file_path):
    result_dict = {}
    with open(file_path, 'r', newline='', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        table_name = path.basename(file_path).split(".")[0]
        header = next(csv_reader)  # livelli di generalizzazione
        file_dict = {key: [] for key in header} #la prima riga contiene le chiavi (0,1 etc)
        for row in csv_reader:  #itero ogni riga
            for col_index, col_value in enumerate(row):  #per ogni colonna di tale riga
                file_dict[header[col_index]].append(col_value) #metto il valore rispettiva lista


        # Aggiungi il sotto-dizionario al risultato usando il nome del file come chiave principale
        result_dict[table_name] = file_dict

    return result_dict

#single dictionary with all dimension tables
def get_dimension_tables(all_csv):
    qis={}
    for csv_path in all_csv:
       qis.update(csv_to_dict(csv_path))
    return qis
     

def create_sql_dimension_tables(tables):
    for qi in tables:
        # create SQL table
        columns = list()
        for i in tables[qi]:
            columns.append("'" + i + "' TEXT")#first line, append TEXT for each value
        cursor.execute("CREATE TABLE IF NOT EXISTS " + qi + "_dim (" + ", ".join(columns) + ")")

        # insert values into the newly created table
        rows = list()
        for i in range(len(tables[qi]["1"])):
            row = "("
            for j in tables[qi]:
                row += "'" + str(tables[qi][j][i]) + "', "
            row = row[:-2] + ")"
            rows.append(row)
        cursor.execute("INSERT INTO " + qi + "_dim VALUES " + ", ".join(rows))
        connection.commit()


def tabella_esiste(nome_tabella):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (nome_tabella,))
    return cursor.fetchone() is not None


def control(tables):
    for qi in tables:
        if tabella_esiste(qi + "_dim"):
            print("esiste")
            cursor.execute("SELECT * FROM " + qi + "_dim")
            result = cursor.fetchall()
            print(result)
        else:
            print(f"La tabella {qi}_dim non esiste.")


def create_tables_Ci_Ei():
    cursor.execute("CREATE TABLE IF NOT EXISTS C1 (ID INTEGER PRIMARY KEY, dim1 TEXT,"
                   " index1 INT, parent1 INT, parent2 INT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS E1 (start INT, end INT)")


def get_parent_index_C1(index, parent1_or_parent2):
    parent_index = index - parent1_or_parent2
    if parent_index < 0:
        parent_index = "null"
    return parent_index


def init_C1_and_E1():
    print("Generating graph for 1 quasi-identifier ", end="")
    id = 1
    for dimension in qis_dimension_tables: #per ogni tabella
        index = 0
        for i in range(len(qis_dimension_tables[dimension])):  #per ogni livello di generalizzazione di tale tabella
            # parenty = index - y
            # parent2 is the parent of parent1
            parent1 = get_parent_index_C1(index, 1)
            parent2 = get_parent_index_C1(index, 2)
            tuple = (id, dimension, index, parent1, parent2)
            cursor.execute("INSERT INTO C1 values (?, ?, ?, ?, ?)", tuple) #creo nodo
            if index >= 1:
                cursor.execute("INSERT INTO E1 values (?, ?)", (id - 1, id)) #creo edges
            id += 1
            index += 1
    print("\t OK")


def basic_incognito_algorithm(priority_queue):
    init_C1_and_E1()
    queue = priority_queue
    """cursor.execute("SELECT C1.* FROM C1, E1 WHERE C1.ID = E1.start EXCEPT SELECT C1.* FROM C1, E1 WHERE C1.ID = E1.end" )  #quelli a cui non appaiono i propri id nel campo Ei.end, ovvero non hanno archi entranti, ovvero sono root
    result = cursor.fetchall()
    print(result) """
    
    """for i in range(1, len(Q) + 1):
        i_str = str(i)
        cursor.execute("SELECT * FROM C" + i_str + "")
        Si = set(cursor) 

        # no edge directed to a node => root
        cursor.execute("SELECT C" + i_str + ".* FROM C" + i_str + ", E" + i_str + " WHERE C" + i_str + ".ID = E" +
                       i_str + ".start EXCEPT SELECT C" + i_str + ".* FROM C" + i_str + ", E" + i_str + " WHERE C" +
                       i_str + ".ID = E" + i_str + ".end ")
        roots = set(cursor)
        roots_in_queue = set()

        for node in roots:
            height = get_height_of_node(node)
            # -height because priority queue shows the lowest first. Syntax: (priority number, data)
            roots_in_queue.add((-height, node))

        for upgraded_node in roots_in_queue:
            queue.put_nowait(upgraded_node)

        while not queue.empty():
            upgraded_node = queue.get_nowait()
            # [1] => pick 'node' in (-height, node),
            node = upgraded_node[1]
            if node[0] not in marked_nodes:
                if node in roots:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_T(node)
                else:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(node, i)
                if table_is_k_anonymous_wrt_attributes_of_node(frequency_set):
                    mark_all_direct_generalizations_of_node(marked_nodes, node, i)
                else:
                    Si.remove(node)
                    insert_direct_generalization_of_node_in_queue(node, queue, i, Si)
                    cursor.execute("DELETE FROM C" + str(i) + " WHERE ID = " + str(node[0]))

        graph_generation(Si, i)
        marked_nodes = set()"""

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Provide the file path for the dataset you want to anonymize,"
                                    " the path for the dimension tables file (in csv format),"
                                    " the desired k value and a threshold value to ignore outliers")
    parser.add_argument("--dataset", "-d", required=True, type=str, help="Dataset path")
    parser.add_argument("--dimension-tables", "-D", required=True, type=str, nargs='+', help="Dimension tables paths")     
    parser.add_argument("--k", "-k", required=True, type=int, help="k value")
    parser.add_argument("--threshold", "-t", required=False, type=int, help="Threshold value")
    args = parser.parse_args()

    #creo un database SQLite in memoria
    connection = sqlite3.connect(":memory:") #connessione ad un database volatile
    cursor = connection.cursor()  # cursor utilizzato per le operazioni nel db
    cursor.execute("PRAGMA synchronous = OFF") #ottimizzazzione, però se il sistema dovesse saltare perderei i dati
    cursor.execute("PRAGMA journal_mode = OFF")#non uso journaling, +prestazioni, -durata di vita delle transazioni
    cursor.execute("PRAGMA locking_mode = EXCLUSIVE") #blocco esclusivo

    # all attributes of the table
    attributes = list()

    dataset_path = args.dataset #dataset path
    prepareTableForKAnonymization(dataset_path) #creo tabella
    dataset_name = path.basename(dataset_path).split(".")[0] #nome tabella
    
    #test tabella principale
    """table_name = path.basename(dataset).split(".")[0]
    cursor.execute("SELECT * FROM " + table_name)
    result = cursor.fetchall()
    # Stampa il risultato
    print(result)
    #Sprint(attributes)
    #dataset = path.basename(dataset).split(".")[0]"""

    # get dimension tables
    qis_dimension_tables= get_dimension_tables(args.dimension_tables) #dict con tutti i qi dimensions
    Q = set(qis_dimension_tables.keys())  #gli attributi Quasi-identificatori

    # create dimension SQL tables
    create_sql_dimension_tables(qis_dimension_tables)  #tabelle sql delle qi dimensions
    """ cursor.execute("SELECT * FROM age_dim" )
    result = cursor.fetchall()
    # Stampa il risultato
    print(result)"""


    k = args.k
    cursor.execute("SELECT * FROM " + str(dataset_name))
    if k > len(list(cursor)) or k <= 0:
        print("ERROR: k value is invalid")
        exit(0)

    try:
        threshold = args.threshold
        if threshold >= k or threshold < 0:
            print("ERROR: threshold value is invalid")
            exit(0)
    except:
        threshold = 0
        pass


    # the first domain generalization hierarchies are the simple A0->A1, O0->O1->O2 and, obviously, the first candidate
    # nodes Ci (i=1) are the "0" ones, that is Ci={A0, O0}
    create_tables_Ci_Ei()

    # we must pass the priorityQueue otherwise the body of the function can't see and instantiates a PriorityQueue
    basic_incognito_algorithm(queue.PriorityQueue())

    connection.close()

    """

    cursor.execute("SELECT * FROM S" + str(len(Q)))
    Sn = list(cursor)

    projection_of_attributes_of_Sn_onto_T_and_dimension_tables(Sn)

    print("DONE")

    connection.close()"""
