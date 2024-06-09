#from pysqlite3 import dbapi2 as sqlite3
import sqlite3
from os import path
from sympy import subsets
import argparse
import queue
import pandas as pd
import csv
from statistics import mean



#fare tanto test

def prepare_table_for_k_anonymization(dataset, dataset_name):
    with open(dataset, "r") as dataset_table:

        print(f"Working on dataset {dataset_name}")

        # first line contains attribute names
        attributes_name = [attr.strip() + " TEXT" for attr in dataset_table.readline().split(",")]
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {dataset_name} ({','.join(attributes_name)})")  # create the table
        #print(f"Attributes found: {[attr.strip() for attr in attributes_name]}")

        # insert records into the SQL table
        for line in dataset_table:  # iterate over lines
            values = [value.strip().replace("-", "_") for value in line.split(",")]

            # a line could be a "\n" => values == [''] => len(values) == 1
            if len(values) > 1:
                placeholders = ', '.join('?' for _ in values)
                cursor.execute(f"INSERT INTO {dataset_name} VALUES ({placeholders})", tuple(values))  # insert the values
                connection.commit()
    return attributes_name


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
            for col_index, col_value in enumerate(row):  # per ogni colonna di tale riga col_index è l'indice della colonna, col_value è il valore
                file_dict[header[col_index]].append(col_value) #metto il valore alla rispettiva lista


        # Aggiungo il sotto-dizionario al risultato usando il nome del file come chiave principale
        result_dict[table_name] = file_dict

    return result_dict

#single dictionary with all dimension tables
def get_dimension_tables(all_csv):
    qis={}
    for csv_path in all_csv:
       qis.update(csv_to_dict(csv_path))
    return qis
     

def create_sql_dimension_tables(tables):
    for qi, table in tables.items():
        # Crea la tabella SQL
        columns = ["'{}' TEXT".format(i.replace("-", "_")) for i in table]
        cursor.execute("CREATE TABLE IF NOT EXISTS {}_dim ({})".format(qi.replace("-", "_"), ", ".join(columns)))

        # Inserisci i valori nella tabella appena creata
        for i in range(len(table["1"])):
            row = ", ".join("'{}'".format(str(table[j][i]).replace("-", "_")) for j in table)
            cursor.execute("INSERT INTO {}_dim VALUES ({})".format(qi.replace("-", "_"), row))
        connection.commit()


def table_exists(nome_tabella):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (nome_tabella,))
    return cursor.fetchone() is not None

#debug sql tables
def control(tables):
    for qi in tables:
        if table_exists(qi + "_dim"):
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


def insert_into_C1_and_E1(id, dimension, index, parent1, parent2):
    cursor.execute("INSERT INTO C1 values (?, ?, ?, ?, ?)", (id, dimension, index, parent1, parent2)) # create node
    if index >= 1:
        cursor.execute("INSERT INTO E1 values (?, ?)", (id - 1, id)) # create edges

def init_C1_and_E1():
    print("Generating graph for 1 quasi-identifier ", end="")
    id = 1
    for dimension, table in qis_dimension_tables.items(): # for each table
        for index in range(len(table)):  # for each level of generalization of that table
            parent1 = get_parent_index_C1(index, 1)
            parent2 = get_parent_index_C1(index, 2)
            insert_into_C1_and_E1(id, dimension, index, parent1, parent2)
            id += 1
    print("\t OK")


def get_height_of_node(node):
    # sum of the indexes in a row (node)
    indices = [2] + [6 + 2 * (i - 1) for i in range(1, (len(node) - 6) // 2 + 1)]#indice 2 è l'indice della prima dimensione
    height = sum([int(node[i]) for i in indices if i < len(node) and node[i] != 'null'])#sommo gli indici delle dimensioni
    return height


def get_dimensions_of_node(node):
    dimensions_temp = set()
    i = 0
    length = len(node)
    while True:
        # 1, 5, 7, 9, ...
        if i == 0:
            j = 1
        else:
            j = 5 + 2*(i-1)
        if j >= length:
            break
        if node[j] != 'null':
            dimensions_temp.add(node[j])
        i += 1
    return dimensions_temp

def get_dims_and_indexes_of_node(node):
    list_temp = list()
    i = 0
    length = len(node)
    while True:
        # dims = 1, 5, 7, ...
        # indexes = 2, 6, 8, ... = dims + 1
        if i == 0:
            j = 1
        else:
            j = 5 + 2*(i-1)
        if j >= length or j+1 >= length:
            break
        list_temp.append((node[j], node[j+1]))
        i += 1
    return list_temp

def prepare_query_parameters(attributes, dims_and_indexes_s_node, group_by_attributes, i):
    column_name = dims_and_indexes_s_node[i][0]
    generalization_level = dims_and_indexes_s_node[i][1]
    generalization_level_str = str(generalization_level)
    previous_generalization_level_str = "0"
    dimension_table = column_name + "_dim"
    dimension_with_previous_generalization_level = f"{dimension_table}.\"{previous_generalization_level_str}\""
    
    if column_name in attributes:
        group_by_attributes.discard(column_name)
        group_by_attributes.add(f"{dimension_table}.\"{generalization_level_str}\"")
    
    return column_name, dimension_table, dimension_with_previous_generalization_level, generalization_level_str


def frequency_set_of_T_wrt_attributes_of_node_using_T(node):
    attributes = get_dimensions_of_node(node)
    dims_and_indexes_s_node = get_dims_and_indexes_of_node(node)
    group_by_attributes = set(attr for attr in attributes if attr != "null")
    dimension_table_names = []
    where_items = []

    for i in range(len(dims_and_indexes_s_node)):
        if dims_and_indexes_s_node[i][0] == "null" or dims_and_indexes_s_node[i][1] == "null":
            continue

        params = prepare_query_parameters(attributes, dims_and_indexes_s_node, group_by_attributes, i)
        column_name, dimension_table, dimension_with_previous_generalization_level, generalization_level_str = params

        where_items.append(f"{dataset_name}.{column_name} = {dimension_with_previous_generalization_level}")
        dimension_table_names.append(dimension_table)

    query = (
        f"SELECT COUNT(*) FROM {dataset_name}, {', '.join(dimension_table_names)} "
        f"WHERE {' AND '.join(where_items)} GROUP BY {', '.join(group_by_attributes)}"
    )
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    return [count[0] for count in results]


def frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(node, i):
    dims_and_indexes_s_node = get_dims_and_indexes_of_node(node)
    attributes = [attr for attr in get_dimensions_of_node(node) if attr != "null"]
    
    cursor.execute(f"CREATE TEMPORARY TABLE TempTable (count INT, {', '.join(attributes)})")

    select_items = []
    where_items = []
    group_by_attributes = set(attributes)
    dimension_table_names = []

    for j in range(len(dims_and_indexes_s_node)):
        if dims_and_indexes_s_node[j][0] == "null" or dims_and_indexes_s_node[j][1] == "null":
            continue
        
        params = prepare_query_parameters(attributes, dims_and_indexes_s_node, group_by_attributes, j)
        column_name, dimension_table, dimension_with_previous_generalization_level, generalization_level_str = params

        select_items.append(f"{dimension_table}.\"{generalization_level_str}\" AS {column_name}")
        where_items.append(f"{dataset_name}.{column_name} = {dimension_with_previous_generalization_level}")
        dimension_table_names.append(dimension_table)

    insert_query = (
        f"INSERT INTO TempTable SELECT COUNT(*) AS count, {', '.join(select_items)} "
        f"FROM {dataset_name}, {', '.join(dimension_table_names)} "
        f"WHERE {' AND '.join(where_items)} GROUP BY {', '.join(group_by_attributes)}"
    )
    
    cursor.execute(insert_query)

    cursor.execute(f"SELECT SUM(count) FROM TempTable GROUP BY {', '.join(attributes)}")
    results = cursor.fetchall()
    
    cursor.execute("DROP TABLE TempTable")
    
    return [result[0] for result in results]


def table_is_k_anonymous_wrt_attributes_of_node(frequency_set):
    if len(frequency_set) == 0: #se non ci sono tuple
        return False
    for count in frequency_set: #per ogni count
        if type(count) == tuple: #se count è una tupla
            count = count[0] #prendo il primo elemento
        if k > count > threshold: #if k>count && count>threshold    se sta nel range di treshold e k allora non è k-anonimo
            return False
    return True

def mark_all_direct_generalizations_of_node(marked_nodes, node, i):
    i_str = str(i)
    marked_nodes.add(node[0])#aggiungo il nodo alla lista dei nodi marcati
    cursor.execute("SELECT E" + i_str + ".end FROM C" + i_str + ", E" + i_str + " WHERE ID = E" + i_str +
                   ".start and ID = " + str(node[0]))#tiro fuori gli eventuali nodi figlio ovvero (Ei.end, possono essere piu tuple -> piu figli)
    for node_to_mark in list(cursor):#per ogni nodo figlio
        marked_nodes.add(node_to_mark[0])#aggiungo il nodo alla lista dei nodi marcati
    #print("Marked nodes: ", marked_nodes)



def insert_direct_generalization_of_node_in_queue(node, queue, i, Si):
    i_str = str(i)
    cursor.execute("SELECT E" + i_str + ".end FROM C" + i_str + ", E" + i_str + " WHERE ID = E" + i_str +
                   ".start and ID = " + str(node[0]))  #tiro fuori gli eventuali nodi figlio ovvero (Ei.end, possono essere piu tuple -> piu figli)
    nodes_to_put = set(cursor)

    Si_indices = set()
    for node in Si:
        Si_indices.add(node[0]) #set of IDs of nodes in Si

    for node_to_put in nodes_to_put: #per ogni nodo figlio
        # node_to_put == (ID,) -.-
        if node_to_put[0] not in Si_indices: #se il nodo (Ei.end) non è in Si
            continue
        node_to_put = node_to_put[0]
        cursor.execute("SELECT * FROM C" + i_str + " WHERE ID = " + str(node_to_put))#tiro fuori il nodo figlio
        node = (list(cursor)[0])#prendo il campo ID
        queue.put_nowait((-get_height_of_node(node), node)) #aggingo alla coda il nodo con priorità -altezza

def basic_incognito_algorithm(priority_queue):
    init_C1_and_E1()
    queue = priority_queue
    marked_nodes = set()
    """
    #test
    cursor.execute("SELECT C1.* FROM C1, E1 WHERE C1.ID = E1.start EXCEPT SELECT C1.* FROM C1, E1 WHERE C1.ID = E1.end" )  #quelli a cui non appaiono i propri id nel campo Ei.end, ovvero non hanno archi entranti, ovvero sono root
    result = cursor.fetchall()
    print(result)"""
    for i in range(1, len(Q) + 1):
        i_str = str(i)
        cursor.execute("SELECT * FROM C" + i_str + "")
        results = cursor.fetchall()
        #print("Table C"+str(i)+" ", results)
        Si = set(results) #Si = set of nodes of Ci
     
        
        # no edge directed to a node => root
        cursor.execute("SELECT C" + i_str + ".* FROM C" + i_str + ", E" + i_str + " WHERE C" + i_str + ".ID = E" +
                       i_str + ".start EXCEPT SELECT C" + i_str + ".* FROM C" + i_str + ", E" + i_str + " WHERE C" +
                       i_str + ".ID = E" + i_str + ".end ") #quelli a cui non appaiono i propri id nel campo Ei.end, ovvero non hanno archi entranti, ovvero sono root
        roots = set(cursor) #set of roots
        roots_in_queue = set()#set of roots to put in queue
        #ordino by heightt
        for node in roots:
            height = get_height_of_node(node)#calcolo altezza
            # -height because priority queue shows the lowest first. Syntax: (priority number, data)
            roots_in_queue.add((-height, node))#aggiungo alla coda con priorità -altezza
        #aggiungo alla coda
        for upgraded_node in roots_in_queue:
            queue.put_nowait(upgraded_node)#aggiungo alla coda le root
      
        while not queue.empty():
            print("Queue size: ", queue.qsize())
            upgraded_node = queue.get_nowait()#prendo il nodo con priorità -altezza
            # [1] => pick 'node' in (-height, node),
            node = upgraded_node[1]

            print("Processing node: ", node)
            
            if node[0] not in marked_nodes:
                if node in roots:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_T(node)
                else:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(node, i)
                if table_is_k_anonymous_wrt_attributes_of_node(frequency_set):
                    print("anonymus")
                    mark_all_direct_generalizations_of_node(marked_nodes, node, i)
                else:
                    print("not anonymus")
                    Si.remove(node)
                    insert_direct_generalization_of_node_in_queue(node, queue, i, Si)
                    cursor.execute("DELETE FROM C" + str(i) + " WHERE ID = " + str(node[0]))
        #print("Si: ", Si)
        graph_generation(Si, i)#crea i candidati nodi e archi per la prossima iterazione dell'algoritmo (join, prune, edge generation)
        marked_nodes = set()


def graph_generation(Si, i):
    i_here = i + 1
    i_str = str(i)
    ipp_str = str(i_here)
    if i < len(Q):
        print("Generating graphs for " + ipp_str + " quasi-identifiers", end="")
    cursor.execute("PRAGMA table_info(C" + i_str + ")")
    column_infos_from_db = cursor.fetchall()

    column_infos = [
        "ID INTEGER PRIMARY KEY" if column[1] == "ID" else f"{column[1]} {column[2]}"
        for column in column_infos_from_db
    ]

    cursor.execute(f"CREATE TABLE IF NOT EXISTS S{i_str} ({', '.join(column_infos)})")
    question_marks = ", ".join(["?"] * len(column_infos_from_db))
    cursor.executemany(f"INSERT INTO S{i_str} VALUES ({question_marks})", Si)

    cursor.execute(f"SELECT * FROM S{i_str}")
    Si_new = set(cursor.fetchall())

    if i == len(Q):
        return

    cursor.execute(f"CREATE TABLE IF NOT EXISTS C{ipp_str} ({', '.join(column_infos)})")
    cursor.execute(f"ALTER TABLE C{ipp_str} ADD COLUMN dim{i_here} TEXT")
    cursor.execute(f"ALTER TABLE C{ipp_str} ADD COLUMN index{i_here} INT")
    cursor.execute(
        f"UPDATE C{ipp_str} SET dim{i_here} = 'null', index{i_here} = 'null' WHERE index{i_here} IS NULL"
    )

    select_str = ""
    select_str_except = ""
    where_str = ""

    for j in range(2, i_here):
        j_str = str(j)
        if j == i_here - 1:
            select_str += f", p.dim{j_str}, p.index{j_str}, q.dim{j_str}, q.index{j_str}"
            select_str_except += f", q.dim{j_str}, q.index{j_str}, p.dim{j_str}, p.index{j_str}"
            where_str += f" AND p.dim{j_str} < q.dim{j_str}"
        else:
            select_str += f", p.dim{j_str}, p.index{j_str}"
            select_str_except += f", q.dim{j_str}, q.index{j_str}"
            where_str += f" AND p.dim{j_str} = q.dim{j_str} AND p.index{j_str} = q.index{j_str}"
    if i > 1:
        join_query = (
            f"INSERT INTO C{ipp_str} SELECT NULL, p.dim1, p.index1, p.ID, q.ID{select_str} "
            f"FROM S{i_str} p, S{i_str} q WHERE p.dim1 = q.dim1 AND p.index1 = q.index1 {where_str}"
        )
    else:
        join_query = (
            f"INSERT INTO C{ipp_str} SELECT NULL, p.dim1, p.index1, p.ID, q.ID, q.dim1, q.index1 "
            f"FROM S{i_str} p, S{i_str} q WHERE p.dim1 < q.dim1"
        )

    cursor.execute(join_query)
    cursor.execute(f"SELECT * FROM C{ipp_str}")
    Ci_new = set(cursor.fetchall())


    all_subsets_of_Si = {tuple(get_dims_and_indexes_of_node(node)) for node in Si_new}
    for c in Ci_new:
        if not all(tuple(s) in all_subsets_of_Si for s in subsets(get_dims_and_indexes_of_node(c), i)):
            node_id = str(c[0])
            cursor.execute(f"DELETE FROM C{ipp_str} WHERE ID = {node_id}")

    cursor.execute(f"CREATE TABLE IF NOT EXISTS E{ipp_str} (start INT, end INT)")
  
    cursor.execute(
        f"INSERT INTO E{ipp_str} WITH CandidatesEdges(start, end) AS ("
        f"SELECT p.ID, q.ID FROM C{ipp_str} p, C{ipp_str} q, E{i_str} e, E{i_str} f "
        f"WHERE (e.start = p.parent1 AND e.end = q.parent1 AND f.start = p.parent2 AND f.end = q.parent2) "
        f"OR (e.start = p.parent1 AND e.end = q.parent1 AND p.parent2 = q.parent2) "
        f"OR (e.start = p.parent2 AND e.end = q.parent2 AND p.parent1 = q.parent1)) "
        f"SELECT D.start, D.end FROM CandidatesEdges D "
        f"EXCEPT SELECT D1.start, D2.end FROM CandidatesEdges D1, CandidatesEdges D2 WHERE D1.end = D2.start"
    )

    cursor.execute(f"SELECT * FROM E{ipp_str}")

    results = cursor.fetchall()
    print("\t OK")



def projection_of_attributes_of_Sn_onto_T_and_dimension_tables(Sn):
    if not Sn:
        print("Unable to anonymize the data: Sn is empty.")
        return
    # Trova il nodo con l'altezza minima
    lowest_node = min(Sn, key=lambda t: get_height_of_node(t))
    height = get_height_of_node(lowest_node)

    # Calcolo del nodo con l'altezza minima
    for node in Sn:
        temp_height = get_height_of_node(node)
        if temp_height < height:
            height = temp_height
            lowest_node = node

    print("Chosen anonymization levels: ", end="")
    #print("Sn", Sn)
    #print("Q", Q)
    #print("attributes", attributes)

 # get QI names and their indexes (i.e. their generalization level)
    qis = list()
    qi_indexes = list()
    for i in range(len(lowest_node)):
        if lowest_node[i] in Q:
            qis.append(lowest_node[i])
            qi_indexes.append(lowest_node[i+1])
            print(str(lowest_node[i]) + "(" + str(lowest_node[i+1]) + ") ", end="")
    print("")

    # get all table attributes with generalized QI's in place of the original ones
    gen_attr = attributes_name
    considered_gen_qis =list()
    for i in range(len(gen_attr)):
        gen_attr[i] = gen_attr[i].split()[0]
        if gen_attr[i] in qis:
            gen_attr[i] = qis[qis.index(gen_attr[i])] + "_dim.'" + str(qi_indexes[qis.index(gen_attr[i])]) + "'"
            considered_gen_qis.append(gen_attr[i])

    # Debug: print generalized attributes and considered QIs
    #print("Generalized attributes:", gen_attr)
    #print("Considered Generalized QIs:", considered_gen_qis)

    # Ottieni i nomi delle tabelle dimensioni
    dim_tables = [f"{qi}_dim" for qi in qis]

    # Debug: stampa tabelle dimensioni
    #print("Dimension tables:", dim_tables)

    # Ottieni le coppie per il JOIN SQL
    pairs = [f"{x}={y}.'0'" for x, y in zip(qis, dim_tables)]

    # Debug: stampa coppie per il JOIN
    #print("Pairs for JOIN:", pairs)

    # Costruisci le clausole della query
    select_clause = "SELECT " + ', '.join(gen_attr)
    from_clause = " FROM " + dataset_name + ", " + ', '.join(dim_tables)
    where_clause = " WHERE " + ' AND '.join(pairs)
    subquery_clause = (
        " AND (" + ', '.join(considered_gen_qis) + ") IN"
        " (SELECT " + ', '.join(considered_gen_qis) +
        " FROM " + dataset_name + ", " + ', '.join(dim_tables) +
        " WHERE " + ' AND '.join(pairs) +
        " GROUP BY " + ', '.join(considered_gen_qis) +
        " HAVING COUNT(*) > " + str(threshold) + ")"
    )

    # Costruzione della query completa
    query = select_clause + from_clause + where_clause + subquery_clause

    # Stampa la query per il debug
   # print("Query:", query)

    # Esecuzione della query
    cursor.execute(query)



    print("Writing k-anonymous table to anonymous_table.csv", end="")
    with open("../datasets/anonymous_table_k_"+str(k)+".csv", "w") as anonymous_table:
        anonymous_table.writelines(','.join(attributes_name) + "\n")
        for row in cursor.fetchall():
            anonymous_table.writelines(','.join(str(x) for x in row) + "\n")
        anonymous_table.close()
    print("\t OK")


                
    

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
    
    dataset_path = args.dataset #dataset path
    dataset_name = path.basename(dataset_path).split(".")[0] #nome tabella
    attributes_name=prepare_table_for_k_anonymization(dataset_path, dataset_name) #creo tabella
    
    """
    #test tabella principale

    cursor.execute("SELECT * FROM " + dataset_name)
    result = cursor.fetchall()
    # Stampa il risultato
    print(result)
    #Sprint(attributes)
    #dataset = path.basename(dataset).split(".")[0]
    """
    
    # get dimension tables
    qis_dimension_tables= get_dimension_tables(args.dimension_tables) #dict con tutti i qi dimensions
    Q = set(qis_dimension_tables.keys())  #gli attributi Quasi-identificatori
    #print(qis_dimension_tables)


    # create dimension SQL tables
    create_sql_dimension_tables(qis_dimension_tables)  #tabelle sql delle qi dimensions
    #control(Q)
    
    #test creazione tabelle dimensioni
    """ cursor.execute('''SELECT hospital.dateandTime, dateandTime_dim."0" FROM hospital, dateandTime_dim''')
    results= cursor.fetchall()
    # Stampa il risultato
    for riga in results:
        print(riga)"""
    # test pragma main table
    
    """ cursor.execute("PRAGMA table_info(dateandTime_dim)")
    result = cursor.fetchall()
    print(result)"""

    #control if k is valid
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


    cursor.execute("SELECT * FROM S" + str(len(Q)))
    Sn = list(cursor)



    projection_of_attributes_of_Sn_onto_T_and_dimension_tables(Sn)

    print("DONE")

    connection.close()
