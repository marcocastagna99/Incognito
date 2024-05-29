import os
import matplotlib.pyplot as plt
from datetime import datetime




#to do: passare in input i csv in base al numero di Qi indicato in modo automatico senza dover chiedere al'utente i path


#concateno i percorsi di ogni file csv richiesto
def dimTables_path(files, Qis):
    path=""
    for qi in range(Qis):
       path+="../datasets/"+files[qi]+" "
    return path



def execute(k, execution_times):
    k_str = str(k)
    for i in QIs:
        i_str = str(i)
        print("Starting with " + i_str + " QIs and k= " + k_str + " ...")
        start = datetime.timestamp(datetime.now())
        #os.system("python3 main.py -d ../datasets/adults.csv -D dimension_tables_" + i_str + "qi.json -k " + k_str)
        os.system("python3 incognito.py -d ../datasets/hospital.csv -D "+dimTables_path(dimensionsTable, i)+ "-k " + k_str)
        #print(dimTables_path(dimensionsTable, i))
        stop = datetime.timestamp(datetime.now())
        execution_time = stop - start
        print()
        print("Execution time with " + i_str + " QIs and k= " + k_str + ": " + str(execution_time))
        print()
        execution_times.append(execution_time)


def plot_with_k(k):
    execution_times = list()
    execute(k, execution_times)
    if len(execution_times) == len(QIs):
        plt.plot(QIs, execution_times)
        plt.xlabel("Number of qi")
        plt.ylabel("Time [s]")
        plt.title("Execution Time with k=" + str(k))
        plt.grid()
        plt.savefig('results.png')


if __name__ == "__main__":
    dimensionsTable = ['dateandTime.csv','gender.csv', 'race.csv', 'age.csv', 'zipCode.csv','height.csv', 'weight.csv'] #without patientId.csv
    #QIs = range(2, 8)
    QIs = range(2, 4)
    plot_with_k(2)
    #plot_with_k(10)
