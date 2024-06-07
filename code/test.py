import os
import matplotlib.pyplot as plt
from datetime import datetime




#todo: mettere l'avanzamento progress bar o in percentuale

#concateno i percorsi di ogni file csv richiesto
def dimTables_path(files, Qis):
    path=""
    for qi in range(Qis):
       path+="../datasets/"+files[qi]+" "
    return path



def execute(k, execution_times, treshold):
    k_str = str(k)
    t_str = str(treshold)
    for i in QIs:
        i_str = str(i)
        print("Starting with " + i_str + " QIs and k= " + k_str + " ...")
        start = datetime.timestamp(datetime.now())
        #os.system("python3 main.py -d ../datasets/adults.csv -D dimension_tables_" + i_str + "qi.json -k " + k_str)
        os.system(f"python3 incognito.py -d ../datasets/hospital.csv -D {dimTables_path(dimensionsTable, i)} -k {k_str} -t {t_str}")
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
        plt.title(f"Execution Time with k={k}")
        plt.grid()
        plt.legend() 
        plt.savefig('results.png')


if __name__ == "__main__":
    dimensionsTable = ['dateandTime.csv','gender.csv', 'race.csv', 'age.csv', 'zipCode.csv','height.csv', 'weight.csv']
    
    for i in range(2):
        if i==0:
            k=2
        else:
            k=10
        print("k =",k)
        if(k==2):
            print("how many tuples?")

            tuples = input()
            while not tuples.isdigit() or int(tuples) < int(k) or int(tuples) < 0:
                print("Invalid input. Please enter a numeric value positive <", k)
                tuples = input()

            os.system(f"python3 ../datasets/data-generation2.py --num_tuples {tuples}")
            os.system("python3 ../datasets/dimensionsTable_to_csv.py")

        print("insert the threshold t  <k (default 0)")
        treshold = input()
        while not treshold.isdigit() or int(treshold) >= int(k):
            print("Invalid input. Please enter a numeric value less than k.")
            treshold = input()
        QIs = range(2, 6)
        plot_with_k(k, int(treshold))

