import pandas as pd
import csv





data = pd.read_csv("hospital.csv", delimiter=',', skiprows=0, low_memory=False)


def generalize_numeric_field_digits(data, field_name):
    maxlength = len(str(data[field_name][0]))  # Lunghezza delle cifre nel campo numerico
    currentlength = maxlength
    generalizations = []
    for k in range(0, maxlength+1):  #k è il numero di cifre che mantengo "numeriche"
        gen_values = []
        for i in range(0, len(data)):
            if k == maxlength:  
                val = str(data[field_name][i])
                gen_values.append(val)
            else:
                val = str(data[field_name][i])[:-currentlength]    #rimuovo current lenght caratteri della stringa, slicing della stringa
                for c in range(0, currentlength):
                    val = val + '*'
                gen_values.append(val)
        generalizations.append(gen_values)
        currentlength = currentlength - 1
    return generalizations






""" def array_to_dict(array_of_arrays):
    array_of_arrays=array_of_arrays[::-1]
    result_dict = {}
    key=0
    for array in array_of_arrays:
        result_dict[key] = array
        key+=1
    return result_dict
 """



patientIdGen=generalize_numeric_field_digits(data,"patientId")
patientIdGen=patientIdGen[::-1]
""" zipGen=generalize_numeric_field_digits(data,"ZIPCode")
zipGen=zipGen[::-1] """
#date time to do
#genderGeneralization = [['Person'],['F', 'M']]
#ageGeneralization = [['*'],["18-39","40-60","61-80","81-100","101-120"],["18-30","31-40","41-50","51-60","61-70","71-80","81-90","91-100"],["18-23","24-30","31-35","36-40","41-45","46-50","51-60","61-65","66-70", "71-75","76-80","81-85","86-90","91-100","101-106","106-110"]]
#zipGen=generalize_numeric_field_digits(data,"ZIPCode")
#heigthGen=zipGen=generalize_numeric_field_digits(data,"Height")
#weightGeneration= [['*'],["40-60","61-80","81-100","100-120"],["40-50","51-60","61-70","71-80","81-90","91-100","101-110","111-120"],["36-40","41-45","46-50","51-60","61-65","66-70","71-75","76-80","81-85","86-90","91-100","101-106","106-110", "111-116", "116-120"]]


#qiDimensionTable=[patientIdGen,genderGeneralization,ageGeneralization,zipGen,heigthGen,weightGeneration]



percorso_file_csv = 'patientId.csv'

# Apri il file CSV in modalità scrittura
with open(percorso_file_csv, 'w', newline='') as file_csv:
    # Crea un oggetto scrittore CSV
    scrittore = csv.writer(file_csv)
    scrittore.writerow(['0', '1', '2', '3', '4', '5', '6'])


    # Itera attraverso gli elementi e scrivi su colonne
    for colonna in zip(*patientIdGen):
        scrittore.writerow(colonna)


""" percorso_file_csv = 'zip.csv'

# Apri il file CSV in modalità scrittura
with open(percorso_file_csv, 'w', newline='') as file_csv:
    # Crea un oggetto scrittore CSV
    scrittore = csv.writer(file_csv)
    scrittore.writerow(['0', '1', '2', '3', '4', '5'])


    # Itera attraverso gli elementi e scrivi su colonne
    for colonna in zip(*zipGen):
        scrittore.writerow(colonna)
 """



# Supponiamo che data sia un DataFrame di pandas con una colonna 'DateandTime'
# Sostituisci questo con il tuo DataFrame effettivo
#data = {'DateandTime': ['2024-02-20 12:30', '2024-02-20 13:45', '2024-02-20 15:00']}

# Percorso del file CSV
percorso_file_csv = 'dateandTime.csv'

# Apri il file CSV in modalità scrittura
with open(percorso_file_csv, 'w', newline='') as file_csv:
    # Crea un oggetto scrittore CSV
    scrittore = csv.writer(file_csv)

    # Scrivi le intestazioni delle colonne
    scrittore.writerow(['0', '1'])

    # Scrivi i dati nelle colonne
    for valore_data in data['dateandTime']:
        scrittore.writerow([valore_data, '*'])

#to do:  fare una funzione che unisce tutti le tabelle in unico dict
#to do valutare i ****
"""def get_dimensions_table(array,qis):
    result={}
    for index in range(0, qis):
         current_dict = array_to_dict(array[index])
         result = {**result, **current_dict}
    return result

print(get_dimensions_table(qiDimensionTable,3))"""