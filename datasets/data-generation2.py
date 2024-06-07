import csv
from faker import Faker
import random
import numpy as np
from datetime import datetime, timedelta
import argparse
import os




# TO DO cambiare il campo diseases e mettere direttamente la patologia e mettere come valori degli 0 e 1 (0 no, 1 sì) a caso con la stessa prob indicata
#fatto
fake = Faker()

def generate_data(num_tuples):
    data = []

    for i in range(num_tuples):
        random_number = fake.random_int(min=10000, max=99999)
        patient_id = int(f"6{random_number}")
        gender = fake.random_element(['M', 'F'])
        
        # Genera una data e un'ora casuale negli ultimi due mesi
        end_datetime = datetime.now()
        start_datetime = end_datetime - timedelta(days=60)
        data_datetime = fake.date_time_between_dates(datetime_start=start_datetime, datetime_end=end_datetime)

        #age
        prob_more_than_50 = 0.7  # Puoi regolare la probabilità secondo le tue preferenze
        if random.random() < prob_more_than_50:
            age = random.randint(31, 100)
        else:
            age = random.randint(18, 30)

        zip_code = random.randint(16121, 16167)

        race = random.choices(["White", "Black", "Asian", "Hispanic", "Native_american", "Other"], [0.7, 0.1, 0.1, 0.05, 0.03, 0.02], k=1)[0]
  
        # Altezza
        #height = fake.random_int(min=140, max=190)  # Altezza in cm
        height_mean = 170 # Media dell'altezza in cm
        height_std_dev = 20  # Deviazione standard dell'altezza in cm
        height = int(np.random.normal(height_mean, height_std_dev))
        height = max(140, min(height, 200))

        
        weight_mean = 60  # Media del peso in kg
        weight_std_dev = 25  # Deviazione standard del peso in kg
        weight = int(np.random.normal(weight_mean, weight_std_dev))
        weight = max(40, min(weight, 120))
        diseases=np.random.choice([0, 1], size=5, p=[0.7, 0.3])
        diseases=diseases.tolist()
        data.append([patient_id, data_datetime, gender, race, age, zip_code, height, weight]+ diseases)

    return data

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Add an argument for the number of tuples
    parser.add_argument('-n', '--num_tuples', type=int, required=True, help='The number of tuples')

    # Parse the arguments
    args = parser.parse_args()

# Now you can use args.num_tuples as the number of tuples
    num_tuples = args.num_tuples
    if num_tuples <= 0:
        print("Error: The number of tuples must be positive.")
        exit(1)

    generated_data = generate_data(num_tuples)

    # Salvataggio del CSV con colonne separate
    csv_filename = "../datasets/hospital.csv"
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(['patientId', 'dateandTime', 'gender', 'race', 'age', 'zipCode', 'height', 'weight', 'Hypertension', 'Arthritis', 'Asthma', 'Pneumonia', 'Diabetes'])
        csvwriter.writerows(generated_data)

    print(f"{num_tuples} tuples generated and saved to {csv_filename}.")
    
