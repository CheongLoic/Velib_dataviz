

print("HELLO WORLD !")

import os
import pandas as pd

# Chemin absolu du fichier en cours d'exécution
current_file_path = os.getcwd()
print("Chemin actuel :", current_file_path)

file_path = "C:/Users/LOL/Desktop/Velib_dataviz/README.md" #os.path.join(current_file_path, 'test.py')

# Vérifier si le fichier existe
if os.path.exists(file_path):
    print(f"File exists: {file_path}")
else:
    raise FileNotFoundError(f"File not found: {file_path}")

# files = os.listdir('.')
# print(files)
from datetime import datetime

# Obtenir la date et l'heure actuelles
current_datetime = datetime.now()
# Formater la date pour qu'elle soit compatible avec un nom de fichier
formatted_date = current_datetime.strftime('%Y-%m-%d_%H-%M-%S')
# Construire le nom du fichier CSV
filename = f"velib_data_{formatted_date}.csv"

print("Test filename , ",filename)  # Exemple : data_2024-12-08_14-30-45.csv


df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })

