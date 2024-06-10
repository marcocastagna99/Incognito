# Incognito Project
## Overview
The Incognito project is designed to perform k-anonymity on a dataset, using a custom algorithm implemented in Python. The process involves generating a dataset, pass dimension tables, and then applying the Incognito algorithm to anonymize the data. The algorithm requires several parameters from the user and performs statistical analysis before and after anonymization.

# Project Structure
The project directory is organized as follows:

Per includere il CSS nel README, è importante notare che GitHub e molti altri sistemi di rendering Markdown non supportano direttamente l'inclusione di CSS. Tuttavia, puoi stilizzare il README usando Markdown e HTML in modo che abbia una buona presentazione senza la necessità di CSS.

Se vuoi aggiungere CSS per visualizzazioni locali o su piattaforme che supportano CSS in Markdown (come alcuni sistemi di documentazione o blog), puoi includere CSS direttamente nel file Markdown usando tag HTML <style>.

Ecco un esempio di come fare:

markdown
Copia codice
# Incognito Project

<style>
  body {
    font-family: Arial, sans-serif;
    line-height: 1.6;
  }
  h1, h2, h3 {
    color: #333;
  }
  code {
    background-color: #f9f9f9;
    border: 1px solid #ddd;
    padding: 2px 4px;
    border-radius: 4px;
  }
  pre {
    background-color: #f9f9f9;
    border: 1px solid #ddd;
    padding: 10px;
    border-radius: 4px;
  }
</style>

## Overview
The Incognito project is designed to perform k-anonymity on a dataset, using a custom algorithm implemented in Python. The process involves generating a dataset, creating dimension tables, and then applying the Incognito algorithm to anonymize the data. The algorithm requires several parameters from the user and performs statistical analysis before and after anonymization.

## Project Structure
The project directory is organized as follows:

incognito/
├── code/
│ ├── incognito.py
│ ├── test.py
│ ├── data-generation2.py
│ ├── dimensionsTable_to_csv.py
│ ├── analysis.py
│ └── ... (other code files)
└── datasets/
└── stats/
└── ... (CSV files)
## Requirements
Ensure you have the following Python packages installed:

-pandas
-matplotlib
-sympy
-sqlite3
