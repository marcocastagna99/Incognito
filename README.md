# Incognito Project
## Overview
The Incognito project is designed to perform k-anonymity on a dataset, using a custom algorithm implemented in Python. The process involves generating a dataset, creating dimension tables, and then applying the Incognito algorithm to anonymize the data. The algorithm requires several parameters from the user and performs statistical analysis before and after anonymization.

# Project Structure
The project directory is organized as follows:
incognito/
    ├── code/
    │   ├── incognito.py
    │   ├── test.py
    │   ├── data-generation2.py
    │   ├── dimensionsTable_to_csv.py
    │   ├── analysis.py
    │   └── ... (other code files)
    └── datasets/
        └── stats/
            └── ... (CSV files)
## Requirements
Ensure you have the following Python packages installed:

-pandas
-matplotlib
-sympy
-sqlite3
