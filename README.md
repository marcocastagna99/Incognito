
# Incognito Project

## Overview
The Incognito project is designed to perform k-anonymity on a dataset, using a custom algorithm implemented in Python. The process involves generating a dataset, pass dimension tables, and then applying the Incognito algorithm to anonymize the data. The algorithm requires several parameters from the user and performs statistical analysis before and after anonymization.

## Project Structure
The project directory is organized as follows:

```
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
```

## Requirements
Ensure you have the following Python packages installed:
- pandas
- matplotlib
- sympy
- sqlite3

You can install these packages using pip:
```bash
pip install pandas matplotlib sympy sqlite3
```

## Running the Program
To run the program, navigate to the `code` directory and execute `test.py`:
```bash
cd incognito/code
python3 test.py
```

## User Input
When you run `test.py`, the program will prompt you for the following inputs:
1. **Number of tuples**: The number of tuples to generate for the dataset.
2. **Number of QIs**: The number of quasi-identifiers (QIs) to use in the test (up to 7, optimal value found to be 5).
3. **QIs to analyze**: Select which QIs you want to analyze.


After providing the required inputs, the program will automatically generate the data and create the initial dataset `../datasets/hospital.csv`. It will also call `dimensionsTable_to_csv.py` to generate the necessary dimension tables.

## Algorithm Requirements
For the algorithm to function correctly, it needs:
- **Path to the main table in CSV format**: For example, `../datasets/hospital.csv`.
- **Paths to the CSV files for dimension tables**: These files should contain all levels of generalization for the QIs and the order of.

### Dimension Table Format
The CSV files for dimension tables must be formatted as follows:
- The filename should correspond to the field in the main table.
- The first column must correspond to the levels of generalization (0 being the lowest, with no generalization, and higher numbers indicating higher levels of generalization).

#### Example: AGE CSV
```
0,1,2
18,20,20
19,20,20
20,20,20
21,20,20
22,20,20
23,20,20
24,20,20
25,25,20
...
```

#### Example: GENDER CSV
```
0,1
F,Person
M,Person
```

## Running the Analysis
When you run the program, it will also launch `analysis.py` to perform statistical analysis before and after the execution of `incognito.py`. This will help you understand the statistical changes to the dataset due to the anonymization process.

### Final User Input
Lastly, the program will ask for:
- **t_value**: The desired value for the outlier threshold. Rows with a certain combination of quasi-identifiers will be deleted if they are present in an amount lower than `t_value`. To disable this feature, set `t_value` to 0.
- The value for `k` will be set automatically by the program, first with `k=2` and then with `k=10`.


## Execution Example
Launch the program with the following command:
```bash
python3 test.py
```
Follow the prompts to provide the necessary parameters, and the program will handle the rest.

---

By following this guide, you should be able to set up and run the Incognito project, ensuring proper anonymization of your dataset while maintaining its statistical integrity.

---
