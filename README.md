# Assignment 1 

## Source to Bronze Layer:
1. Utilized common functions defined in the `utils` notebook.
2. Read CSV files with custom schemas (`read_csv_with_custom_schema` function).
3. Converted CamelCase column names to snake_case (`convert_camel_to_snake_case` function).
4. Wrote DataFrames to DBFS location in CSV format.

## Bronze to Silver Layer:
1. Created database "Employee_info" if it doesn't exist.
2. Read CSV files with custom schemas into DataFrames.
3. Converted CamelCase column names to snake_case.
4. Added a load_date column with the current date.
5. Wrote DataFrames as Delta tables to the DBFS location.

## Silver to Gold Layer:
1. Utilized Delta tables from the Silver layer.
2. Found the average salary of each department in descending order.
3. Listed department names along with their corresponding country names.
4. Found the number of employees in each department located in each country.
5. Calculated the average age of employees in each department.
6. Added the at_load_date column to DataFrames.
7. Wrote DataFrames to DBFS location in Delta format with overwrite and replace condition on at_load_date.



