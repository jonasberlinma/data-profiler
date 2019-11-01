# data-profiler

Use Scala to create a data profiler. Some of the stuff it does:

* Calculate the number of blanks (nulls)
* Identify the most likely data type
* Identify date and timestamp columns
* Calculate min, max, and average value for continuous types
* Calculate frequency distribution for categorical types
* Calculate the frequency of patterns (numeric, alpha, special characters, etc.) for categorical types
* Try to identify FK->PK relationships between datasets

