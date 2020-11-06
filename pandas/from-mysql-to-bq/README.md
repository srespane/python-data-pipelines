# Solution

## Solution
- **from_mysql_to_bq_pandas.py** 
  - Executable python file This file contains the code for executing the extraction of a query from MySQL to BigQuery using the local file system as temporary storage in Parquet + Snappy format
  - The script will infer the schema from data directly and create a new table with data. If table name exists it will overwite its structure and content.
  - In order to execute this file we need to use python and pass as argument the path to the configuration file.
- **config.yaml.template**
  - Template configuration file
  - This file contains all the information needed to configure the python executable file. It’s a YAML file and from it we can control the whole process.
- **example.sql**
  - This file contains the query to be executed in the source MySQL database in multiline plain text
- **requirements.txt**
  - Python requirements file with all needed dependencies


## 1-Before executing 

It is necessary to set the variable **GOOGLE_APPLICATION_CREDENTIALS** with the path to the credentials json file.

```bash
export  GOOGLE_APPLICATION_CREDENTIALS="/path/to/json/credentials.json"
```

## 2-Generate Config

Copy the config.yaml.template file and complete with own values

```yaml
query-file: /path/to/query/file/example.sql
source-db:
    host: 1.2.3.4
    port: 3306
    db: somedb
    user: someuser
    pass: somepass
target-table:
    project: some-bq-project
    dataset: some-bq-dataset
    table: some-bq-table
temp-storage: /path/to/target/file.parquet
```

## 3-Query file
Copy example.sql file and generate your own with the desired query

```sql
select *
from table_a a
inner join table_b b on a.id = b.id
limit 1000
```

## 4-Install python requirements using pip on your new or existent python environment
```bash
pip install -r requirements.txt
```

## Execute data pipeline
```bash
python from_mysql_to_bq_pandas.p /path/to/the/config/file.yaml
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)