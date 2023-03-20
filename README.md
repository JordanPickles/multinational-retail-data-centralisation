# multinational-retail-data-centralisation

This was a scenario based project set by Ai Core forming part of the data science career accelerator. This scenario aimed to build skills in data extraction and cleaning from multiple sources in python before uploading the data to a postgres database. The database schema was then designed using the star schema and the data was then queried using PostgreSQL to provide data driven insights for the scenario outlined below.

Scenario: You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location. Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business.

## Mileston 1 - Data Extraction and Cleaning.

- Data was extracted from multiple sources (RDS Tables, PDF's, API's, AWS S3 Buckets.

The following code was written in order to extract and clean the data

- RDS Tables
- Connecting to the tables
```
    def read_db_creds(self):
        with open("db_creds.yaml", "r") as f:
            db_creds = yaml.safe_load(f)
        return db_creds

    def init_db_engine(self, db_creds):
        engine = create_engine(f"{db_creds['RDS_DATABASE_TYPE']}+{db_creds['DB_API']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        engine.connect()
        
        # Take in the db_creds output and initialise and return an sql_alchemy database engine
        return engine

    def list_db_tables(self, engine):
        engine.connect()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
```

- Extracting the data from a table
```
    def read_rds_table(self, table_names, table_name, engine):
        engine = engine.connect()
        data = pd.read_sql_table(table_name, engine)
        return data
```

- Cleaning the data
```
    def clean_user_data(self, legacy_users_table):
        legacy_users_table.replace('NULL', pd.NaT, inplace=True)
        legacy_users_table.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)
        legacy_users_table['date_of_birth'] = pd.to_datetime(legacy_users_table['date_of_birth'], errors = 'ignore')
        legacy_users_table['join_date'] = pd.to_datetime(legacy_users_table['join_date'], errors ='coerce')
        legacy_users_table = legacy_users_table.dropna(subset=['join_date'])
        legacy_users_table['phone_number'] = legacy_users_table['phone_number'].str.replace('/W', '')
        legacy_users_table = legacy_users_table.drop_duplicates(subset=['email_address'])
        legacy_users_table.drop(legacy_users_table.columns[0], axis=1, inplace=True)
        
        return legacy_users_table 
```

- Extracting data from a PDF document

```
    def retrieve_pdf_data(self, link):
        pdf_path = link
        df = tb.read_pdf(pdf_path, pages="all")
        df = pd.concat(df)
        df = df.reset_index(drop=True)
        return df
```

- Cleaning the PDF data
```
    def clean_card_data(self, card_data_table):
        card_data_table.replace('NULL', pd.NaT, inplace=True)
        card_data_table.dropna(subset=['card_number'], how='any', axis=0, inplace=True)
        card_data_table = card_data_table[~card_data_table['card_number'].str.contains('[a-zA-Z?]', na=False)]
        card_data_table.to_csv('outputs.csv')
        return card_data_table
```

- Extracting data from an API
```
    def list_number_of_stores(self, endpoint, api_key):
        response = requests.get(endpoint, headers=api_key)
        content = response.text
        result = json.loads(content)
        number_stores = result['number_stores']
        
        return number_stores

    def retrieve_stores_data(self, number_stores, endpoint, api_key):
        data = []
        for store in range(0, number_stores):
            response = requests.get(f'{endpoint}{store}', headers=api_key)
            content = response.text
            result = json.loads(content)
            data.append(result)

        df = pd.DataFrame(data)
        print(df.head(10))
        return df
```
- Cleaning the data from the API
```
 def clean_store_data(self, store_data):
        store_data = store_data.reset_index(drop=True)
        store_data.replace('NULL', pd.NaT, inplace=True)
        store_data.loc[[31, 179, 248, 341, 375], 'staff_number'] = [78, 30, 80, 97, 39] # individually replaces values that have been inccorectly including text
        store_data.dropna(subset=['address'], how='any', axis=0, inplace=True)
        store_data = store_data[~store_data['staff_number'].str.contains('[a-zA-Z?]', na=False)]
        store_data = store_data.drop('lat', axis = 1)
        store_data['continent'] = store_data['continent'].str.replace('eeEurope', 'Europe').str.replace('eeAmerica', 'America')
        
        return store_data

```

- Extracting multiple data tables from an AWS S3 Bucket in CSV and .json format

```
    def extract_from_s3(self, s3_address):
        s3 = boto3.resource('s3')
        if 's3://' in s3_address:
            s3_address = s3_address.replace('s3://','' )
        elif 'https' in s3_address:
            s3_address = s3_address.replace('https://', '')

        bucket_name, file_key = s3_address.split('/', 1)
        bucket_name = 'data-handling-public'
        obj = s3.Object(bucket_name, file_key)
        body = obj.get()['Body']
        if 'csv' in file_key:
            df = pd.read_csv(body)
        elif '.json' in file_key:
            df = pd.read_json(body)
        df = df.reset_index(drop=True)
        return df


```
- Several methods were created to clean the relevant data extracted from the AWS tables.

```
    def convert_product_data(self, x):
        if 'kg' in x:
            x = x.replace('kg', '')
            x = float(x)

        elif 'ml' in x:
            x = x.replace('ml', '')
            x = float(x)/1000

        elif 'g' in x:
            x = x.replace('g', '')
            x = float(x)/1000

        elif 'lb' in x:
            x = x.replace('lb', '')
            x = float(x)*0.453591
            
        return x

    def clean_product_data(self, data): 
        data.replace('NULL', pd.NaT, inplace=True)
        data['date_added'] = pd.to_datetime(data['date_added'], errors ='coerce')
        data.dropna(subset=['date_added'], how='any', axis=0, inplace=True)
        data['weight'] = data['weight'].apply(lambda x: x.replace(' .', ''))

        temp_cols = data.loc[data.weight.str.contains('x'), 'weight'].str.split('x', expand=True) # splits the weight column intop 2 temp columns split by the 'x'
        numeric_cols = temp_cols.apply(lambda x: pd.to_numeric(x.str.extract('(\d+\.?\d*)', expand=False)), axis=1) # Extracts the numeric values from the temp columns just created
        final_weight = numeric_cols.prod(axis=1) # Gets the product of the 2 numeric values
        data.loc[data.weight.str.contains('x'), 'weight'] = final_weight

        data['weight'] = data['weight'].apply(lambda x: str(x).lower().strip())
        data['weight'] = data['weight'].apply(lambda x: self.convert_product_data(x))
        data.drop(data.columns[1], axis=1, inplace=True) 
        return data
        
    def clean_date_data(self, data):
        data = data[~data['year'].str.contains('[a-zA-Z?]', na=False)]
        data.dropna(subset=['year'], how='any', axis=0, inplace=True)
        return data

```
