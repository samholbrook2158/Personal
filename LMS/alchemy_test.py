import sqlalchemy as sa
import pandas as pd  # pd read sql to_sql insert read write from pandas only
import json

with open('config.json') as config_file:
    config = json.load(config_file)

user = config['user']
password = config['password']
host = config['host']
port = config['port']
database = config['database']

e = sa.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

# csv_file_path = 'test.csv'  # Replace with your CSV file path
# df = pd.read_csv(csv_file_path)

new_data = [
    {
        "name": "jay do",
        "email": "jaydo@atlbest.com"
    },

    {
        "name": "jay do2",
        "email": "jaydo2@atlbest.com"
    },
    {
        "name": "jay do 3",
        "email": "jaydo3@atlbest.com"
    },


]
df = pd.DataFrame(new_data)
df.to_sql('test', con=e, if_exists='append', index=False)

print(df.head())

e.dispose()

print("Data from CSV inserted successfully.")

