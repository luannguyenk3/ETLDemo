import pandas as pd 
import yaml 
import pyodbc
from sqlalchemy import create_engine 


excel_file_path = r"D:\HK5\BI\etl_python-master\SaleDataETLDemo.xlsx"

df_ORDER = pd.read_excel(excel_file_path, sheet_name="ORDER")
df_CUSTOMER = pd.read_excel(excel_file_path, sheet_name="CUSTOMER")
df_PRODUCT = pd.read_excel(excel_file_path, sheet_name="PRODUCT")

# Merge ORDER with PRODUCT to create order_product table

df_order_product = pd.merge(df_ORDER, df_PRODUCT, on='ProductID', how='outer')

# Merge order_product with CUSTOMER

df_OPC = pd.merge(df_order_product, df_CUSTOMER, on='CustomerID', how='outer')

# Perform revenue and profit calculation steps

df_OPC['Sales']=df_OPC['Units'] * df_OPC['Price']
df_OPC['COGS']=df_OPC['Units'] * df_OPC['Cost']
df_OPC['Profit'] = df_OPC['Sales'] - df_OPC['COGS']

# remove duplicate rowsD

df_OPC = df_OPC.drop_duplicates()

# Read information from a YAML file
with open('dulieusql.yml', 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)
    print(config)

server = config['database']['server']
database = config['database']['database']
username = config['database']['username']
password = config['database']['password']

# Create a connection string for pyodbc.
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Database={database};UID={username};PWD={password}'
conx = pyodbc.connect(conn_str)

# Create a connection URL for SQLAlchemy
db_url = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url)

# Replace 'table_name' with the name of the table you want to save the data into
table_name = 'ETL_Demo_test_2710'

# Save a DataFrame into a database.

df_OPC.to_sql(table_name, con=engine, if_exists='replace', index=False)
