import pandas as pd
import psycopg2

# Carregar o arquivo CSV
csv_file_path = '../ds7trab.csv'
df = pd.read_csv(csv_file_path)

# Conectar ao banco de dados Postgres
conn = psycopg2.connect(
    dbname="ds7trab",
    user="postgres",
    password="123",
    host="localhost",
    port="5432"
)

# Cria tabela para persistir os dados
create_table_query = """
CREATE TABLE IF NOT EXISTS saude (
prodestatID BIGSERIAL PRIMARY KEY,
id BIGINT,
Name Varchar,
Age Bigint,
Gender Varchar,
Blood_Type Varchar,
Medical_Condition Varchar,
Date_Admission Date,
Doctor Varchar,
Hospital Varchar,
Insurance_Provider Varchar,
Billing_Amount Float,
Room_Number Bigint,
Admission_Type Varchar,
Discharge_Date Date,
Medication Varchar,
Test_Results Varchar
);
"""

cursor = conn.cursor()
cursor.execute(create_table_query)
conn.commit()

# Inserir dados do DataFrame no banco de dados
for index, row in df.iterrows():
    insert_query = """
    INSERT INTO saude (Name,Age,Gender,Blood_Type,Medical_Condition,Date_Admission,Doctor,Hospital,Insurance_Provider,Billing_Amount,Room_Number,Admission_Type,Discharge_Date,Medication,Test_Results)
    VALUES (%s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_query, tuple(row))
conn.commit()

# Fechar a conexão
cursor.close()
conn.close()