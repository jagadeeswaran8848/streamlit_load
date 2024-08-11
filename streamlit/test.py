import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer, Float, Numeric, Date, Boolean, DateTime, Text
from sqlalchemy.exc import SQLAlchemyError
import configparser
import os
from urllib.parse import quote_plus

def main():
    # Configuration file setup
    config_file = 'config.ini'
    config = configparser.ConfigParser()
    load_config(config, config_file)
    encode_password = quote_plus(config.get('Database', 'password', fallback=''))
    
    db_type_value = config.get('Database', 'db_type', fallback='mysql+pymysql')
    host_value = config.get('Database', 'host', fallback='')
    username_value = config.get('Database', 'username', fallback='')
    password_value = encode_password
    database_name_value = config.get('Database', 'database_name', fallback='')
    
    st.title('CSV Uploader')

    db_type = st.selectbox('Database Type', ["mysql+pymysql", "mysql+mysqlconnector"], index=["mysql+pymysql", "mysql+mysqlconnector"].index(db_type_value))
    host = st.text_input('Host', host_value)
    username = st.text_input('Username', username_value)
    password = st.text_input('Password', password_value, type='password')
    database_name = st.text_input('Database Name', database_name_value)
    table_name = st.text_input('Table Name')

    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        st.write("Data Preview:", df.head())

        st.write("Define column data types and update options:")
        
        dtype_dict = {}
        update_commands = []
        phone_column_set = False

        columns = df.columns.tolist()
        dtype_choices = [
            "VARCHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT", 
            "INT", "BIGINT", 
            "FLOAT", "DECIMAL", 
            "DATE", "DATETIME", "BOOLEAN"
        ]
        
        update_options = ["None", "Update Phone", "Update CRN"]

        for col_name in columns:
            st.subheader(f"Column: {col_name}")

            dtype = st.selectbox(f"Data Type for {col_name}", dtype_choices, index=dtype_choices.index("VARCHAR"))
            size = st.text_input(f"Size for {col_name}", "255" if dtype == "VARCHAR" else "")
            precision_scale = st.text_input(f"Precision, Scale for {col_name}", "10,2" if dtype == "DECIMAL" else "")
            max_size = st.text_input(f"Max Size for {col_name}", "")
            update_option = st.selectbox(f"Update Option for {col_name}", update_options, index=update_options.index("None"))

            if dtype == 'VARCHAR':
                max_size = int(size) if size else 255
                dtype_dict[col_name] = String(max_size)
            elif dtype in ['INT', 'BIGINT']:
                dtype_dict[col_name] = Integer()
            elif dtype == 'FLOAT':
                dtype_dict[col_name] = Float()
            elif dtype == 'DECIMAL':
                size, scale = map(int, precision_scale.split(',')) if precision_scale else (10, 2)
                dtype_dict[col_name] = Numeric(precision=size, scale=scale)
            elif dtype == 'DATE':
                dtype_dict[col_name] = Date()
            elif dtype == 'DATETIME':
                dtype_dict[col_name] = DateTime()
            elif dtype == 'BOOLEAN':
                dtype_dict[col_name] = Boolean()
            elif dtype == 'TEXT':
                dtype_dict[col_name] = Text()

            if update_option != 'None':
                if update_option == "Update Phone":
                    phone_column_set = True
                    update_commands.append(
                        f"UPDATE `{table_name}` "
                        f"SET `{col_name}` = CONCAT('0', TRIM(`{col_name}`)) "
                        f"WHERE TRIM(`{col_name}`) NOT LIKE '0%'"
                    )
                elif update_option == "Update CRN":
                    update_commands.append(
                        f"UPDATE `{table_name}` "
                        f"SET `{col_name}` = LPAD(`{col_name}`, 8, '0') "
                        f"WHERE `{col_name}` IS NOT NULL"
                    )
        
        if phone_column_set:
            dtype_dict = {col: String(20) if 'phone' in col.lower() else dtype for col, dtype in dtype_dict.items()}
        
        db_uri = f'{db_type}://{username}:{password}@{host}/{database_name}'
        engine = create_engine(db_uri)

        try:
            df.to_sql(table_name, con=engine, if_exists='replace', index=False, dtype=dtype_dict)
            st.success(f"Data loaded into table '{table_name}' successfully.")
            
            with engine.connect() as connection:
                with connection.begin():  # Start a transaction
                    for command in update_commands:
                        try:
                            result = connection.execute(text(command))
                            st.write(f"Rows affected: {result.rowcount}")
                        except Exception as e:
                            st.error(f"Error executing command: {e}")
                    connection.commit()  # Commit the transaction

                query = f"SELECT * FROM `{table_name}` LIMIT 10"
                df_verification = pd.read_sql(query, con=engine)
                st.write(f"Data after updates:\n{df_verification.head()}")

            save_form_data(config_file, db_type, host, username, password, database_name)
        
        except SQLAlchemyError as e:
            st.error(f"Database Error: {e}")
        except Exception as e:
            st.error(f"Error: {e}")

def load_config(config, config_file):
    if os.path.exists(config_file):
        config.read(config_file)

def save_form_data(config_file, db_type, host, username, password, database_name):
    config = configparser.ConfigParser()
    if not os.path.exists(config_file):
        open(config_file, 'w').close()  # Create the file if it doesn't exist

    config.read(config_file)
    if not config.has_section('Database'):
        config.add_section('Database')
    
    config.set('Database', 'db_type', db_type)
    config.set('Database', 'host', host)
    config.set('Database', 'username', username)
    config.set('Database', 'password', password)
    config.set('Database', 'database_name', database_name)
    
    with open(config_file, 'w') as configfile:
        config.write(configfile)

if __name__ == "__main__":
    main()
