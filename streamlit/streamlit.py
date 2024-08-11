import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, String, Integer, Float, Numeric, Date, Boolean, DateTime, Text
from sqlalchemy.exc import SQLAlchemyError

def main():
    st.title('CSV Uploader with Column Configuration')

    # Page 1: Database Connection Setup
    st.header('Setup Database Connection')

    # Collect database connection details
    db_type = st.selectbox('Database Type', ['mysql+pymysql', 'mysql+mysqlconnector'])
    host = st.text_input('Host')
    username = st.text_input('Username')
    password = st.text_input('Password', type='password')
    database_name = st.text_input('Database Name')
    table_name = st.text_input('Table Name')

    uploaded_file = st.file_uploader("Upload CSV", type="csv")
    
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("CSV Columns:")
        st.write(df.head())

        # Page 2: Column Type Setup
        st.header('Setup Column Data Types')

        # Initialize session state for column configurations
        if 'columns_config' not in st.session_state:
            st.session_state.columns_config = pd.DataFrame({
                'Column Name': df.columns,
                'Data Type': ['VARCHAR'] * len(df.columns),
                'Size': [255] * len(df.columns),
                'Scale': [0] * len(df.columns)
            })

        # Display and edit column configurations
        columns_df = st.session_state.columns_config

        with st.form(key='column_setup_form'):
            st.write("Edit Column Configurations:")
            for i, column in columns_df.iterrows():
                cols = st.columns(4)
                with cols[0]:
                    col_name = st.text_input(f'Column Name {i+1}', value=column['Column Name'], key=f'col_name_{i}')
                with cols[1]:
                    dtype = st.selectbox(
                        f'Data Type {i+1}', 
                        ['VARCHAR', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'INT', 'BIGINT', 'FLOAT', 'DECIMAL', 'DATE', 'DATETIME', 'BOOLEAN'], 
                        index=['VARCHAR', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'INT', 'BIGINT', 'FLOAT', 'DECIMAL', 'DATE', 'DATETIME', 'BOOLEAN'].index(column['Data Type']), 
                        key=f'dtype_{i}'
                    )
                with cols[2]:
                    size = st.number_input(
                        f'Size {i+1}', 
                        min_value=0, 
                        value=column['Size'] if dtype in ['VARCHAR'] else 0, 
                        key=f'size_{i}'
                    )
                with cols[3]:
                    scale = st.number_input(
                        f'Scale {i+1}', 
                        min_value=0, 
                        value=column['Scale'] if dtype == 'DECIMAL' else 0, 
                        key=f'scale_{i}'
                    )
                
                # Update the session state DataFrame
                st.session_state.columns_config.at[i, 'Column Name'] = col_name
                st.session_state.columns_config.at[i, 'Data Type'] = dtype
                st.session_state.columns_config.at[i, 'Size'] = size
                st.session_state.columns_config.at[i, 'Scale'] = scale

            submit_button = st.form_submit_button(label='Update Configurations')

            if submit_button:
                st.success('Configurations updated successfully.')

        # Display updated column configurations
        st.write("Updated Column Configurations:")
        st.dataframe(st.session_state.columns_config)

        if st.button('Save to Database'):
            try:
                db_uri = f'{db_type}://{username}:{password}@{host}/{database_name}'
                engine = create_engine(db_uri)

                # Define a mapping of column names to SQLAlchemy types
                sqlalchemy_types = {
                    'VARCHAR': lambda size: String(length=size),
                    'TEXT': Text,
                    'MEDIUMTEXT': Text,
                    'LONGTEXT': Text,
                    'INT': Integer,
                    'BIGINT': Integer,
                    'FLOAT': Float,
                    'DECIMAL': lambda precision, scale: Numeric(precision=precision, scale=scale),
                    'DATE': Date,
                    'DATETIME': DateTime,
                    'BOOLEAN': Boolean
                }

                dtype_dict = {}
                for _, row in st.session_state.columns_config.iterrows():
                    col_name = row['Column Name']
                    dtype = row['Data Type']
                    size = row['Size']
                    scale = row['Scale']

                    # Ensure size and scale are integers and handle empty values
                    try:
                        size = int(size) if pd.notna(size) else 255
                        scale = int(scale) if pd.notna(scale) else 0
                    except ValueError:
                        size = 255
                        scale = 0

                    if dtype == 'VARCHAR':
                        dtype_dict[col_name] = sqlalchemy_types['VARCHAR'](size)
                    elif dtype == 'DECIMAL':
                        dtype_dict[col_name] = sqlalchemy_types['DECIMAL'](size, scale)
                    elif dtype in ['TEXT', 'MEDIUMTEXT', 'LONGTEXT']:
                        dtype_dict[col_name] = sqlalchemy_types['TEXT']()
                    elif dtype == 'FLOAT':
                        dtype_dict[col_name] = sqlalchemy_types['FLOAT']()
                    elif dtype == 'BIGINT':
                        dtype_dict[col_name] = sqlalchemy_types['BIGINT']()
                    else:
                        dtype_dict[col_name] = sqlalchemy_types.get(dtype, String)  # Default to String if unknown

                # Create table and insert data
                df.to_sql(table_name.strip(), con=engine, if_exists='replace', index=False, dtype=dtype_dict)
                st.success(f"Data loaded into table '{table_name}' successfully.")
            except SQLAlchemyError as e:
                st.error(f"Database Error: {e}")
            except Exception as e:
                st.error(f"Error: {e}")

if __name__ == "__main__":
    main()
