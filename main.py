import sql_server_client as ssc
import requests
import pandas as pd
from sqlalchemy import text

def types_transform(df):
    type_mapping = {
        'int': lambda x: int(x.replace(',', '')),
        'datetime64': lambda x: pd.to_datetime(x, format='%Y-%m-%dT%H:%M:%S.%f', errors='coerce'),
        'str': lambda x: str(x)
    }
    # loop over columns and convert to appropriate type based on values
    for col in df.columns:
        col_values = df[col].unique()
        for t, func in type_mapping.items():
            try:
                if all(isinstance(func(val), eval(t)) for val in col_values):
                    df[col] = df[col].apply(func).astype(eval(t))
                    if col.endswith('_datetime'):
                        df[col] = pd.to_datetime(df[col])
                    break
            except:
                continue
    df = df.loc[:, ~df.columns.duplicated()]
    p_key = get_primary_key_column(df)
    df = df.drop_duplicates(subset=p_key)
    return df


def get_primary_key_column(df_columns):
    for col in df_columns:
        if col.endswith('_id'):
            p_key = col
    return p_key


def extract_api_and_write_to_tbl(sql_client):
    limit = 50000
    offset = 0
    data = []
    fact_table_name = 'Fact_Incidents'
    dim_table_name = 'Dim_Incidents'


    while True: #to limit ittereations set offset > 50001
        url = f'https://data.cityofnewyork.us/resource/8m42-w767.json?$limit={limit}&$offset={offset}'
        response = requests.get(url)

        if response.status_code == 200:
            json_data = response.json()
            if len(json_data) == 0:
                # no more data to retrieve
                break
            data += json_data
            offset += len(json_data)
        else:
            print(f'API request failed with status code {response.status_code}')
            break
        df = pd.DataFrame(data)
        data = []
        df_to_tbl = types_transform(df)

        memory_usage = df_to_tbl.memory_usage(index=True).sum()
        print(f"The dataframe uses {memory_usage / 1024:.2f} KB of memory.")
        num_rows, num_cols = df_to_tbl.shape
        print("Number of rows:", num_rows)
        print("Number of columns:", num_cols)

        fact_df, dim_df = set_data_module(df_to_tbl)

        fact_df.to_sql(fact_table_name, sql_client.engine, if_exists='append', index=False,
                         method='multi', chunksize=50)
        dim_df.to_sql(dim_table_name, sql_client.engine, if_exists='append', index=False,
                       method='multi', chunksize=50)
    return fact_df.columns, dim_df.columns

def set_data_module(df):
    # select the incident attributes
    fact_cols = ['starfire_incident_id', 'incident_datetime', 'incident_close_datetime',
    'incident_borough', 'alarm_box_borough', 'dispatch_response_seconds_qy', 'incident_response_seconds_qy',
    'incident_travel_tm_seconds_qy']
    # create the fact table
    fact_table = df[fact_cols]

    # select the incident details attributes

    dim_cols = ['starfire_incident_id', 'alarm_box_location', 'highest_alarm_level',
    'incident_classification', 'incident_classification_group',
    'zipcode', 'policeprecinct', 'citycouncildistrict',
    'communitydistrict', 'communityschooldistrict', 'congressionaldistrict',
    'alarm_source_description_tx', 'alarm_level_index_description',
    'first_assignment_datetime', 'first_activation_datetime',
    'first_on_scene_datetime',
    'valid_dispatch_rspns_time_indc', 'valid_incident_rspns_time_indc', 'engines_assigned_quantity',
    'ladders_assigned_quantity', 'other_units_assigned_quantity']

    # create the dimension table
    dim_table = df[dim_cols]
    return fact_table, dim_table


if __name__ == '__main__':
    tables = ['Fact_Incidents', 'Dim_Incidents']
    sql_client = ssc.SQLServer("DESKTOP-JJP3BVB", "NYC_DB", "DESKTOP-JJP3BVB\\Yariv", "SQL Server", 'yes')
    fact_columns, dim_columns = extract_api_and_write_to_tbl(sql_client)

    print("set primary key to tables? y/n:")
    input1 = input()
    if input1 == 'y':
        for t in tables:
            p_key = get_primary_key_column(fact_columns)
            sql_client.execute(text(f'ALTER TABLE {t} ALTER COLUMN {p_key} INT NOT NULL'))
            sql_client.execute(text(f'ALTER TABLE {t} ADD PRIMARY KEY ({p_key})'))
            sql_client.execute(text(f'create index index_col_id on {t}({p_key})'))
