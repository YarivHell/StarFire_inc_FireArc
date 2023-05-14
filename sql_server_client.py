from sqlalchemy import create_engine

class SQLServer:
    def __init__(self, server, database, username, driver, trusted_connection):
        self.server = server
        self.database = database
        self.username = username
        self.driver = driver
        self.trusted_connection = trusted_connection
        self.engine = create_engine(
            f'mssql+pyodbc://{username}@{server}/{database}?driver={driver}&Trusted_Connection={trusted_connection}', use_setinputsizes=False)

    def execute(self, query):
        with self.engine.connect() as conn:
            try:
                res = conn.execute(query)
            except Exception as e:
                print(f'An error occurred: {str(e)}')
            else:
                print(f'query executed successfully. Rowcount: {res.rowcount}')

    def fetch_all(self, query):
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return result.fetchall()

    def fetch_one(self, query):
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return result.fetchone()

    def close_connection(self):
        self.engine.dispose()
