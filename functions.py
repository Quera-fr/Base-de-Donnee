import sqlalchemy as db
import pandas as pd
from sqlalchemy import text

class DataBaseV1():
    # SQLalchemy1.4.4
    def __init__(self,
                 db_user,
                 db_password,
                 db_host,
                 db_name: str = 'database'
                 ):
        self.name = db_name
        self.url = f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}'
        self.engine = db.create_engine(self.url)
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()
        self.table_names = self.engine.table_names()

    def create_table(self, name_table: str, **kwargs):
        if name_table in self.table_names:
            print(f"La table '{name_table}' existe déjà.")
            return False
        columns = [db.Column(k, v, primary_key=True) if 'id_' in k else db.Column(k, v) for k, v in kwargs.items()]
        table = db.Table(name_table, self.metadata, *columns)
        table.create(self.engine)
        self.table_names.append(name_table)
        print(f"Table '{name_table}' créée avec succès.")

    def delete_table(self, name_table: str):
        if name_table in self.table_names:
            table = self.read_table(name_table)
            table.drop(self.engine)
            self.table_names.remove(name_table)
            print(f"Table '{name_table}' supprimée avec succès.")
        else:
            print(f"La table '{name_table}' n'existe pas.")

    def read_table(self, name_table: str):
        return db.Table(name_table, self.metadata, autoload=True, autoload_with=self.engine)
    
    def dataframe(self, name_table: str):
        return pd.DataFrame(self.select_table(name_table))

    def add_row(self, name_table: str, **kwargs):
        table = self.read_table(name_table)
        stmt = db.insert(table).values(**kwargs)
        self.connection.execute(stmt)
        print("Ligne ajoutée avec succès.")

    def delete_row_by_id(self, name_table: str, id_: int):
        table = self.read_table(name_table)
        stmt = db.delete(table).where(table.c.id_ == id_)
        result = self.connection.execute(stmt)
        if result.rowcount > 0:
            print(f"Ligne avec ID {id_} supprimée avec succès.")
        else:
            print(f"Aucune ligne avec ID {id_} trouvée.")

    def select_table(self, name_table: str):
        table = self.read_table(name_table)
        stm = db.select([table])
        result = self.connection.execute(stm).fetchall()
        return result

    def update_row_by_id(self, name_table: str, id_: int, **kwargs):
        table = self.read_table(name_table)
        stmt = db.update(table).where(table.c.id_ == id_).values(**kwargs)
        result = self.connection.execute(stmt)
        if result.rowcount > 0:
            print(f"Ligne avec ID {id_} mise à jour avec succès.")
        else:
            print(f"Aucune ligne avec ID {id_} trouvée.")

    def show_all_tables(self):
        if not self.table_names:
            print("Aucune table dans la base de données.")
        else:
            print("Tables présentes dans la base de données:")
            for table_name in self.table_names:
                print(table_name)

# SQLalchemy2.0.1
class DataBaseV2():
    def __init__(self, 
                 db_name:str='database',
                 db_type:str='sqlite',
                 db_user:str=None,
                 db_password:str=None,
                 db_host:str=None,
                 db_port:int=None,
                 db_url:str=None):
        """
        Connect to a database with sqlalchemy==2.0.1

        db_type : str : 'sqlite', 'mysql', 'postgresql'
        db_name : str : name of the database
        db_user : str : user name
        db_password : str : user password
        db_host : str : host name
        db_port : int : port number
        db_url : str : url of the database

        Choose db_type and db_name
        Choose db_url or db_user, db_password, db_host, db_port
        """
        
        if db_type == 'sqlite':self.url = f"{db_type}:///{db_name}.db" if db_url is None else db_url
        if db_type == 'mysql': self.url = f'{db_type}+pymysql://{db_user}:{db_password}@{db_host}:{str(db_port)}/{db_name}' if db_url is None else db_url
        if db_type == 'postgresql': self.url = f'{db_type}+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}' if db_url is None else db_url

        self.db_name = db_name
        self.engine = db.create_engine(self.url)
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()
        self.tables = self.engine.dialect.get_table_names(self.connection)

        self.data_type = {
                'int64' : db.Numeric(10, 0),
                'float64' : db.Float,
                'object' : db.String(255),
                'datetime64' : db.DateTime(),
                'bool' : db.Boolean(),
                'category' : db.String()    
            }


    def show_tables_infos(self) -> dict:
        """
        Show all tables in the database
        
        Return : dict : {table_name : table}
        """
        self.tables = self.engine.dialect.get_table_names(self.connection)
        return {table:self.read_table(table) for table in self.tables}


    def create_table(self, table_name:str, **kwargs:dict):
        """
        Create a table in the database
        
        table_name : str : name of the table
        **kwargs : dict : {column_name : column_type}
        
        Return : None
        """
        try:
            colums = [db.Column(k, v, primary_key = True) if 'id_' in k else db.Column(k, v) for k,v in kwargs.items()]
            db.Table(table_name, self.metadata, *colums)
            self.metadata.create_all(self.engine)
            print(f"Table : '{table_name}' are created succesfully")
            self.tables = self.engine.dialect.get_table_names(self.connection)
        except:
            print(f"Error: Table : '{table_name}' are not created, check the name or the columns or the table already exist")


    def read_table(self, table_name:str, return_keys:bool=False) -> db.Table or list:
        """
        Read a table in the database

        table_name : str : name of the table

        Return : Table : table
        """
        try:
            table = db.Table(table_name, self.metadata, autoload_with=self.engine)
            if return_keys:return table.columns.keys() 
            else : return table
        except:
            print(f"Error: Table : '{table_name}' not found")


    def dataframe(self, table_name: str) -> pd.DataFrame:
        """
        Return the table in a dataframe
        
        table_name : str : name of the table
        
        Return : DataFrame : table
        """
        try : return pd.DataFrame(self.select_table(table_name))
        except : print(f"Error: Table : '{table_name}' not found")


    def get_info_columns_from_table(self, table_name:str, return_keys:bool=False) -> list:
        """
        Get all columns from a table
        
        table_name : str : name of the table
        
        Return : list : columns
        """
        try : return self.engine.dialect.get_columns(self.connection, table_name)
        except : return 'No table named : ' + str(table_name)


    def insert_row(self, table_name:str, verbose:bool=True, **kwarrgs:dict) -> None:
        """
        Insert a row in the table
        
        table_name : str : name of the table
        **kwarrgs : dict : {column_name : value}
        
        Return : None
        """
        try :       
            with self.engine.connect() as conn:
                table = self.read_table(table_name)
                stmt = (db.insert(table).values(kwarrgs))
                conn.execute(stmt)
                conn.commit()
            if verbose :print(f'Row added')
        except:
            print(f'Error: Row not added')

    def select_table(self, table_name:str) -> list or None:
        """
        Return all rows in the table

        table_name : str : name of the table

        Return : list : rows
        """
        try:
            with self.engine.connect() as conn:
                table = self.read_table(table_name)
                stm = db.select(table)
                return conn.execute(stm).fetchall()
        except:
            print(f'Error: Table : {table_name} not found')
    

    def delete_row_by_id(self, table_name:str, row_index:int) -> None:
        """
        Delete a row in the table

        table : str : name of the table
        row_index : int : index of the row

        Return : None
        """
        
        try:
            table = self.read_table(table_name)
            primary_key = [column for column in table.columns.keys() if 'id_' in column][0]
            stmt = db.delete(table).where(table.c[primary_key] == row_index)
            with self.engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
                print(f'Row with id {row_index} deleted')
        except:
            print(f'Error: Column with id {row_index} not found')
            print("Verify the table name, the id_ and if the table have a column with id_ in the name")


    def delete_table(self, table_name:str) -> None:
        """
        Delete a table in the database
        
        table_name : str : name of the table
        
        Return : None
        """

        table = self.read_table(table_name)
        
        if table is not None:
            with self.engine.connect() as conn:
                table.drop(self.engine)
                conn.commit()
                #table.drop(self.engine)
                print(f'Table {table_name} deleted successfully')
        else: print(f'Error: Table {table_name} not found')
        #self.metadata.remove(self.read_table(table_name))
        self.tables = self.engine.dialect.get_table_names(self.connection)

 
    def query_to_dataframe(self, query:str) -> pd.DataFrame:
        """
        Execute a query and return the result in a dataframe

        query : str : query

        Return : DataFrame : result
        """

        try:return pd.read_sql(sql=text(query), con=self.engine.connect())
        except:print(f'Error: Query not executed')
        
    def send_query(self, query:str) -> None:
        """
        Execute a query

        query : str : query

        Return : None
        """

        try:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                self.tables
            print(f'Query executed')
        except:
            print(f'Error: Query not executed')


    def insert_csv_to_table(self, table_name:str, path:str, sep:str=',') -> None:
        """
        Create a table from a csv file

        table_name : str : name of the table
        path : str : path of the csv file
        sep : str : separator

        Return : None
        """
        df = pd.read_csv(path, sep=sep)
        self.insert_dataframe_to_table(table_name, df)


    def insert_dataframe_to_table(self, table_name:str, df:pd.DataFrame) -> None:
        """
        Insert a dataframe in a table

        table_name : str : name of the table
        df : DataFrame : dataframe

        Return : None
        """

        columns = df.columns
        if  table_name not in self.tables:
            definition_columns = {column:self.data_type[str(df[column].dtype)] for column in columns}
            print(definition_columns)

            self.create_table(table_name, **definition_columns)
            for n_ligne in range(len(df)):
                data = dict(df.iloc[n_ligne])
                print(data)
                self.insert_row(table_name, verbose=False, **data)
            print(f'Table {table_name} created and data inserted')
        else:
            columns_table = self.get_info_columns_from_table(table_name, return_keys=True)
            for n_ligne in range(len(df)):
                data = dict(df[columns_table].iloc[n_ligne])
                self.insert_row(table_name, verbose=False, **data)
            print(f'Data inserted in table {table_name}')
            


    def update_row_by_id(self, table_name:str, id_:(int, str), **kwargs) -> None:
        """
        Update a row in the table

        table_name : str : name of the table
        id_ : int, str : id of the row
        **kwargs : dict : {column_name : value}

        Return : None
        """
        table = self.read_table(table_name)
        primary_key = [column for column in table.columns.keys() if 'id_' in column][0]
        print(primary_key)
        id_column = getattr(table.c, primary_key, None)
        print(id_column)
        try:
            stmt = db.update(table).where(id_column == id_).values(**kwargs)
            with self.engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
                print(f'Row with id {id_} updated')
        except:
            print(f'Error: Column with id {id_} not found')