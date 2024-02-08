import sqlalchemy as db
import pandas as pd

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


class DataBaseV2():
    # SQLalchemy2.0.1
    def __init__(self, 
                 db_name='database',
                 db_type='sqlite',
                 db_user=None,
                 db_password=None,
                 db_host=None,
                 db_port=None,
                 db_url=None):
        
        if db_type == 'sqlite':self.url = f"{db_type}:///{db_name}.db" if db_url is None else db_url
        if db_type == 'mysql': self.url = f'{db_type}+pymysql://{db_user}:{db_password}@{db_host}:{str(db_port)}/{db_name}' if db_url is None else db_url
        if db_type == 'postgresql': self.url = f'{db_type}+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}' if db_url is None else db_url

        self.db_name = db_name
        self.engine = db.create_engine(self.url)
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()
        try:self.table = self.engine.dialect.get_table_names(self.connection)
        except: pass

    def show_tables(self):
        for table in self.table:
            try:self.read_table(table)
            except:pass
        return self.metadata.tables



    def create_table(self, name_table, **kwargs):
        colums = [db.Column(k, v, primary_key = True) if 'id_' in k else db.Column(k, v) for k,v in kwargs.items()]
        db.Table(name_table, self.metadata, *colums)
        self.metadata.create_all(self.engine)
        print(f"Table : '{name_table}' are created succesfully")
        self.table = list(self.metadata.tables.keys())

    def read_table(self, name_table, return_keys=False):
        table = db.Table(name_table, self.metadata, autoload_with=self.engine)
        if return_keys:table.columns.keys() 
        else : return table

    def dataframe(self, name_table: str):
        return pd.DataFrame(self.select_table(name_table))

    def get_columns_table(self, table_name):
        try : return self.engine.dialect.get_columns(self.connection, table_name)
        except : return 'No table named : ' + str(table_name)


    def add_row(self, name_table, **kwarrgs):
        try :       
            with self.engine.connect() as conn:
                name_table = self.read_table(name_table)
                stmt = (db.insert(name_table).values(kwarrgs))
                conn.execute(stmt)
                conn.commit()
            print(f'Row id added')
        except:
            print(f'Error: Row id not added')

    def select_table(self, name_table):
        name_table = self.read_table(name_table)
        stm = db.select(name_table)
        return self.connection.execute(stm).fetchall()
    
    def delete_row_by_id(self, table, id_):
        name_table = self.read_table(table)
        primary_key = [column for column in name_table.columns.keys() if 'id_' in column][0]
        print(primary_key)
        id_column = getattr(name_table.c, primary_key, None)
    
        try:
            stmt = db.delete(name_table).where(name_table.c[primary_key] == id_)
            with self.engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
                print(f'Row with id {id_} deleted')
        except:
            print(f'Error: Column with id {id_} not found')

    def delete_table(self, table):
        name_table = self.read_table(table)
        if name_table is not None:name_table.drop(self.engine), print(f'Table {table} deleted successfully')
        else: print(f'Error: Table {table} not found')
        self.metadata.remove(self.read_table(table))
        self.table = list(self.metadata.tables.keys())

    from sqlalchemy import text
 
    def query_to_dataframe(self, query):
        return pd.read_sql(sql=str(query), con=self.engine.connect())
 
        

    def update_row_by_id(self, table, id_, **kwargs):
        name_table = self.read_table(table)
        primary_key = [column for column in name_table.columns.keys() if 'id_' in column][0]
        print(primary_key)
        id_column = getattr(name_table.c, primary_key, None)
        try:
            stmt = db.update(name_table).where(id_column == id_).values(**kwargs)
            self.connection.execute(stmt)
            print(f'Row with id {id_} updated')
        except:
            print(f'Error: Column with id {id_} not found')