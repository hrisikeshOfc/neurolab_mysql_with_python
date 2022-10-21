import mysql.connector as connection




class mysql_with_python:
    cursor = None
    connected_db = None
    def __init__(self,  host, username, password, database):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.connect_mysql()
        
      
    def connect_mysql(self):
        
        if (mysql_with_python.cursor is None
            and mysql_with_python.connected_db is None):
        
            connected_db = connection.connect(
                host = self.host,
                user= self.username,
                passwd = self.password,
                use_pure = True
                
            )
            
            if connected_db.is_connected():
                mysql_with_python.cursor = connected_db.cursor()
                mysql_with_python.connected_db = connected_db
        
    def create_database(self):
        #create database if does not exists
        cursor = mysql_with_python.cursor
        query_for_creating_database = f"Create Database if not exists {self.database}"
        cursor.execute(query_for_creating_database)
        
    def create_table(self, table_name, columns:list, column_types:list):
        self.create_database()
        columns_with_types = ', '.join([f"{key} {value}" for key, value in dict(zip(columns, column_types)).items()])
        query = f"CREATE TABLE {self.database}.{table_name} ({columns_with_types})"
        print(query) 
        
        
        mysql_with_python.cursor.execute(query)
        print("table created")
        
    def insert_data_to_table(self, table_name, data):
        cursor = mysql_with_python.cursor
      
        query = f"INSERT INTO {self.database}.{table_name} values({data});"
        cursor.execute(query)
        mysql_with_python.connected_db.commit()
        print("data inserted")
        
        
    
                
                
        
                
            
        
            
HOST = "PUT YOUR HOST URL"
USER = "PUT YOUR USERNAME"
DATABASE = "GIVE YOUR DATABASE NAME"
PASSWORD = "GIVE YOUR PASSWORD"

TABLE_NAME = "GIVE TABLE NAME"
COLUMNS = ['test_column_1', 'test_column_2', 'test_column_3', 'test_column_4']
COLUMN_TYPES = ["varchar(30)", 'varchar(30)', 'varchar(30)', 'varchar(30)']


DATA_TO_INSERT = " 'test done for column_1', 'test done for column_2', 'test done for column_3', 'test done column_4' "
mysql_connector = mysql_with_python(
    host=HOST,
    username=USER,
    password=PASSWORD,
    database=DATABASE
)

mysql_connector.create_table(
    table_name = TABLE_NAME,
    columns= COLUMNS,
    column_types= COLUMN_TYPES
)

mysql_connector.insert_data_to_table( table_name=TABLE_NAME,
                                     data = DATA_TO_INSERT)

