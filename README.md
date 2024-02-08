# DataBaseV2 - Python Database Utility

### Overview
The `DataBaseV2` class is a Python utility for interacting with relational databases using SQLAlchemy. This utility is designed to simplify common database operations such as table creation, data retrieval, insertion, deletion, and updates. It currently supports SQLite, MySQL, and PostgreSQL database types.

### Dependencies
- Python 3.x
- SQLAlchemy 2.0.1
- pandas

### Installation
Ensure that the required dependencies are installed. You can install them using the following command:

```bash
pip install sqlalchemy pandas
```

### Usage

#### Initializing the Database Connection
```python
from DataBaseV2 import DataBaseV2

# Example for SQLite
db_instance = DataBaseV2(db_name='example_db', db_type='sqlite')

# Example for MySQL
db_instance = DataBaseV2(
    db_name='example_db',
    db_type='mysql',
    db_user='your_username',
    db_password='your_password',
    db_host='localhost',
    db_port=3306
)

# Example for PostgreSQL
db_instance = DataBaseV2(
    db_name='example_db',
    db_type='postgresql',
    db_user='your_username',
    db_password='your_password',
    db_host='localhost'
)
```

#### Creating a Table
```python
db_instance.create_table('example_table', id=db.Integer, name=db.String(255), age=db.Integer)
```

#### Adding a Row
```python
db_instance.add_row('example_table', name='John Doe', age=25)
```

#### Retrieving Data as DataFrame
```python
data = db_instance.dataframe('example_table')
print(data)
```

#### Deleting a Row by ID
```python
db_instance.delete_row_by_id('example_table', 1)
```

#### Updating a Row by ID
```python
db_instance.update_row_by_id('example_table', 1, name='Updated Name', age=30)
```

#### Querying Database and Getting Result as DataFrame
```python
query = "SELECT * FROM example_table WHERE age > 25"
result = db_instance.query_to_dataframe(query)
print(result)
```

#### Deleting a Table
```python
db_instance.delete_table('example_table')
```

### Note
- Ensure that you have the necessary privileges to perform operations on the specified database.
- Error handling has been implemented for common scenarios, but it's advisable to further enhance it based on specific use cases.

### Contributing
Feel free to contribute to the improvement and expansion of this utility. Fork the repository, make your changes, and submit a pull request.

### License
This project is licensed under the [MIT License](LICENSE).
