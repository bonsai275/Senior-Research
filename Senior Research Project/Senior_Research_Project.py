
import sqlite3
import time
import os
import random
import string

# Step 1: Database Connection
class DatabaseConnection:
    @staticmethod
    def get_connection(db_name="test.db"):
        return sqlite3.connect(db_name)

# Step 2: Indexing Techniques
class IndexingTechniques:
    @staticmethod
    def create_index(connection, table_name, index_name, column_name):
        query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name})"
        with connection:
            connection.execute(query)

    @staticmethod
    def drop_index(connection, table_name, index_name):
        query = f"DROP INDEX IF EXISTS {index_name}"
        with connection:
            connection.execute(query)

# Step 3: Normalization Techniques
class NormalizationTechniques:
    @staticmethod
    def normalize_to_2nf(connection):
        query1 = """
        CREATE TABLE IF NOT EXISTS department (
            department_id INTEGER PRIMARY KEY,
            department_name TEXT
        )
        """
        query2 = """
        INSERT OR IGNORE INTO department (department_id, department_name)
        SELECT DISTINCT department_id, department_name
        FROM employees
        """

        query3 = """
        CREATE TABLE IF NOT EXISTS employees_normalized (
            employee_id INTEGER PRIMARY KEY,
            employee_name TEXT,
            department_id INTEGER,
            FOREIGN KEY(department_id) REFERENCES department(department_id)
        )
        """
        query4 = """
        INSERT OR IGNORE INTO employees_normalized (employee_id, employee_name, department_id)
        SELECT employee_id, employee_name, department_id
        FROM employees
        """
        with connection:
            connection.execute(query1)
            connection.execute(query2)
            connection.execute(query3)
            connection.execute(query4)

''' @staticmethod
    def normalize_to_3nf(connection):
        query1 = """
        CREATE TABLE IF NOT EXISTS project (
            project_id INTEGER PRIMARY KEY,
            project_name TEXT
        )
        """
        query2 = """
        INSERT INTO project (project_id, project_name)
        SELECT DISTINCT project_id, project_name
        FROM employees_normalized
        """
        query3 = """
        CREATE TABLE IF NOT EXISTS employees_fully_normalized (
            employee_id INTEGER PRIMARY KEY,
            employee_name TEXT,
            department_id INTEGER,
            project_id INTEGER,
            FOREIGN KEY(department_id) REFERENCES department(department_id),
            FOREIGN KEY(project_id) REFERENCES project(project_id)
        )
        """
        query4 = """
        INSERT INTO employees_fully_normalized (employee_id, employee_name, department_id, project_id)
        SELECT employee_id, employee_name, department_id, project_id
        FROM employees_normalized
        """
        with connection:
            connection.execute(query1)
            connection.execute(query2) # Move this line after query3
            connection.execute(query3)
            connection.execute(query4)
'''
# Step 4: Partitioning Strategies
class PartitioningTechniques:
    @staticmethod
    def horizontal_partitioning(connection, table_name, partition_column, partition_values):
        for value in partition_values:
            query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}_{value} AS
            SELECT * FROM {table_name} WHERE {partition_column} = '{value}'
            """
            with connection:
                connection.execute(query)

    @staticmethod
    def vertical_partitioning(connection, table_name, column_groups):
        for i, columns in enumerate(column_groups):
            query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}_part{i} AS
            SELECT {', '.join(columns)} FROM {table_name}
            """
            with connection:
                connection.execute(query)

# Step 5: Query Optimization Techniques
class QueryOptimizationTechniques:
    @staticmethod
    def apply_cost_based_optimization(connection, query):
        start_time = time.perf_counter()
        IndexingTechniques.create_index(connection, "employees", "idx_employee_name", "employee_name")
        connection.execute(query)
        connection.commit()
        end_time = time.perf_counter()
        print(f"Cost-based Optimization Time: {end_time - start_time} seconds")

    @staticmethod
    def apply_heuristic_optimization(connection, query):
        optimized_query = query.replace("*", "employee_name, department_name")
        start_time = time.perf_counter()
        connection.execute(optimized_query)
        connection.commit()
        end_time = time.perf_counter()
        print(f"Heuristic Optimization Time: {end_time - start_time} seconds")

# Step 6: Caching Mechanisms
class CachingMechanisms:
    result_cache = {}

    @staticmethod
    def get_cached_result(query):
        return CachingMechanisms.result_cache.get(query)

    @staticmethod
    def cache_result(query, result):
        CachingMechanisms.result_cache[query] = result

# Step 7: Implementation and Testing
class DatabaseBenchmarking:
    @staticmethod
    def setup_database(connection):
        query = """
        CREATE TABLE IF NOT EXISTS employees (
            employee_id INTEGER PRIMARY KEY,
            employee_name TEXT,
            department_id INTEGER,
            department_name TEXT,
            project_id INTEGER,
            project_name TEXT
        )
        """
        with connection:
            connection.execute(query)

    @staticmethod
    def populate_database(connection, num_records=100000):
        print(f"populating")
        departments = ["HR", "Finance", "IT", "Marketing", "Sales"]
        projects = ["ProjectA", "ProjectB", "ProjectC", "ProjectD", "ProjectE"]

        query = """
        INSERT INTO employees (employee_name, department_id, department_name, project_id, project_name)
        VALUES (?, ?, ?, ?, ?)
        """

        with connection:
            for i in range(num_records):
                employee_name = ''.join(random.choices(string.ascii_letters, k=10))
                department_id = random.randint(1, len(departments))
                department_name = departments[department_id - 1]
                project_id = random.randint(1, len(projects))
                project_name = projects[project_id - 1]
                connection.execute(query, (employee_name, department_id, department_name, project_id, project_name))
            connection.commit()

    @staticmethod
    def execute_query(connection, query):
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    @staticmethod
    def benchmark():
        connection = DatabaseConnection.get_connection()
        # Setup Database
        DatabaseBenchmarking.setup_database(connection)
        for i in range(100):
                print(f"Iteration {i+1}")
                # Populate Database with random data
                DatabaseBenchmarking.populate_database(connection, num_records=100000)

                # Sample Query
                query = "SELECT * FROM employees WHERE employee_name = 'John Doe'"

                # Indexing
                start_time = time.perf_counter()
                IndexingTechniques.create_index(connection, "employees", "idx_employee_name", "employee_name")
                DatabaseBenchmarking.execute_query(connection, query)
                end_time = time.perf_counter()
                print(f"Indexing Time: {end_time - start_time} seconds")

                # Normalization
                start_time = time.perf_counter()
                NormalizationTechniques.normalize_to_2nf(connection)
                DatabaseBenchmarking.execute_query(connection, query)
                end_time = time.perf_counter()
                print(f"Normalization to 2NF Time: {end_time - start_time} seconds")

                ''' start_time = time.perf_counter()
                NormalizationTechniques.normalize_to_3nf(connection)
                DatabaseBenchmarking.execute_query(connection, query)
                end_time = time.perf_counter()
                print(f"Normalization to 3NF Time: {end_time - start_time} seconds")
                '''
                # Partitioning
                start_time = time.perf_counter()
                PartitioningTechniques.horizontal_partitioning(connection, "employees", "department_id", ["1", "2", "3", "4", "5"])
                DatabaseBenchmarking.execute_query(connection, query)
                end_time = time.perf_counter()
                print(f"Horizontal Partitioning Time: {end_time - start_time} seconds")

                # Caching
                start_time = time.perf_counter()
                if not CachingMechanisms.get_cached_result(query):
                    result = DatabaseBenchmarking.execute_query(connection, query)
                    CachingMechanisms.cache_result(query, result)
                end_time = time.perf_counter()
                print(f"Caching Time: {end_time - start_time} seconds")

                # Query Optimization
                start_time = time.perf_counter()
                QueryOptimizationTechniques.apply_cost_based_optimization(connection, query)
                end_time = time.perf_counter()
                print(f"Cost-based Query Optimization Time: {end_time - start_time} seconds")

                start_time = time.perf_counter()
                QueryOptimizationTechniques.apply_heuristic_optimization(connection, query)
                end_time = time.perf_counter()
                print(f"Heuristic Query Optimization Time: {end_time - start_time} seconds")
                    # Clean up
                connection.execute("DELETE FROM employees")
                connection.commit()

        # Clean up
        connection.close()
        os.remove("test.db")  # Delete the test database after benchmarking

if __name__ == "__main__":
    DatabaseBenchmarking.benchmark()
