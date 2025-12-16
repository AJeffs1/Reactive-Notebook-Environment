"""
Database Module

Handles Postgres database connections and SQL query execution.
Returns results as pandas DataFrames.
"""

from typing import Optional, Any
import pandas as pd


class DatabaseManager:
    """
    Manages Postgres database connections and query execution.
    """

    def __init__(self):
        self._connection = None
        self._connection_string: Optional[str] = None

    def connect(self, connection_string: str):
        """
        Connect to a Postgres database.

        Args:
            connection_string: Postgres connection string
                Format: postgresql://user:password@host:port/database

        Raises:
            Exception if connection fails
        """
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for database connections. "
                "Install it with: pip install psycopg2-binary"
            )

        # Close existing connection if any
        self.close()

        try:
            self._connection = psycopg2.connect(connection_string)
            self._connection_string = connection_string
        except Exception as e:
            self._connection = None
            self._connection_string = None
            raise Exception(f"Failed to connect to database: {e}")

    def is_connected(self) -> bool:
        """Check if database is connected."""
        if self._connection is None:
            return False

        try:
            # Test connection
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception:
            # Connection lost
            self._connection = None
            return False

    def close(self):
        """Close the database connection."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame.
        Handles both SELECT queries and DDL/DML statements.

        Args:
            query: SQL query string

        Returns:
            pandas DataFrame with query results (or status message for non-SELECT)

        Raises:
            Exception if query fails or not connected
        """
        if not self.is_connected():
            raise Exception("Not connected to database")

        try:
            # Check if this is a SELECT query
            query_stripped = query.strip().upper()
            is_select = query_stripped.startswith('SELECT') or query_stripped.startswith('WITH')

            if is_select:
                # Use pandas for SELECT queries
                df = pd.read_sql_query(query, self._connection)
                return df
            else:
                # For DDL/DML (CREATE, INSERT, UPDATE, DELETE, etc.)
                with self._connection.cursor() as cursor:
                    cursor.execute(query)
                    rowcount = cursor.rowcount
                self._connection.commit()

                # Return a status DataFrame
                if rowcount >= 0:
                    return pd.DataFrame({'status': [f'OK, {rowcount} rows affected']})
                else:
                    return pd.DataFrame({'status': ['OK']})

        except Exception as e:
            # Try to rollback on error
            try:
                self._connection.rollback()
            except Exception:
                pass
            raise Exception(f"Query failed: {e}")

    def execute_statement(self, statement: str) -> int:
        """
        Execute a SQL statement (INSERT, UPDATE, DELETE, etc.).

        Args:
            statement: SQL statement string

        Returns:
            Number of affected rows

        Raises:
            Exception if statement fails or not connected
        """
        if not self.is_connected():
            raise Exception("Not connected to database")

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(statement)
                rowcount = cursor.rowcount
            self._connection.commit()
            return rowcount
        except Exception as e:
            try:
                self._connection.rollback()
            except Exception:
                pass
            raise Exception(f"Statement failed: {e}")

    def get_tables(self) -> list[str]:
        """
        Get list of tables in the database.

        Returns:
            List of table names
        """
        if not self.is_connected():
            return []

        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """
        try:
            df = self.execute_query(query)
            return df['table_name'].tolist()
        except Exception:
            return []

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """
        Get schema information for a table.

        Args:
            table_name: Name of the table

        Returns:
            DataFrame with column information
        """
        query = f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        return self.execute_query(query)
