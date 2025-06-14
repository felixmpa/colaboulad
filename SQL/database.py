"""
Módulo de conexión y operaciones con la base de datos MySQL usando SQLAlchemy.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

class DatabaseConnection:
    def __init__(self):
        self.engine: Engine | None = None
        self.connection = None

    def connect(self):
        """Establece conexión con la base de datos MySQL usando SQLAlchemy."""
        try:
            user = os.getenv('DB_USER', 'root')
            password = os.getenv('DB_PASSWORD', 'root')
            host = os.getenv('DB_HOST', 'localhost')
            port = os.getenv('DB_PORT', '3306')
            database = os.getenv('DB_DATABASE', 'oulad')

            url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
            self.engine = create_engine(url)
            self.connection = self.engine.connect()
            print("✓ Conexión exitosa a MySQL con SQLAlchemy")
            return True
        except Exception as e:
            print(f"✗ Error al conectar con SQLAlchemy: {e}")
            return False

    def disconnect(self):
        """Cierra la conexión SQLAlchemy."""
        if self.connection:
            self.connection.close()
            print("✓ Conexión cerrada")

    def execute_script(self, script_path):
        """Ejecuta un script SQL desde un archivo."""
        try:
            with open(script_path, 'r') as file:
                sql_script = file.read()

            statements = [s.strip() for s in sql_script.split(';') if s.strip()]
            with self.engine.begin() as conn:
                for stmt in statements:
                    conn.execute(text(stmt))
            print(f"✓ Script ejecutado: {script_path}")
            return True
        except Exception as e:
            print(f"✗ Error ejecutando script: {e}")
            return False

    def execute_many(self, query, values_list):
        """
        Ejecuta múltiples inserciones usando SQLAlchemy.
        Requiere que el query use placeholders con nombre (:key)
        y que values_list sea una lista de diccionarios.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(query), values_list)
            return True
        except Exception as e:
            print(f"✗ Error en execute_many: {e}")
            return False

    def execute_many(self, query, values_list):
        """Ejecuta múltiples inserciones."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(query), values_list)
            return True
        except Exception as e:
            print(f"✗ Error en execute_many: {e}")
            return False
    def fetch_one(self, query, params=None):
        """Ejecuta una consulta SELECT y retorna un resultado."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchone()
        except Exception as e:
            print(f"✗ Error en fetch_one: {e}")
            return None

    def fetch_all(self, query, params=None):
        """Ejecuta una consulta SELECT y retorna todos los resultados."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchall()
        except Exception as e:
            print(f"✗ Error en fetch_all: {e}")
            return []