"""
Módulo de conexión y operaciones con la base de datos MySQL.
"""

import mysql.connector
from mysql.connector import Error
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class DatabaseConnection:
    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establece conexión con la base de datos MySQL."""
        try:
            self.connection = mysql.connector.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', 3306)),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', 'root'),
                database=os.getenv('DB_DATABASE', 'oulad')
            )
            self.cursor = self.connection.cursor()
            print("✓ Conexión exitosa a MySQL")
            return True
        except Error as e:
            print(f"✗ Error al conectar a MySQL: {e}")
            return False
    
    def disconnect(self):
        """Cierra la conexión con la base de datos."""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("✓ Conexión cerrada")
    
    def execute_script(self, script_path):
        """Ejecuta un script SQL desde un archivo."""
        try:
            with open(script_path, 'r') as file:
                sql_script = file.read()
            
            # Dividir por statements
            statements = [s.strip() for s in sql_script.split(';') if s.strip()]
            
            for statement in statements:
                if statement:
                    self.cursor.execute(statement)
            
            self.connection.commit()
            print(f"✓ Script ejecutado: {script_path}")
            return True
        except Error as e:
            print(f"✗ Error ejecutando script: {e}")
            self.connection.rollback()
            return False
    
    def execute_query(self, query, params=None):
        """Ejecuta una consulta SQL."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
            return True
        except Error as e:
            print(f"✗ Error en query: {e}")
            self.connection.rollback()
            return False
    
    def execute_many(self, query, data):
        """Ejecuta múltiples inserciones."""
        try:
            self.cursor.executemany(query, data)
            self.connection.commit()
            return True
        except Error as e:
            print(f"✗ Error en execute_many: {e}")
            self.connection.rollback()
            return False
    
    def fetch_one(self, query, params=None):
        """Ejecuta una consulta y retorna un resultado."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchone()
        except Error as e:
            print(f"✗ Error en fetch_one: {e}")
            return None
    
    def fetch_all(self, query, params=None):
        """Ejecuta una consulta y retorna todos los resultados."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as e:
            print(f"✗ Error en fetch_all: {e}")
            return []