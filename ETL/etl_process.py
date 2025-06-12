"""
Proceso ETL principal para cargar datos OULAD a MySQL.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from .database import DatabaseConnection
from .data_cleaner import DataCleaner
from tqdm import tqdm

class ETLProcess:
    def __init__(self, data_path="./Datasets"):
        self.data_path = Path(data_path)
        self.db = DatabaseConnection()
        self.cleaner = DataCleaner()
        self.domain_maps = {}
        
    def run(self):
        """Ejecuta el proceso ETL completo."""
        print("\n=== INICIANDO PROCESO ETL OULAD ===\n")
        
        # 1. Conectar a la base de datos
        if not self.db.connect():
            return False
        
        try:
            # 2. Crear schema si no existe
            print("1. Creando schema y tablas...")
            self._create_schema()
            
            # 3. Cargar tablas de dominio
            print("\n2. Cargando tablas de dominio...")
            self._load_domain_tables()
            
            # 4. Cargar datos principales
            print("\n3. Cargando datos principales...")
            self._load_courses()
            self._load_assessments()
            self._load_vle()
            self._load_student_info()
            self._load_student_registration()
            self._load_student_assessment()
            self._load_student_vle()
            
            print("\n✓ PROCESO ETL COMPLETADO EXITOSAMENTE!")
            
        except Exception as e:
            print(f"\n✗ Error en proceso ETL: {e}")
            return False
        finally:
            self.db.disconnect()
        
        return True
    
    def _create_schema(self):
        """Ejecuta el script SQL para crear el schema."""
        script_path = Path(__file__).parent.parent / "SQL" / "PhysicalSchema_OULAD.sql"
        if script_path.exists():
            self.db.execute_script(script_path)
        else:
            print(f"✗ No se encuentra el archivo: {script_path}")
    
    def _load_domain_tables(self):
        """Carga los valores únicos en las tablas de dominio."""
        print("  - Analizando valores únicos...")
        
        # Leer archivos necesarios para extraer dominios
        student_info = pd.read_csv(self.data_path / "studentInfo.csv")
        assessments = pd.read_csv(self.data_path / "assessments.csv")
        vle = pd.read_csv(self.data_path / "vle.csv")
        
        # Cargar dominios
        self._load_domain('gender_domain', 'gender', student_info['gender'].unique())
        self._load_domain('region_domain', 'region', student_info['region'].unique())
        self._load_domain('education_domain', 'highest_education', student_info['highest_education'].unique())
        self._load_domain('imd_band_domain', 'imd_band', student_info['imd_band'].unique())
        self._load_domain('age_band_domain', 'age_band', student_info['age_band'].unique())
        self._load_domain('disability_domain', 'disability', student_info['disability'].unique())
        self._load_domain('final_result_domain', 'final_result', student_info['final_result'].unique())
        self._load_domain('assessment_type_domain', 'assessment_type', assessments['assessment_type'].unique())
        self._load_domain('activity_type_domain', 'activity_type', vle['activity_type'].unique())
    
    def _load_domain(self, table_name, column_name, values):
        """Carga valores únicos en una tabla de dominio."""
        # Limpiar valores nulos o vacíos
        values = [v for v in values if pd.notna(v) and str(v).strip()]
        
        # Insertar valores
        id_column = table_name.replace('_domain', '_id')
        for value in values:
            query = f"INSERT IGNORE INTO {table_name} ({column_name}) VALUES (%s)"
            self.db.execute_query(query, (value,))
        
        # Crear mapa de valores a IDs
        query = f"SELECT {id_column}, {column_name} FROM {table_name}"
        results = self.db.fetch_all(query)
        self.domain_maps[table_name] = {row[1]: row[0] for row in results}
        
        print(f"    ✓ {table_name}: {len(values)} valores")
    
    def _load_courses(self):
        """Carga la tabla courses."""
        print("  - Cargando courses...")
        df = pd.read_csv(self.data_path / "courses.csv")
        df = self.cleaner.clean_courses(df)
        
        data = df.to_records(index=False).tolist()
        query = "INSERT IGNORE INTO courses (code_module, code_presentation, length) VALUES (%s, %s, %s)"
        self.db.execute_many(query, data)
        print(f"    ✓ {len(data)} registros")
    
    def _load_assessments(self):
        """Carga la tabla assessments."""
        print("  - Cargando assessments...")
        df = pd.read_csv(self.data_path / "assessments.csv")
        df = self.cleaner.clean_assessments(df)
        
        # Agregar campo ordinal
        df['assessment_type_ordinal'] = df['assessment_type'].map(
            self.domain_maps['assessment_type_domain']
        )
        
        data = [(row['id_assessment'], row['code_module'], row['code_presentation'],
                row['assessment_type'], row['assessment_type_ordinal'], 
                row['date'], row['weight']) for _, row in df.iterrows()]
        
        query = """INSERT IGNORE INTO assessments 
                   (id_assessment, code_module, code_presentation, assessment_type, 
                    assessment_type_ordinal, date, weight) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        self.db.execute_many(query, data)
        print(f"    ✓ {len(data)} registros")
    
    def _load_vle(self):
        """Carga la tabla vle."""
        print("  - Cargando vle...")
        df = pd.read_csv(self.data_path / "vle.csv")
        df = self.cleaner.clean_vle(df)
        
        # Agregar campo ordinal
        df['activity_type_ordinal'] = df['activity_type'].map(
            self.domain_maps['activity_type_domain']
        )
        
        data = [(row['id_site'], row['code_module'], row['code_presentation'],
                row['activity_type'], row['activity_type_ordinal'],
                row['week_from'], row['week_to']) for _, row in df.iterrows()]
        
        query = """INSERT IGNORE INTO vle 
                   (id_site, code_module, code_presentation, activity_type,
                    activity_type_ordinal, week_from, week_to) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        self.db.execute_many(query, data)
        print(f"    ✓ {len(data)} registros")
    
    def _load_student_info(self):
        """Carga la tabla student_info."""
        print("  - Cargando student_info...")
        df = pd.read_csv(self.data_path / "studentInfo.csv")
        df = self.cleaner.clean_student_info(df)
        
        # Agregar campos ordinales
        for field in ['gender', 'region', 'highest_education', 'imd_band', 
                     'age_band', 'disability', 'final_result']:
            domain_table = f"{field}_domain"
            if field == 'highest_education':
                domain_table = "education_domain"
            df[f"{field}_ordinal"] = df[field].map(self.domain_maps[domain_table])
        
        # Renombrar columna para consistencia
        df.rename(columns={'highest_education_ordinal': 'education_ordinal'}, inplace=True)
        
        data = []
        for _, row in df.iterrows():
            data.append((
                row['id_student'], row['code_module'], row['code_presentation'],
                row['gender'], row['gender_ordinal'],
                row['region'], row['region_ordinal'],
                row['highest_education'], row['education_ordinal'],
                row['imd_band'], row['imd_band_ordinal'],
                row['age_band'], row['age_band_ordinal'],
                row['num_of_prev_attempts'], row['studied_credits'],
                row['disability'], row['disability_ordinal'],
                row['final_result'], row['final_result_ordinal']
            ))
        
        query = """INSERT IGNORE INTO student_info 
                   (id_student, code_module, code_presentation,
                    gender, gender_ordinal, region, region_ordinal,
                    highest_education, education_ordinal,
                    imd_band, imd_band_ordinal, age_band, age_band_ordinal,
                    num_of_prev_attempts, studied_credits,
                    disability, disability_ordinal,
                    final_result, final_result_ordinal) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        # Insertar en lotes
        batch_size = 1000
        for i in tqdm(range(0, len(data), batch_size), desc="    Insertando"):
            batch = data[i:i + batch_size]
            self.db.execute_many(query, batch)
        
        print(f"    ✓ {len(data)} registros")
    
    def _load_student_registration(self):
        """Carga la tabla student_registration."""
        print("  - Cargando student_registration...")
        df = pd.read_csv(self.data_path / "studentRegistration.csv")
        df = self.cleaner.clean_student_registration(df)
        
        data = []
        for _, row in df.iterrows():
            data.append((
                row['id_student'], row['code_module'], row['code_presentation'],
                row['date_registration'], row.get('date_unregistration', None)
            ))
        
        query = """INSERT IGNORE INTO student_registration 
                   (id_student, code_module, code_presentation,
                    date_registration, date_unregistration) 
                   VALUES (%s, %s, %s, %s, %s)"""
        self.db.execute_many(query, data)
        print(f"    ✓ {len(data)} registros")
    
    def _load_student_assessment(self):
        """Carga la tabla student_assessment."""
        print("  - Cargando student_assessment...")
        df = pd.read_csv(self.data_path / "studentAssessment.csv")
        df = self.cleaner.clean_student_assessment(df)
        
        data = []
        for _, row in df.iterrows():
            data.append((
                row['id_assessment'], row['id_student'],
                row['date_submitted'], row['is_banked'], row['score']
            ))
        
        query = """INSERT IGNORE INTO student_assessment 
                   (id_assessment, id_student, date_submitted, is_banked, score) 
                   VALUES (%s, %s, %s, %s, %s)"""
        
        # Insertar en lotes
        batch_size = 5000
        for i in tqdm(range(0, len(data), batch_size), desc="    Insertando"):
            batch = data[i:i + batch_size]
            self.db.execute_many(query, batch)
        
        print(f"    ✓ {len(data)} registros")
    
    def _load_student_vle(self):
        """Carga la tabla student_vle."""
        print("  - Cargando student_vle...")
        df = pd.read_csv(self.data_path / "studentVle.csv")
        df = self.cleaner.clean_student_vle(df)
        
        data = []
        for _, row in df.iterrows():
            data.append((
                row['id_student'], row['code_module'], row['code_presentation'],
                row['id_site'], row['date'], row['sum_click']
            ))
        
        query = """INSERT IGNORE INTO student_vle 
                   (id_student, code_module, code_presentation,
                    id_site, date, sum_click) 
                   VALUES (%s, %s, %s, %s, %s, %s)"""
        
        # Insertar en lotes grandes (esta tabla es muy grande)
        batch_size = 10000
        for i in tqdm(range(0, len(data), batch_size), desc="    Insertando"):
            batch = data[i:i + batch_size]
            self.db.execute_many(query, batch)
        
        print(f"    ✓ {len(data)} registros")