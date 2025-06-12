"""
Módulo de limpieza de datos para OULAD.
"""

import pandas as pd
import numpy as np

class DataCleaner:
    """Clase para limpiar y validar datos OULAD."""
    
    def clean_courses(self, df):
        """Limpia datos de courses."""
        # Eliminar duplicados
        df = df.drop_duplicates()
        
        # Validar que length sea positivo
        df = df[df['length'] > 0]
        
        # Asegurar tipos de datos
        df['length'] = df['length'].astype(int)
        
        return df
    
    def clean_assessments(self, df):
        """Limpia datos de assessments."""
        # Eliminar duplicados
        df = df.drop_duplicates()
        
        # Manejar fechas faltantes (exámenes finales)
        # Si date es NaN, asignar un valor alto (fin del curso)
        df['date'] = df['date'].fillna(999)
        
        # Convertir a entero
        df['date'] = df['date'].astype(int)
        df['weight'] = df['weight'].astype(int)
        
        # Validar pesos
        df = df[df['weight'] >= 0]
        
        return df
    
    def clean_vle(self, df):
        """Limpia datos de VLE."""
        # Eliminar duplicados
        df = df.drop_duplicates()
        
        # Manejar valores nulos en semanas
        df['week_from'] = df['week_from'].fillna(0).astype(int)
        df['week_to'] = df['week_to'].fillna(df['week_from']).astype(int)
        
        # Asegurar que week_to >= week_from
        df.loc[df['week_to'] < df['week_from'], 'week_to'] = df['week_from']
        
        return df
    
    def clean_student_info(self, df):
        """Limpia datos de student_info."""
        # Eliminar duplicados
        df = df.drop_duplicates()
        
        # Manejar valores nulos
        df['num_of_prev_attempts'] = df['num_of_prev_attempts'].fillna(0).astype(int)
        df['studied_credits'] = df['studied_credits'].fillna(0).astype(int)
        
        # Limpiar valores de texto
        text_columns = ['gender', 'region', 'highest_education', 'imd_band', 
                       'age_band', 'disability', 'final_result']
        for col in text_columns:
            df[col] = df[col].fillna('Unknown')
            df[col] = df[col].str.strip()
        
        # Normalizar valores de disability
        df['disability'] = df['disability'].replace({'Y': 'Yes', 'N': 'No'})
        
        return df
    
    def clean_student_registration(self, df):
        """Limpia datos de student_registration."""
        # Eliminar duplicados
        df = df.drop_duplicates()
        
        # Convertir fechas a enteros
        df['date_registration'] = df['date_registration'].fillna(0).astype(int)
        
        # date_unregistration puede ser NaN (estudiantes que completaron)
        if 'date_unregistration' in df.columns:
            df['date_unregistration'] = df['date_unregistration'].replace('?', np.nan)
            df['date_unregistration'] = pd.to_numeric(df['date_unregistration'], errors='coerce')
        
        return df
    
    def clean_student_assessment(self, df):
        """Limpia datos de student_assessment."""
        # Eliminar duplicados
        df = df.drop_duplicates()
        
        # Convertir is_banked a booleano
        df['is_banked'] = df['is_banked'].astype(int).astype(bool)
        
        # Manejar fechas faltantes
        df['date_submitted'] = df['date_submitted'].fillna(0).astype(int)
        
        # Validar scores
        df['score'] = df['score'].clip(0, 100)
        
        return df
    
    def clean_student_vle(self, df):
        """Limpia datos de student_vle."""
        # Esta tabla puede ser muy grande, procesar por chunks si es necesario
        
        # Eliminar duplicados (puede haber múltiples interacciones)
        # En este caso, agrupar por clave y sumar clicks
        df = df.groupby(['id_student', 'code_module', 'code_presentation', 
                        'id_site', 'date'], as_index=False)['sum_click'].sum()
        
        # Asegurar tipos de datos
        df['date'] = df['date'].astype(int)
        df['sum_click'] = df['sum_click'].astype(int)
        
        # Validar clicks positivos
        df = df[df['sum_click'] > 0]
        
        return df