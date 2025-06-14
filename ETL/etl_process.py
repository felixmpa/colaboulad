import pandas as pd
from pathlib import Path
from SQL.database import DatabaseConnection
from .data_cleaner import DataCleaner
from tqdm import tqdm

class ETLProcess:
    def __init__(self, data_path="./Datasets"):
        self.data_path = Path(data_path)
        self.db = DatabaseConnection()
        self.cleaner = DataCleaner()
        self.domain_maps = {}

    def run(self):
        print("\n=== INICIANDO PROCESO ETL OULAD ===\n")

        if not self.db.connect():
            return False

        try:
            print("1. Creando schema y tablas...")
            self._create_schema()

            print("\n2. Cargando tablas de dominio...")
            self._load_domain_tables()

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
        script_path = Path(__file__).parent.parent / "SQL" / "PhysicalSchema_OULAD.sql"
        if script_path.exists():
            self.db.execute_script(script_path)
        else:
            print(f"✗ No se encuentra el archivo: {script_path}")

    def _load_domain_tables(self):
        print("  - Analizando valores únicos...")
        student_info = pd.read_csv(self.data_path / "studentInfo.csv")
        assessments = pd.read_csv(self.data_path / "assessments.csv")
        vle = pd.read_csv(self.data_path / "vle.csv")

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
        values = [v for v in values if pd.notna(v) and str(v).strip()]
        data = [{column_name: v} for v in values]
        query = f"INSERT IGNORE INTO {table_name} ({column_name}) VALUES (:{column_name})"
        self.db.execute_many(query, data)
        id_column = table_name.replace('_domain', '_id')
        results = self.db.fetch_all(f"SELECT {id_column}, {column_name} FROM {table_name}")
        self.domain_maps[table_name] = {row[1]: row[0] for row in results}
        print(f"    ✓ {table_name}: {len(values)} valores")

    def _load_courses(self):
        print("  - Cargando courses...")
        df = pd.read_csv(self.data_path / "courses.csv")
        df = self.cleaner.clean_courses(df).where(pd.notnull(df), None)
        data = df.to_dict(orient="records")
        query = """
        INSERT IGNORE INTO courses (code_module, code_presentation, module_presentation_length)
        VALUES (:code_module, :code_presentation, :module_presentation_length)
        """
        self.db.execute_many(query, data)
        print(f"    ✓ {len(data)} registros")

    def _load_assessments(self):
        print("  - Cargando assessments...")
        df = pd.read_csv(self.data_path / "assessments.csv")
        df = self.cleaner.clean_assessments(df).where(pd.notnull(df), None)
        df['assessment_type_ordinal'] = df['assessment_type'].map(self.domain_maps['assessment_type_domain'])
        data = df.to_dict(orient="records")
        query = """
        INSERT IGNORE INTO assessments 
        (id_assessment, code_module, code_presentation, assessment_type, 
         assessment_type_ordinal, date, weight)
        VALUES (:id_assessment, :code_module, :code_presentation, :assessment_type, 
                :assessment_type_ordinal, :date, :weight)
        """
        self.db.execute_many(query, data)
        print(f"    ✓ {len(data)} registros")

    def _load_vle(self):
        print("  - Cargando vle...")
        df = pd.read_csv(self.data_path / "vle.csv")
        df = self.cleaner.clean_vle(df).where(pd.notnull(df), None)
        df['activity_type_ordinal'] = df['activity_type'].map(self.domain_maps['activity_type_domain'])
        data = df.to_dict(orient="records")
        query = """
        INSERT IGNORE INTO vle 
        (id_site, code_module, code_presentation, activity_type,
         activity_type_ordinal, week_from, week_to)
        VALUES (:id_site, :code_module, :code_presentation, :activity_type,
                :activity_type_ordinal, :week_from, :week_to)
        """
        self.db.execute_many(query, data)
        print(f"    ✓ {len(data)} registros")

    def _load_student_info(self):
        print("  - Cargando student_info...")
        df = pd.read_csv(self.data_path / "studentInfo.csv")
        df = self.cleaner.clean_student_info(df).where(pd.notnull(df), None)

        for field in ['gender', 'region', 'highest_education', 'imd_band', 'age_band', 'disability', 'final_result']:
            domain_table = "education_domain" if field == 'highest_education' else f"{field}_domain"
            df[f"{field}_ordinal"] = df[field].map(self.domain_maps[domain_table])

        df.rename(columns={'highest_education_ordinal': 'education_ordinal'}, inplace=True)
        data = df.to_dict(orient="records")
        query = """
        INSERT IGNORE INTO student_info 
        (id_student, code_module, code_presentation, gender, gender_ordinal, region, region_ordinal,
         highest_education, education_ordinal, imd_band, imd_band_ordinal, age_band, age_band_ordinal,
         num_of_prev_attempts, studied_credits, disability, disability_ordinal, final_result, final_result_ordinal)
        VALUES (:id_student, :code_module, :code_presentation, :gender, :gender_ordinal, :region, :region_ordinal,
                :highest_education, :education_ordinal, :imd_band, :imd_band_ordinal, :age_band, :age_band_ordinal,
                :num_of_prev_attempts, :studied_credits, :disability, :disability_ordinal, :final_result, :final_result_ordinal)
        """
        for i in tqdm(range(0, len(data), 1000), desc="    Insertando"):
            self.db.execute_many(query, data[i:i + 1000])
        print(f"    ✓ {len(data)} registros")

    def _load_student_registration(self):
        print("  - Cargando student_registration...")
        df = pd.read_csv(self.data_path / "studentRegistration.csv")
        df = self.cleaner.clean_student_registration(df).where(pd.notnull(df), None)
        data = df.to_dict(orient="records")
        query = """
        INSERT IGNORE INTO student_registration 
        (id_student, code_module, code_presentation, date_registration, date_unregistration)
        VALUES (:id_student, :code_module, :code_presentation, :date_registration, :date_unregistration)
        """
        self.db.execute_many(query, data)
        print(f"    ✓ {len(data)} registros")

    def _load_student_assessment(self):
        print("  - Cargando student_assessment...")
        df = pd.read_csv(self.data_path / "studentAssessment.csv")
        df = self.cleaner.clean_student_assessment(df).where(pd.notnull(df), None)
        data = df.to_dict(orient="records")
        query = """
        INSERT IGNORE INTO student_assessment 
        (id_assessment, id_student, date_submitted, is_banked, score)
        VALUES (:id_assessment, :id_student, :date_submitted, :is_banked, :score)
        """
        for i in tqdm(range(0, len(data), 5000), desc="    Insertando"):
            self.db.execute_many(query, data[i:i + 5000])
        print(f"    ✓ {len(data)} registros")

    def _load_student_vle(self):
        print("  - Cargando student_vle...")
        df = pd.read_csv(self.data_path / "studentVle.csv")
        df = self.cleaner.clean_student_vle(df).where(pd.notnull(df), None)
        data = df.to_dict(orient="records")
        query = """
        INSERT IGNORE INTO student_vle 
        (id_student, code_module, code_presentation, id_site, date, sum_click)
        VALUES (:id_student, :code_module, :code_presentation, :id_site, :date, :sum_click)
        """
        for i in tqdm(range(0, len(data), 10000), desc="    Insertando"):
            self.db.execute_many(query, data[i:i + 10000])
        print(f"    ✓ {len(data)} registros")
