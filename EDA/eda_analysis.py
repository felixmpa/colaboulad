import pandas as pd
from tqdm import tqdm
from SQL.database import DatabaseConnection
from EDA.visualizations import Visualizations


class EDAAnalysis:
    def __init__(self):
        self.db = DatabaseConnection()

    def run(self):
        if not self.db.connect():
            print("Error al conectar a la base de datos.")
            return

        try:
            print("Preparando EDA para las visualizaciones (puede usar el Jupyter Notebook)...")

            queries = [
                ("student_info", "SELECT * FROM student_info"),
                ("student_registration", "SELECT * FROM student_registration"),
                ("student_vle", "SELECT * FROM student_vle"),
                ("assessments", "SELECT * FROM assessments"),
                ("student_assessment", "SELECT * FROM student_assessment"),
            ]

            dataframes = {}

            for name, query in tqdm(queries, desc="Cargando datasets", unit="tabla"):
                tqdm.write(f"Cargando tabla: {name}")
                dataframes[name] = pd.read_sql(query, self.db.connection)

            print("¡Ya! - Datos cargados.")

            # Extraer cada dataframe
            student_info = dataframes["student_info"]
            student_registration = dataframes["student_registration"]
            student_vle = dataframes["student_vle"]
            assessments = dataframes["assessments"]
            student_assessment = dataframes["student_assessment"]

            # --- Visualizaciones ---
            confusion_matrix = pd.crosstab(student_info['gender'], student_info['final_result'])
            Visualizations.plot_confusion_matrix(confusion_matrix)

            numeric_cols = ['num_of_prev_attempts', 'studied_credits']
            corr_matrix = student_info[numeric_cols].corr()
            Visualizations.plot_correlation_matrix(corr_matrix)

            Visualizations.plot_boxplot(student_info, 'num_of_prev_attempts', 'final_result')
            Visualizations.plot_histogram(student_info, 'studied_credits')
            Visualizations.plot_scatter(student_info, 'num_of_prev_attempts', 'studied_credits')

            Visualizations.plot_boxplot(student_info, 'studied_credits', 'age_band')
            Visualizations.plot_boxplot(student_info, 'studied_credits', 'imd_band')

            Visualizations.plot_registration_status_distribution(student_registration)
            Visualizations.plot_vle_weekly_interactions(student_vle)
            Visualizations.plot_vle_activity_type_distribution(student_vle)
            Visualizations.plot_assessment_type_distribution(assessments)
            Visualizations.plot_assessment_score_distribution(student_assessment)

        except Exception as e:
            print(f"Error en análisis EDA: {e}")
        finally:
            self.db.disconnect()
