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

            # Estadísticas descriptiva y correlación

            for name, df in dataframes.items():
                print(f"\n=== Análisis Exploratorio: {name.upper()} ===")

                # Variables numéricas
                numeric_df = df.select_dtypes(include="number")
                if not numeric_df.empty:
                    print("\nEstadísticas numéricas:")
                    print(numeric_df.describe().round(2))

                    print("\nMatriz de correlación:")
                    print(numeric_df.corr().round(2))

                    Visualizations.print_strong_correlations(numeric_df)

                    # Descomenta si deseas ver el heatmap visual
                    # Visualizations.plot_correlation_heatmap(numeric_df, title=f"Matriz de Correlación: {name}")

                # Variables categóricas
                cat_cols = df.select_dtypes(include="object").columns
                if not cat_cols.empty:
                    print("\nEstadísticas categóricas:")
                    for col in cat_cols:
                        print(f"\n{col} (top 5):")
                        print(df[col].value_counts().head(5))

            if not cat_cols.empty:
                print("\nEstadísticas categóricas:")
                for col in cat_cols:
                    print(f"\n{col} (top 5):")
                    print(df[col].value_counts().head(5))

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

            # ANOVA: ¿hay diferencias en créditos estudiados entre bandas de edad?
            Visualizations.run_anova(student_info, 'studied_credits', 'age_band')

            # ANOVA: ¿hay diferencias en créditos estudiados según el resultado final?
            Visualizations.run_anova(student_info, 'studied_credits', 'final_result')

        except Exception as e:
            print(f"Error en análisis EDA: {e}")
        finally:
            self.db.disconnect()
