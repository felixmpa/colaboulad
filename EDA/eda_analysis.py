import pandas as pd
from tqdm import tqdm
from SQL.database import DatabaseConnection
from EDA.visualizations import Visualizations
from scipy.stats import mannwhitneyu
from scipy.stats import chi2_contingency
import numpy as np
from scipy.stats import skew
import scipy.stats as stats
from scipy.stats import kurtosis
import matplotlib.pyplot as plt
import seaborn as sns



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

            #Prueba Mann-Whitney U para final_result: retirados (2) vs no retirados (!=2)

            # Verificar media de final_result_ordinal
            print("Media general de final_result_ordinal:", 
                student_info['final_result_ordinal'].mean().round(2))

            # Separar grupos por género
            grupo_masculino = student_info.loc[
                student_info['gender'] == 'M', 
                'final_result_ordinal'
            ]
            grupo_femenino = student_info.loc[
                student_info['gender'] == 'F', 
                'final_result_ordinal'
            ]

            stat, p_value = mannwhitneyu(grupo_masculino, grupo_femenino, alternative='two-sided')

            print("\n--- Mann-Whitney U: final_result_ordinal por género ---")
            print(f"Estadístico U: {stat}")
            print(f"Valor p: {p_value:.5f}")

            if p_value < 0.05:
                print("Resultado: Hay diferencia significativa entre géneros.")
            else:
                print("Resultado: No hay diferencia significativa entre géneros.")

            #Chi cuadrado

            conf_matrix = pd.crosstab(
                student_info['gender'],
                student_info['final_result_ordinal']
            )

            chi2, p, dof, expected = chi2_contingency(conf_matrix)

            print("\n--- Test de Chi-cuadrado: Género vs final_result_ordinal ---")
            print(f"χ² = {chi2:.2f}")
            print(f"Grados de libertad = {dof}")
            print(f"p‑valor = {p:.5f}")

            # 2. Cálculo de Cramer's V para el tamaño del efecto
            n = conf_matrix.values.sum()
            r, c = conf_matrix.shape
            cramers_v = np.sqrt(chi2 / (n * (min(r, c) - 1)))

            print(f"Tamaño del efecto (Cramer's V) = {cramers_v:.3f}")

            if p < 0.05:
                print("→ Hay asociación estadísticamente significativa.")
            else:
                print("→ No hay asociación estadísticamente significativa.")


            # Matriz de proporciones por fila (cada género), usando la columna ordinal
            prop_matrix = pd.crosstab(
                student_info['gender'],
                student_info['final_result_ordinal'],
                normalize='index'   # normaliza cada fila a total = 1
            ).round(3) * 100        # redondea a 3 decimales y multiplica por %

            # Renombrar las columnas numéricas a sus etiquetas
            prop_matrix.columns = ['Pass', 'Withdrawn', 'Fail', 'Distinction']

            print("Porcentajes de final_result_ordinal por género (%):")
            print(prop_matrix)

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

            print("Asimetría de score en student_assessment:")
            print(student_assessment['score'].skew())

            def run_anova(df, numeric_col, group_col):
                groups = [group[numeric_col].dropna().values for _, group in df.groupby(group_col)]
                f_stat, p_val = stats.f_oneway(*groups)
                result_str = (
                    f"ANOVA results for {numeric_col} by {group_col}:\n"
                    f"F-statistic: {f_stat:.4f}\n"
                    f"p-value: {p_val:.4f}\n"
                )
                if p_val < 0.05:
                    result_str += "Hay diferencias significativas entre los grupos."
                else:
                    result_str += "No hay diferencias significativas entre los grupos."
                return result_str

            # Ejecutar y mostrar
            print(run_anova(student_info, 'studied_credits', 'age_band'))
            print(run_anova(student_info, 'studied_credits', 'final_result'))
       

            # Suponiendo que 'id_student' es la clave común en ambos
            df = student_assessment.merge(student_info[['id_student', 'gender']], on='id_student', how='left')

            kurtosis_by_gender = {}
            for gender, group in df.groupby('gender'):
                scores = group['score'].dropna()
                kurt = kurtosis(scores)
                kurtosis_by_gender[gender] = kurt

            for gender, kurt in kurtosis_by_gender.items():
                print(f'Curtosis de score para género {gender}: {kurt:.4f}')
            
            skewness_by_gender = {}
            for gender, group in df.groupby('gender'):
                scores = group['score'].dropna()
                skew_val = skew(scores)
                skewness_by_gender[gender] = skew_val

            # Mostramos resultados
            for gender, skew_val in skewness_by_gender.items():
                print(f"Asimetría (skewness) de score para género {gender}: {skew_val:.4f}")

            plt.figure(figsize=(8, 6))

            for gender in df['gender'].unique():
                sns.histplot(df[df['gender'] == gender]['score'], label=gender, kde=True, bins=20, alpha=0.5)

            plt.title('Distribución de Score por Género')
            plt.xlabel('Score')
            plt.ylabel('Frecuencia')
            plt.legend(title='Género')
            plt.grid(True)
            plt.show()
      
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
