import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import f_oneway

class Visualizations:
    @staticmethod
    def plot_confusion_matrix(confusion_matrix):
        """
        Muestra una matriz de confusión entre género y resultado final.
        Útil para detectar posibles brechas de género en el rendimiento académico.
        """
        plt.figure(figsize=(8, 6))
        sns.heatmap(confusion_matrix, annot=True, fmt='d', cmap='Blues')
        plt.xlabel('Final Result')
        plt.ylabel('Gender')
        plt.title('Confusion Matrix (Gender vs Final Result)')
        plt.show()

    @staticmethod
    def plot_correlation_matrix(corr_matrix):
        """
        Muestra una matriz de correlación entre variables numéricas.
        Útil para descubrir relaciones lineales entre variables como intentos previos y créditos estudiados.
        """
        plt.figure(figsize=(8, 6))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0)
        plt.title('Correlation Matrix (Numeric Variables)')
        plt.show()

    @staticmethod
    def plot_boxplot(df, column, by):
        """
        Muestra un boxplot de una variable numérica segmentada por una variable categórica.
        Útil para visualizar la distribución, la mediana y los outliers en diferentes grupos.
        """
        plt.figure(figsize=(10, 6))
        sns.boxplot(x=by, y=column, data=df)
        plt.title(f'Boxplot of {column} by {by}')
        plt.show()

    @staticmethod
    def plot_histogram(df, column):
        """
        Muestra un histograma de una variable numérica.
        Útil para comprender la distribución de la variable (asimetría, sesgo, normalidad).
        """
        plt.figure(figsize=(8, 6))
        sns.histplot(df[column], kde=True)
        plt.title(f'Histogram of {column}')
        plt.show()

    @staticmethod
    def plot_scatter(df, x, y):
        """
        Muestra un scatter plot (diagrama de dispersión) entre dos variables numéricas.
        Útil para detectar patrones, correlaciones o relaciones no lineales entre variables.
        """
        plt.figure(figsize=(8, 6))
        sns.scatterplot(x=x, y=y, data=df)
        plt.title(f'Scatter Plot of {x} vs {y}')
        plt.show()

    @staticmethod
    def plot_registration_status_distribution(df):
        """
        Muestra la proporción de estudiantes que completaron el curso vs los que se retiraron (churn).
        Útil para analizar tasas de retención de estudiantes.
        """
        status_counts = df['date_unregistration'].isnull().value_counts()
        labels = ['Completed', 'Withdrawn']
        plt.figure(figsize=(6, 6))
        plt.pie(status_counts, labels=labels, autopct='%1.1f%%', colors=['lightgreen', 'lightcoral'])
        plt.title('Registration Status Distribution')
        plt.show()

    @staticmethod
    def plot_vle_weekly_interactions(df):
        """
        Muestra la evolución semanal de las interacciones de los estudiantes con el entorno virtual (VLE).
        Útil para identificar patrones de engagement, como picos antes de exámenes o periodos de baja actividad.
        """
        weekly_interactions = df.groupby('date')['sum_click'].sum().reset_index()
        plt.figure(figsize=(10, 6))
        sns.lineplot(x='date', y='sum_click', data=weekly_interactions)
        plt.xlabel('Date')
        plt.ylabel('Total Interactions')
        plt.title('Weekly VLE Interactions')
        plt.show()

    @staticmethod
    def plot_vle_activity_type_distribution(df):
        """
        Muestra la distribución del número total de clics por tipo de actividad en el VLE.
        Útil para entender qué tipos de recursos son más utilizados (por ejemplo: foros, contenidos, quizzes).
        """
        activity_counts = df['activity_type'].value_counts()
        plt.figure(figsize=(10, 6))
        sns.barplot(x=activity_counts.index, y=activity_counts.values)
        plt.xlabel('Activity Type')
        plt.ylabel('Total Clicks')
        plt.title('Distribution of VLE Activity Types')
        plt.xticks(rotation=45)
        plt.show()

    @staticmethod
    def plot_assessment_type_distribution(df):
        """
        Muestra la distribución de los tipos de assessment (TMA, CMA, Exam).
        Útil para comprender la estructura de evaluación de los cursos.
        """
        assessment_type_counts = df['assessment_type'].value_counts()
        plt.figure(figsize=(8, 6))
        sns.barplot(x=assessment_type_counts.index, y=assessment_type_counts.values)
        plt.xlabel('Assessment Type')
        plt.ylabel('Count')
        plt.title('Distribution of Assessment Types')
        plt.show()

    @staticmethod
    def plot_assessment_score_distribution(df):
        """
        Muestra la distribución de las puntuaciones obtenidas en los assessments.
        Útil para analizar el rendimiento de los estudiantes en las diferentes evaluaciones.
        """
        plt.figure(figsize=(8, 6))
        sns.histplot(df['score'], kde=True)
        plt.xlabel('Score')
        plt.title('Distribution of Assessment Scores')
        plt.show()

    @staticmethod
    def run_anova(df, numeric_column, group_column):
        """
        Ejecuta un ANOVA unidireccional para comparar medias entre grupos.
        """
        groups = df[group_column].dropna().unique()
        samples = [df[df[group_column] == g][numeric_column].dropna() for g in groups]

        f_stat, p_val = f_oneway(*samples)

        print(f"\n--- ANOVA: {numeric_column} by {group_column} ---")
        print(f"F-statistic: {f_stat:.4f}")
        print(f"P-value: {'< 0.001' if p_val < 0.001 else f'{p_val:.4f}'}")

        if p_val < 0.05:
            print("Diferencias significativas entre al menos un par de grupos.")
        else:
            print("No hay diferencias significativas.")
