import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import accuracy_score, mean_squared_error
import argparse


def load_data(path: str) -> pd.DataFrame:
    """Load dataset and print basic statistics."""
    df = pd.read_csv(path)
    print(f"Dataset loaded with shape: {df.shape}")
    print("\n--- Describe (numeric) ---")
    print(df.describe().transpose())
    print("\n--- Describe (categorical) ---")
    cat_cols = df.select_dtypes(include="object").columns
    print(df[cat_cols].describe().transpose())
    return df


def prepare_features(df: pd.DataFrame, target_col: str) -> tuple:
    """Split dataframe into features and target with preprocessing pipeline."""
    X = df.drop(columns=[target_col], errors="ignore")
    y = df[target_col] if target_col in df.columns else None

    numeric_cols = X.select_dtypes(include="number").columns
    categorical_cols = X.select_dtypes(include="object").columns

    preprocessor = ColumnTransformer([
        ("categorical", OneHotEncoder(handle_unknown="ignore"), categorical_cols)
    ], remainder="passthrough")

    return X, y, preprocessor


def run_classification(df: pd.DataFrame) -> None:
    """Example binary classification using logistic regression."""
    target_col = "final_result"
    if target_col not in df.columns or df[target_col].nunique() <= 1:
        print("\n[Info] No hay variabilidad suficiente en 'final_result'. "
              "Creando columna 'passed' basada en studied_credits como ejemplo.")
        df[target_col] = (df["studied_credits"] >= df["studied_credits"].median()).astype(int)
    else:
        df[target_col] = df[target_col].map({"Pass": 1, "Fail": 0}).fillna(0)

    X, y, preprocessor = prepare_features(df, target_col)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    clf = Pipeline([
        ("preprocess", preprocessor),
        ("model", LogisticRegression(max_iter=1000))
    ])
    clf.fit(X_train, y_train)
    preds = clf.predict(X_test)
    acc = accuracy_score(y_test, preds)
    print(f"\nClassification accuracy: {acc:.3f}")


def run_regression(df: pd.DataFrame) -> None:
    """Example regression using linear regression."""
    target_col = "sum_clics"
    if target_col not in df.columns:
        raise ValueError("sum_clics column not found for regression example")

    X, y, preprocessor = prepare_features(df, target_col)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    reg = Pipeline([
        ("preprocess", preprocessor),
        ("model", LinearRegression())
    ])
    reg.fit(X_train, y_train)
    preds = reg.predict(X_test)
    rmse = mean_squared_error(y_test, preds, squared=False)
    print(f"\nRegression RMSE for {target_col}: {rmse:.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run basic models on OULAD data")
    parser.add_argument("--data", default="Datasets/OULAD_Experiment_cleaned.csv", help="Path to CSV file")
    args = parser.parse_args()

    df = load_data(args.data)
    run_classification(df.copy())
    run_regression(df.copy())


if __name__ == "__main__":
    main()
