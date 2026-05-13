"""
TRAIN GRADIENT BOOSTING with hyperparameter tuning
Saves the best model to models/gradient_boosting_best.pkl
"""

import mysql.connector
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
import warnings
warnings.filterwarnings('ignore')

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LOCAL_DB_CONFIG, ACTIVE_DB

print('-'*140)
print("TRAINING GRADIENT BOOSTING WITH HYPERPARAMETER TUNING")
print('-'*140)


def get_data():
    """Fetch data from database"""
    if ACTIVE_DB == 'local':
        db_config = LOCAL_DB_CONFIG
    else:
        print("ERROR: Cloud database not configured")
        return None
    
    connection = mysql.connector.connect(**db_config)
    query = """
    SELECT dr.rating_number, f.price_numeric
    FROM fact_book_analysis f
    INNER JOIN dim_rating dr ON f.rating_id = dr.rating_id
    """
    df = pd.read_sql(query, connection)
    connection.close()
    print(f"Loaded {len(df)} records")
    return df


def save_model(model, name):
    """Save model to disk"""
    models_dir = Path(__file__).parent / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    filename = models_dir / f"{name}.pkl"
    joblib.dump(model, filename)
    print(f"Saved: {filename}")
    return filename


def main():
    # Load data
    df = get_data()
    if df is None:
        return
    
    X = df[['rating_number']]
    y = df['price_numeric']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"Training: {len(X_train)} rows, Testing: {len(X_test)} rows")
    
    print("\n" + '='*70)
    print("TUNING GRADIENT BOOSTING HYPERPARAMETERS")
    print('='*70)
    
    # Parameter grid to test
    param_grid = {
        'n_estimators': [50, 100],
        'max_depth': [3, 5],
        'learning_rate': [0.01, 0.1],
        'min_samples_split': [2, 5]
    }
    
    print("\nTesting parameter combinations:")
    print(f"  n_estimators: {param_grid['n_estimators']}")
    print(f"  max_depth: {param_grid['max_depth']}")
    print(f"  learning_rate: {param_grid['learning_rate']}")
    print(f"  min_samples_split: {param_grid['min_samples_split']}")
    
    # Grid search
    gb = GradientBoostingRegressor(random_state=42)
    grid_search = GridSearchCV(gb, param_grid, cv=5, scoring='r2', n_jobs=-1)
    grid_search.fit(X_train, y_train)
    
    print("\n" + '='*70)
    print("BEST PARAMETERS FOUND:")
    print('='*70)
    for param, value in grid_search.best_params_.items():
        print(f"  {param}: {value}")
    
    print(f"\nBest cross-validation R²: {grid_search.best_score_:.4f}")
    
    # Save best model
    save_model(grid_search.best_estimator_, "gradient_boosting_best")
    
    # Evaluate on test set
    test_pred = grid_search.best_estimator_.predict(X_test)
    from sklearn.metrics import mean_absolute_error, r2_score
    test_mae = mean_absolute_error(y_test, test_pred)
    test_r2 = r2_score(y_test, test_pred)
    
    print(f"\nTest set performance:")
    print(f"  MAE: ${test_mae:.2f}")
    print(f"  R²: {test_r2:.4f}")
    
    print('-'*140)
    print("STATUS: Gradient Boosting training complete")
    print('-'*140)


if __name__ == "__main__":
    main()