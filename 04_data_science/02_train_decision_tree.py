"""
TRAIN DECISION TREE with hyperparameter tuning
Saves the best model to models/decision_tree_best.pkl
"""

import mysql.connector
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
import warnings
warnings.filterwarnings('ignore')

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LOCAL_DB_CONFIG, ACTIVE_DB

print('-'*140)
print("TRAINING DECISION TREE WITH HYPERPARAMETER TUNING")
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
    print("TUNING DECISION TREE HYPERPARAMETERS")
    print('='*70)
    
    # Parameter grid to test
    param_grid = {
        'max_depth': [3, 5, 7, 10, None],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'criterion': ['squared_error', 'absolute_error']
    }
    
    print("\nTesting parameter combinations:")
    print(f"  max_depth: {param_grid['max_depth']}")
    print(f"  min_samples_split: {param_grid['min_samples_split']}")
    print(f"  min_samples_leaf: {param_grid['min_samples_leaf']}")
    print(f"  criterion: {param_grid['criterion']}")
    print(f"  Total combinations: {len(param_grid['max_depth']) * len(param_grid['min_samples_split']) * len(param_grid['min_samples_leaf']) * len(param_grid['criterion'])}")
    
    # Grid search
    dt = DecisionTreeRegressor(random_state=42)
    grid_search = GridSearchCV(dt, param_grid, cv=5, scoring='r2', n_jobs=-1)
    grid_search.fit(X_train, y_train)
    
    print("\n" + '='*70)
    print("BEST PARAMETERS FOUND:")
    print('='*70)
    for param, value in grid_search.best_params_.items():
        print(f"  {param}: {value}")
    
    print(f"\nBest cross-validation R²: {grid_search.best_score_:.4f}")
    
    # Save best model
    save_model(grid_search.best_estimator_, "decision_tree_best")
    
    # Evaluate on test set
    test_pred = grid_search.best_estimator_.predict(X_test)
    from sklearn.metrics import mean_absolute_error, r2_score
    test_mae = mean_absolute_error(y_test, test_pred)
    test_r2 = r2_score(y_test, test_pred)
    
    print(f"\nTest set performance:")
    print(f"  MAE: ${test_mae:.2f}")
    print(f"  R²: {test_r2:.4f}")
    
    # Feature importance
    print(f"\nFeature importance:")
    print(f"  rating_number: {grid_search.best_estimator_.feature_importances_[0]:.4f}")
    
    print('-'*140)
    print("STATUS: Decision Tree training complete")
    print('-'*140)


if __name__ == "__main__":
    main()