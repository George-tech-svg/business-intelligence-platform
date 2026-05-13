"""
TRAIN LINEAR MODELS (Linear, Ridge, Lasso) with hyperparameter tuning
Saves the best linear model to models/linear_best.pkl
"""

import mysql.connector
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
import warnings
warnings.filterwarnings('ignore')

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LOCAL_DB_CONFIG, ACTIVE_DB

print('-'*140)
print("TRAINING LINEAR MODELS (Linear, Ridge, Lasso)")
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
    print("TRAINING LINEAR REGRESSION MODELS")
    print('='*70)
    
    results = []
    
    # Model 1: Standard Linear Regression
    print("\n1. Standard Linear Regression")
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    lr_score = cross_val_score(lr, X_train, y_train, cv=5, scoring='r2').mean()
    print(f"   Cross-validation R²: {lr_score:.4f}")
    results.append(('linear_regression', lr, lr_score))
    
    # Model 2: Ridge Regression (with tuning)
    print("\n2. Ridge Regression (L2 Regularization)")
    ridge = Ridge()
    ridge_params = {'alpha': [0.01, 0.1, 1.0, 10.0, 100.0]}
    ridge_grid = GridSearchCV(ridge, ridge_params, cv=5, scoring='r2')
    ridge_grid.fit(X_train, y_train)
    print(f"   Best alpha: {ridge_grid.best_params_['alpha']}")
    print(f"   Best R²: {ridge_grid.best_score_:.4f}")
    results.append(('ridge_regression', ridge_grid.best_estimator_, ridge_grid.best_score_))
    
    # Model 3: Lasso Regression (with tuning)
    print("\n3. Lasso Regression (L1 Regularization)")
    lasso = Lasso()
    lasso_params = {'alpha': [0.01, 0.1, 1.0, 10.0, 100.0]}
    lasso_grid = GridSearchCV(lasso, lasso_params, cv=5, scoring='r2')
    lasso_grid.fit(X_train, y_train)
    print(f"   Best alpha: {lasso_grid.best_params_['alpha']}")
    print(f"   Best R²: {lasso_grid.best_score_:.4f}")
    results.append(('lasso_regression', lasso_grid.best_estimator_, lasso_grid.best_score_))
    
    # Find best linear model
    best = max(results, key=lambda x: x[2])
    
    print("\n" + '='*70)
    print(f"BEST LINEAR MODEL: {best[0]}")
    print(f"Best R²: {best[2]:.4f}")
    print('='*70)
    
    # Save best model
    save_model(best[1], "linear_best")
    
    # Evaluate on test set
    test_pred = best[1].predict(X_test)
    from sklearn.metrics import mean_absolute_error, r2_score
    test_mae = mean_absolute_error(y_test, test_pred)
    test_r2 = r2_score(y_test, test_pred)
    print(f"\nTest set performance:")
    print(f"  MAE: ${test_mae:.2f}")
    print(f"  R²: {test_r2:.4f}")
    
    print('-'*140)
    print("STATUS: Linear models training complete")
    print('-'*140)


if __name__ == "__main__":
    main()