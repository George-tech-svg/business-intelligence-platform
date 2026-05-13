"""
COMPARE ALL TRAINED MODELS
Loads all saved models and compares their performance on test data
"""

import mysql.connector
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LOCAL_DB_CONFIG, ACTIVE_DB

print('-'*140)
print("COMPARING ALL TRAINED MODELS")
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


def load_model(model_name):
    """Load a trained model from disk"""
    models_dir = Path(__file__).parent / "models"
    model_path = models_dir / f"{model_name}.pkl"
    
    if model_path.exists():
        return joblib.load(model_path)
    else:
        return None


def main():
    # Load data
    df = get_data()
    if df is None:
        return
    
    X = df[['rating_number']]
    y = df['price_numeric']
    
    # Split data (use same random state as training)
    _, X_test, _, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"Test set size: {len(X_test)} rows")
    
    # Models to compare
    models_to_compare = [
        ('linear_best', 'Linear (Best)'),
        ('decision_tree_best', 'Decision Tree'),
        ('random_forest_best', 'Random Forest'),
        ('gradient_boosting_best', 'Gradient Boosting')
    ]
    
    results = []
    
    print("\n" + '='*70)
    print("EVALUATING EACH MODEL ON TEST DATA")
    print('='*70)
    
    for model_file, display_name in models_to_compare:
        model = load_model(model_file)
        
        if model is not None:
            y_pred = model.predict(X_test)
            
            mae = mean_absolute_error(y_test, y_pred)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            r2 = r2_score(y_test, y_pred)
            
            results.append({
                'name': display_name,
                'model': model,
                'file': model_file,
                'mae': mae,
                'rmse': rmse,
                'r2': r2
            })
            
            print(f"\n{display_name}:")
            print(f"  MAE: ${mae:.2f}")
            print(f"  RMSE: ${rmse:.2f}")
            print(f"  R²: {r2:.4f}")
        else:
            print(f"\n{display_name}: MODEL NOT FOUND - Run training first")
    
    # Comparison table
    print("\n" + '='*140)
    print("FINAL COMPARISON - ALL MODELS")
    print('='*140)
    
    comparison_df = pd.DataFrame([
        {
            'Model': r['name'],
            'MAE ($)': round(r['mae'], 2),
            'RMSE ($)': round(r['rmse'], 2),
            'R² Score': round(r['r2'], 4)
        }
        for r in results
    ]).sort_values('MAE ($)')
    
    print(comparison_df.to_string(index=False))
    print('='*140)
    
    # Winner
    if results:
        winner = min(results, key=lambda x: x['mae'])
        
        print("\n" + '-'*70)
        print(f"WINNER: {winner['name']}")
        print(f"  MAE: ${winner['mae']:.2f}")
        print(f"  R²: {winner['r2']:.4f}")
        print('-'*70)
        
        # Save winner info
        winner_file = Path(__file__).parent / "models" / "best_model.txt"
        with open(winner_file, 'w') as f:
            f.write(f"best_model={winner['file']}\n")
            f.write(f"mae={winner['mae']}\n")
            f.write(f"r2={winner['r2']}\n")
            f.write(f"rmse={winner['rmse']}\n")
        print(f"\nWinner info saved to: {winner_file}")
        
        # Show predictions from winner
        print("\n" + '-'*70)
        print(f"PRICE PREDICTIONS USING WINNER: {winner['name']}")
        print('-'*70)
        for rating in range(1, 6):
            pred = winner['model'].predict([[rating]])[0]
            print(f"{rating} star: ${pred:.2f}")
    
    print('-'*140)
    print("STATUS: Model comparison complete")
    print('-'*140)


if __name__ == "__main__":
    main()