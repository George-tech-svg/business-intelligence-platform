"""
PREDICT PRICES USING THE BEST MODEL
Loads the best model from comparison and makes predictions
"""

import joblib
from pathlib import Path

print('-'*140)
print("PRICE PREDICTION USING BEST MODEL")
print('-'*140)

models_dir = Path(__file__).parent / "models"
best_model_file = models_dir / "best_model.txt"

# Load best model info
if best_model_file.exists():
    with open(best_model_file, 'r') as f:
        content = f.read()
        for line in content.split('\n'):
            if line.startswith('best_model='):
                model_name = line.replace('best_model=', '').strip()
                break
        else:
            model_name = "linear_best"
else:
    print("No best model info found. Run 05_compare_all_models.py first.")
    model_name = "linear_best"

# Load the model
model_path = models_dir / f"{model_name}.pkl"

if not model_path.exists():
    print(f"ERROR: Model not found at {model_path}")
    print("Please run training files first:")
    print("  python 01_train_linear_models.py")
    print("  python 02_train_decision_tree.py")
    print("  python 03_train_random_forest.py")
    print("  python 04_train_gradient_boosting.py")
    exit()

model = joblib.load(model_path)
print(f"Loaded model: {model_name}")
print('-'*140)

print("\nENTER A RATING TO GET PRICE PREDICTION")
print("Ratings: 1, 2, 3, 4, or 5")
print('-'*140)

while True:
    try:
        user_input = input("\nEnter rating (1-5) or 'quit' to exit: ")
        
        if user_input.lower() == 'quit':
            print("\nGoodbye!")
            break
        
        rating = int(user_input)
        
        if rating < 1 or rating > 5:
            print("Please enter a rating between 1 and 5")
            continue
        
        predicted_price = model.predict([[rating]])[0]
        print(f"Predicted price for a {rating}-star book: ${predicted_price:.2f}")
        
    except ValueError:
        print("Please enter a valid number (1-5)")
    except Exception as error:
        print(f"Error: {error}")