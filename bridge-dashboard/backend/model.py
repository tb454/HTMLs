import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import joblib


data = pd.read_csv('C:/Users/tbyer/BRidge-html/HTMLs/bridge-dashboard/data/cleaned_scrap_prices.csv')


# Define features and target variable (assume 'price' is the target)
X = data.drop('price', axis=1)
y = data['price']

# Optionally, perform preprocessing here (e.g., encoding categorical variables)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create and train the model
model = LinearRegression()
model.fit(X_train, y_train)

# Evaluate the model
print("Model R^2 score on test set:", model.score(X_test, y_test))

# Save the trained model to a file for later use
joblib.dump(model, 'price_prediction_model.pkl')
