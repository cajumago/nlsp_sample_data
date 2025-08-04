# import requests
from config import *
from gold_pipeline import *

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

from xgboost import XGBClassifier, XGBRegressor



# Read the data using Spark.SQL
nasdaq_data = spark.sql("""
SELECT * FROM nasdaq_uc_ws.3_gold_db.nlsp_obt_gold
""")

# Convert to Pandas DataFrame
df = nasdaq_data.toPandas()

#print(df.shape)
#df.head()


# # Implementing ML model to predict 'nocp'

columns = ["tracking_id", "symbol", "security_class", "price", "size", "sale_condition", "trading_state", "high_price", "low_price", "nocp", "consolidated_volume"]

df = df.reindex(columns=[])
df = df.rename(columns={})
#df.head()

df.isna().sum().sort_values(ascending=False)
# df.isnull().sum().sort_values(ascending=False)

X_train = train_df.drop(train_df[train_df[] > 2024].index)
X_train = X_train.iloc[:,1:-1].to_numpy()
X_valid = train_df.drop(train_df[train_df[] < 2025].index)
X_valid = X_valid.iloc[:,1:-1].to_numpy()
y_train = train_df.drop(train_df[train_df[] > 2024].index)
y_train = y_train.iloc[:,-1:].to_numpy()
y_valid = train_df.drop(train_df[train_df[] < 2025].index)
y_valid = y_valid.iloc[:,-1:].to_numpy()

# https://discuss.huggingface.co/t/problem-in-xgboost-with-hosted-infernece-api/30376/10
model = Pipeline([('transform', FunctionTransformer(np.float64)), 
                  ('regressor', XGBRegressor(n_estimators=1000, max_depth=4, learning_rate=0.1))])

#model = XGBRegressor(n_estimators=1000, max_depth=4, learning_rate=0.1)
#model = LinearRegression()
model.fit(X_train, y_train_trans)

preds = model.predict(X_valid)

# Convert X_train to DataFrame if it's not already
X_train_df = pd.DataFrame(X_train, columns=[f'feature_{i}' for i in range(X_train.shape[1])])

# Extract feature importances
feature_importances = pd.DataFrame(
    model.named_steps['regressor'].feature_importances_,
    index=X_train_df.columns.tolist(),
    columns=['importance']
)

# Sort feature importances
feature_importances.sort_values('importance', ascending=False, inplace=True)

display(feature_importances)