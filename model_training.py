# Databricks notebook source
telco_df = spark.table(f"main.default.combined_customer")

# COMMAND ----------

telco_pd = telco_df.toPandas()

# COMMAND ----------

telco_pd

# COMMAND ----------

telco_pd.label.sum()

# COMMAND ----------

telco_pd.shape

# COMMAND ----------

train_df = telco_pd.iloc[:3000]
train_df.shape

# COMMAND ----------

def transform(df):
    df['Partner'] = df['Partner'].apply(lambda v: 1 if v == 'Yes' else 0)
    df = df.sample(frac=1.)
    df.reset_index(drop=True, inplace=True)
    return df

# COMMAND ----------

train_df = transform(train_df)

# COMMAND ----------

train_df

# COMMAND ----------

features = ['SeniorCitizen', 'Partner', 'tenure', 'MonthlyCharges', 'gender']

# COMMAND ----------

X = train_df[features]
y = train_df['label']

# COMMAND ----------

from sklearn.model_selection import train_test_split
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2)

# COMMAND ----------

X_train.shape, X_val.shape

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score

# COMMAND ----------

import sklearn

import mlflow
import mlflow.shap
import mlflow.sklearn
from mlflow.models.signature import infer_signature

# COMMAND ----------



# COMMAND ----------

y_val.sum()

# COMMAND ----------

mlflow.sklearn.autolog()

# COMMAND ----------

with mlflow.start_run():
    params = {'n_estimators': 20, 'max_depth': 3}
    clf = RandomForestClassifier(**params)
    clf.fit(X_train, y_train)
    pred_val = clf.predict_proba(X_val)[:, -1]
    score = roc_auc_score(y_val, pred_val)
    print(score)
    mlflow.log_metric('roc_auc_val', score)
    mlflow.log_params(params)

    
    

# COMMAND ----------


