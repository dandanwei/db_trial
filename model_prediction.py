# Databricks notebook source
telco_df = spark.table(f"main.default.combined_customer")

# COMMAND ----------

telco_pd = telco_df.toPandas()

# COMMAND ----------

score_df = telco_pd.iloc[3000:]
score_df.shape

# COMMAND ----------

score_df.head()

# COMMAND ----------

def transform(df, is_scoring=False):
    df['Partner'] = df['Partner'].apply(lambda v: 1 if v == 'Yes' else 0)
    if is_scoring:
        df = df.sample(frac=1.)
        df.reset_index(drop=True, inplace=True)
    return df

# COMMAND ----------

score_df = transform(score_df, is_scoring=True)

# COMMAND ----------

score_df.head()

# COMMAND ----------

features = ['SeniorCitizen', 'Partner', 'tenure', 'MonthlyCharges', 'gender']

# COMMAND ----------

X = score_df[features]
y = score_df['label']

# COMMAND ----------

import mlflow
logged_model = 'runs:/9b8010252ca34f51a47131905b7f192a/model'

# Load model as a PyFuncModel.
loaded_model = mlflow.sklearn.load_model(logged_model)

# Predict on a Pandas DataFrame.
import pandas as pd
pred = loaded_model.predict_proba(X)[:, -1]

# COMMAND ----------

from sklearn.metrics import roc_auc_score

# COMMAND ----------

roc_auc_score(y, pred)

# COMMAND ----------

score_df.columns

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

result_df = score_df[['customerID']]
result_df['timestamp'] = datetime.now()
result_df['score'] = pred


# COMMAND ----------

result_df.head()

# COMMAND ----------

result_df = spark.createDataFrame(result_df)

result_df.write.mode("append").saveAsTable("main.default.telco_churn_scores")


# COMMAND ----------

spark.table(f"main.default.telco_churn_scores").toPandas()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT timestamp from main.default.telco_churn_scores

# COMMAND ----------


