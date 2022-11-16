# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.male_telco_customer AS
# MAGIC SELECT customerID, SeniorCitizen, Partner, tenure, MonthlyCharges  FROM main.default.telco_customer_churn_cleaned
# MAGIC WHERE gender = 'Male'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.female_telco_customer AS
# MAGIC SELECT customerID, SeniorCitizen, Partner, tenure, MonthlyCharges  FROM main.default.telco_customer_churn_cleaned
# MAGIC WHERE gender = 'Female'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.target_telco_customer AS
# MAGIC SELECT customerID, IFF(Churn = 'Yes', 1, 0) as label  FROM main.default.telco_customer_churn_cleaned

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.combined_customer AS
# MAGIC WITH customers AS (
# MAGIC   SELECT *, 1 as gender from main.default.male_telco_customer
# MAGIC   WHERE MonthlyCharges > 30
# MAGIC   UNION
# MAGIC   SELECT *, 0 as gender from main.default.female_telco_customer
# MAGIC   WHERE MonthlyCharges > 30
# MAGIC )
# MAGIC SELECT *  FROM customers
# MAGIC LEFT JOIN main.default.target_telco_customer USING (customerID)

# COMMAND ----------


