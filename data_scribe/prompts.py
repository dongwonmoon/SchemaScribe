COLUMN_DESCRIPTION_PROMPT = """  
Briefly describe the business meaning of the '{col_name}' ({col_type}) column in table '{table_name}' in under 15 characters.  
(Example: Unique identifier of a user)  
"""

DBT_MODEL_PROMPT = """  
You are a dbt expert.  
Below is the SQL query that defines the '{model_name}' dbt model.  
Summarize in 1-2 sentences what kind of data this model produces or aggregates from a business perspective.  

```sql
{raw_sql}
```  
"""

DBT_COLUMN_PROMPT = """  
You are a professional data analyst.  
In the dbt model '{model_name}', there is a column '{col_name}' ({col_type}).  
This column is created as part of the following SQL:  

SQL context:  
```sql
{raw_sql}
```  
Briefly describe the business meaning of the '{col_name}' column in under 15 characters.  
(Example: Total order amount per customer)  
"""
