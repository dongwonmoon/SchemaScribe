"""
This module contains the prompt templates used to generate descriptions with the LLM.

Each constant is a formatted string that provides context and instructions to the language model
for generating meaningful descriptions for database columns, dbt models, and dbt columns.
"""

# Prompt template for generating a description for a database column.
# This prompt asks the LLM to provide a brief, business-focused meaning of the column.
COLUMN_DESCRIPTION_PROMPT = """  
Briefly describe the business meaning of the '{col_name}' ({col_type}) column in table '{table_name}' in under 15 characters.  
(Example: Unique identifier of a user)  
"""

# Prompt template for generating a summary for a dbt model.
# This prompt provides the model's SQL code and asks the LLM to summarize its business purpose.
DBT_MODEL_PROMPT = """  
You are a dbt expert.  
Below is the SQL query that defines the '{model_name}' dbt model.  
Summarize in 1-2 sentences what kind of data this model produces or aggregates from a business perspective.  

```sql
{raw_sql}
```  
"""

# Prompt template for generating a description for a dbt column.
# This prompt provides the model's SQL and column details, asking for a concise business description.
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
