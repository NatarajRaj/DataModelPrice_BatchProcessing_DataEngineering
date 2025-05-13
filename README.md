Financial Health Scoring Pipeline
Purpose
This project is designed to provide a comprehensive financial health scoring system for individuals, using transactional data, salary information, and various financial metrics. The pipeline processes raw data from multiple sources, performs aggregations, calculations, and applies scoring logic to generate an overall financial health score for each customer. This score can be used by financial institutions, credit scoring companies, or personal finance management applications to assess an individual's financial behavior and health.
Tech Stack
* Apache Spark (PySpark): For data processing, aggregation, and transformation.
* Delta Lake: For scalable and reliable storage of raw, silver, and gold data layers.
* SQL: For querying and aggregation in the Spark DataFrames.
* Apache Kafka: For handling real-time data streams (if applicable).
* Python: For scripting and implementing business logic.
* Jupyter/PyCharm: For development and testing.
* UUID: To generate unique identifiers for audit logging.
* Pandas (optional for offline processing): For small-scale operations or additional data manipulations.
Architecture
This project follows a multi-layered architecture for data processing and enrichment:
1. Bronze Layer (Raw Data Ingestion):
o Data is ingested from multiple sources, such as:
* Database (e.g., salary information, transactions) using JDBC.
* CSV/Parquet files (e.g., credit card statements, subscriptions).
o Raw data is written into Delta format for efficient querying and updates.
2. Silver Layer (Data Transformation and Aggregation):
o Data is transformed by applying various business rules, such as:
* Categorization of transactions (e.g., rent, utilities, subscriptions).
* Calculating rolling 3-month spend by category.
* Aggregating daily spend to calculate volatility (spend behavior consistency).
o Additional aggregations and metrics are computed, such as total spend and volatility index.
3. Gold Layer (Financial Health Score Calculation):
o Data from the Silver layer is joined with salary information to calculate key financial metrics such as:
* Discretionary Spend Ratio: Ratio of spend on non-essential items (subscriptions, discretionary purchases) vs. total spend.
* Fixed Commitment Ratio: Ratio of fixed financial obligations (rent, utilities, credit repayments) vs. salary.
* Salary to Spend Ratio: Comparison of salary to total spend.
* Volatility Index: A measure of the consistency of spending behavior.
* Recurring Spend Ratio: Ratio of recurring transactions to total transactions.
o A scoring logic is applied to each customer based on these metrics, with scores calculated on a scale.
o The final customer scores are stored in the Gold Layer for easy access.
4. Audit Log:
o A UUID is generated for each score calculation.
o Metadata, including calculation time and the entity performing the calculation, is stored for auditing purposes.
Data Pipeline
The project follows an ETL pipeline structure:
1. Extract: Data is extracted from different sources (databases, files).
2. Transform: Data is cleaned, categorized, and aggregated.
3. Load: Transformed data is loaded into Delta Lake tables (bronze, silver, and gold layers).
4. Score: Customer financial scores are calculated based on predefined rules.
5. Audit: Detailed logs and audit trails are maintained for transparency and tracking.
Use Cases
* Financial Institutions: Banks, credit unions, and fintech companies can use the financial health score to assess the creditworthiness of customers or create personalized financial products.
* Personal Finance Management: Individuals can monitor their financial behavior and track improvements over time.
* Debt Management: Institutions can use volatility and recurring spend ratios to identify customers who may be struggling with debt or mismanaging their finances.
Example Output
Here’s an example of the output produced by the pipeline:
Customer IDTotal SpendDiscretionary RatioFixed Commitment RatioSalary to Spend RatioVolatility IndexRecurring Spend RatioSDS ScoreScore Month12345120000.450.400.90150000.256502025-056789090000.550.601.2200000.155002025-05
