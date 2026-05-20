# process_etl
This is the process of Extract, Load, Transform project (DE Project): Medallion Architecture-Based Stock Market Data Pipeline.

This project is an end-to-end ETL pipeline built using PySpark and MySQL. The pipeline extracts stock market OHLCV data from the Alpha Vantage REST API, stores raw API responses in a Bronze layer, performs data cleansing and validation in a Silver layer, and applies business transformations in a Gold layer before loading the final analytics-ready data into MySQL using JDBC.

The project follows Medallion Architecture and uses modular components for extraction, transformation, loading, logging, and configuration management. I also implemented centralized logging, schema validation, duplicate checks, and externalized configuration handling for maintainability and scalability.


This project demonstrates REAL engineering practices:
Modular structure	
Config-driven design
Logging	Production 
Spark transformations	
JDBC loading	
Data quality checks	
Schema management	
API ingestion	
Medallion Architecture	
