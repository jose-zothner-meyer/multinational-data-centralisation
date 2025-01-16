# Multinational Retail Data Centralisation Project

## **Context**
In today's data-driven landscape, the organisation aims to consolidate its sales data into a **single centralised database** to serve as the **single source of truth** for all sales-related analysis. This initiative will improve data accessibility, consistency, and reliability across the organisation, enabling better decision-making and insights.

This project involves creating a system that:

- Centralises retail sales data from multiple sources.
- Cleanses and structures the data for efficient storage and retrieval.
- Implements a **star-schema** database model to enable advanced querying and analysis.

---

## **Aim**
The goal is to explore the end-to-end process of sourcing, cleaning, and centralising retail sales data into a database. By completing this project, I will gain hands-on experience in building a data pipeline that can extract, transform, and load (ETL) data from diverse sources into a structured database.

### **Objectives**
1. Extract sales data from multiple sources:
   - **AWS RDS**, **AWS S3**, **PDF files**, and **APIs**.
2. Clean and transform the data to ensure accuracy and consistency.
3. Push the cleansed data into a database as structured tables.
4. Design and implement a **star-schema** database model for querying.
5. Query the database to perform **sales analysis** and extract actionable insights.

---

## **Skills and Concepts**

### **Key Prerequisites**
- **Object-Oriented Programming (OOP)**
  - Designing classes, methods, and functions for modular, reusable code.
- **Pandas**
  - Reading, cleaning, and transforming data from various sources, and pushing it into a database.
- **AWS**
  - Using `boto3` for programmatic interaction with AWS services, and configuring AWS resources via the CLI.
- **SQL**
  - Creating database models (e.g., **star-schema**) and writing SQL queries for data extraction and analysis.

### **Other Prerequisites**
- **APIs**
  - Extracting store data programmatically.
- **Tabula**
  - Extracting structured data from PDF files (requires Java).
- **Data Formats**
  - Working with YAML, JSON, and CSV file types.

---

## **Secrets Management Using YAML**
- This project manages sensitive information (API keys, database credentials) via **YAML** files.
- These YAML files store secrets in a structured, human-readable format while remaining separate from the main codebase.

### **Use Cases for YAML in This Project**

1. **API Connection Keys**  
   ```yaml
   api_key: "YOUR_API_KEY"
1. **API Connection Keys**  
   ```yaml
   aws:
  access_key: "YOUR_ACCESS_KEY"
  secret_key: "YOUR_SECRET_KEY"
1. **API Connection Keys**  
   ```yaml
   RDS_USER: "YOUR_DATABASE_USER"
    RDS_PASSWORD: "YOUR_DATABASE_PASSWORD" 
    RDS_HOST: "YOUR_DATABASE_HOST"
    RDS_PORT: 5432
    RDS_DATABASE: "YOUR_DATABASE_NAME"

# Benefits of Using YAML

## Separation of Code and Secrets
Keeps secrets out of source files.

## Environment-Specific Configurations
Easily switch between development, staging, and production.

## Human-Readable Format
Straightforward to edit and understand.

---

# Security Note
- Always add your secret-containing YAML files to `.gitignore`.
- Consider using **AWS Secrets Manager** or **HashiCorp Vault** for production environments.

---

# Configuration Using `config.ini`

To avoid hardcoding non-sensitive configuration (like API endpoints, S3 URIs, or PDF links) in the code, this project uses a `config.ini` file with the `[API]` section:

## Example `config.ini`
```ini
[API]
stores_endpoint = https://example-execute-api.com/prod/number_stores
store_details_endpoint = https://example-execute-api.com/prod/store_details
pdf_link = https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf
s3_uri = s3://data-handling-public/products.csv
json_url = https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json
```

# Why Use config.ini?

## Centralised Configuration
All API-related URLs and resource locations are in one place.

## Maintainability
Updates to endpoints happen without changing source code—just edit `config.ini`.

## Version Control
You can commit `config.ini` if it contains no secrets (or else exclude it).

---

# How config.ini Is Used

- A `ConfigParser` (from Python’s standard library) reads these values in your code.
- The `DataExtractor` class can then reference `stores_endpoint`, `store_details_endpoint`, etc., from this file.

---

# Software Requirements

- **VSCode** (or another IDE) for developing and testing code.
- **Conda** for managing Python packages and creating isolated environments.
- **pgAdmin4** or **SQLTools** for interfacing with the PostgreSQL database.

---

# Requirements File

The `requirements.txt` file lists all the Python packages that are needed to run this project. It ensures that anyone who wants to run the project can install the exact versions of the packages that were used during development. To install the dependencies, run:

```bash
pip install -r requirements.txt
```
---

# Outcome

By completing this project, I will have:

- Built a robust **ETL pipeline** for retail sales data.
- Centralised the data into a **single database** for analysis.
- Designed a scalable **star-schema** model for efficient querying.
- Gained practical experience in data engineering and analytics, bridging raw data to actionable insights.
- Implemented secure **secrets management** for sensitive configurations using YAML.
- Maintained non-sensitive endpoints and resource locations in a `config.ini` for clean, modular code.
- Answered key business questions using <b>SQL</b> queries on the centralised database, such as:
  - Identifying the top-selling products across different regions.
  - Analyzing sales trends over time to forecast future sales.
  - Determining the most profitable stores and regions.
  - Evaluating the effectiveness of marketing campaigns based on sales data.
  - Understanding customer purchasing behavior and preferences.