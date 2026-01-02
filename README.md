📊 Modern Azure Lakehouse Data Platform

A scalable Azure Databricks Lakehouse project implementing the Bronze–Silver–Gold (Medallion) architecture to deliver governed, analytics-ready datasets for reporting and business insights using Power BI.

📝 About the Project

This project demonstrates how to design and build a modern data platform on Azure that supports reliable data ingestion, structured transformations, and trusted analytics.

The solution separates data processing into clear layers—Bronze, Silver, and Gold—ensuring:

Data quality and consistency

Strong governance and security

Reusability and scalability

Auditability and traceability

It reflects real-world enterprise and government data engineering practices.

<img width="646" height="320" alt="image" src="https://github.com/user-attachments/assets/f4b61033-9371-43b6-b40b-e9c634cb9ffb" />







🧪 Data Layers Explained
🟫 Bronze Layer – Raw

One-to-one copy of source data

Stored in Delta format

Minimal transformations

Includes ingestion metadata

Supports reprocessing and audits

⚪ Silver Layer – Cleaned

Data quality checks

Deduplication and standardisation

Business rule application

Conformed datasets across sources

🟨 Gold Layer – Business

Aggregated and curated datasets

Fact and dimension tables

KPIs and metrics

Optimised for reporting

Single source of truth

🔐 Governance & Security

Centralised governance using Unity Catalog

Fine-grained access control (schema, table, column)

Data lineage tracking

Audit logging and ownership management
