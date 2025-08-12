# HelloBetter Data Engineering Pipeline

## 📊 Workflow
![Workflow Diagram](https://github.com/user-attachments/assets/cecd64e4-e159-4c39-b200-606d6f10ba99)

---

## 📜 Project Context
This project was developed as part of the **HelloBetter Challenge** to design, build, and deploy a cloud-based data pipeline that integrates chatbot interaction data with weather information to explore correlations between environmental factors and user mood.

The pipeline:
- Extracts chatbot session logs from CSV files
- Enriches the data with weather details from **WeatherAPI.com**
- Performs transformations and modeling in **AWS Glue**
- Stores final datasets in **Amazon RDS**
- Creates an interactive dashboard in **Metabase** for analysis

---

## 🛠 Tools & Technologies

- **Cloud Platform:** Amazon Web Services (AWS)
- **Programming Language:** Python (data ingestion workflows, pipeline logic

- **Data Analytics:** SQL (exploration, analysis, querying)
- **Processing Engine:** Apache Spark (ETL in AWS Glue)
- **Storage:** Amazon S3, Amazon RDS (PostgreSQL)
- **Orchestration & Automation:** AWS Lambda, S3 Event Triggers
- **Dashboarding Tool:** Metabase

---

## 🧩 HelloBetter Challenge

**Objective:**  
You are joining the Data team at HelloBetter, a mental health platform providing digital therapeutic solutions. Your role as Data Engineer involves designing, building, and maintaining the data pipelines that extract data from various sources, transform it appropriately, and load it into our Data Warehouse for use by our Data Analysts, Data Scientists, ML Engineers, and stakeholders through Metabase.

HelloBetter has recently launched a chatbot that provides mental health support to users in the form of coaching. This chatbot generates detailed interaction logs, which are stored in a MongoDB database. Our Product team has requested that we integrate this data into our analytics platform and enrich it with weather data to analyze potential correlations between environmental factors and user mental states.

For example:  
> *Do individuals experience a happier mood on a sunny day?*
