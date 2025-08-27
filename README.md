# Project-Three
# Learner Overview Dashboard – Excelerate Data Visualization Virtual Internship 📊

<p align="center">
  <img width="300" height="112" src="https://github.com/user-attachments/assets/b980519a-c3f5-4cff-9769-0ad53abdba34" alt="Excelerate Dashboard Banner">
</p>

[![SQL](https://img.shields.io/badge/SQL-PostgreSQL-blue)](https://www.postgresql.org/)
[![Looker Studio](https://img.shields.io/badge/Looker_Studio-Visualization-green)](https://lookerstudio.google.com/)
[![Internship](https://img.shields.io/badge/Internship-Virtual-orange)](#)

This **interactive dashboard** provides a detailed overview of learners participating in the **Excelerate Data Visualization Virtual Internship**, showing learner counts, demographics, and distributions by **country, age group, degree level, and gender**. The data pipeline used **SQL**, **PGAdmin 4**, and **Looker Studio** for processing and visualization.

---


## 📸 Visual Preview
![Learner_Overview_Dashboard1_page-0001](https://github.com/user-attachments/assets/7a2a944e-3ff9-4e7c-9b2b-a4c63e918c20)



---

## 🗂 Data Processing

### SQL Data Cleaning
The dataset contained inconsistencies like missing values, duplicates, and formatting issues. SQL queries were used to:  

- ✅ Remove duplicate learner entries  
- ✅ Standardize categorical fields (country, degree, gender)  
- ✅ Handle missing/null values in age, gender, and degree  
- ✅ Aggregate data for visualizations (e.g., total learners by country, age group, or degree)  

### Database Management
- Managed using **PGAdmin 4** with **PostgreSQL**  
- Cleaned datasets stored in structured tables for efficient querying  
- Used **joins, filters, and aggregation queries** to prepare data for Looker Studio  

---

## 📊 Dashboard Features

| Feature | Insight | Visualization |
|---------|---------|---------------|
| Total Unique Learners | 136,776 learners | Numeric Highlight |
| Learners by Country | Top countries: India (33,888), Nigeria (31,134), Pakistan (14,843) | Horizontal Bar Chart |
| Learners by Age Group | Largest groups: Unknown (32.9%), 25–34 (32.1%), 18–24 (30.1%) | Pie Chart |
| Learners by Degree Level | Most learners are undergraduates & graduates | Bar Chart |
| Learners by Gender | Male: 39.5%, Female: 28.1%, Rather Not Say: 32.5% | Donut Chart |

---

## 🔑 Key Insights
- **Demographics:** Most learners are young adults and students  
- **Global Reach:** Learners mainly from India, Nigeria, and Pakistan  
- **Education Background:** Strong interest from undergraduate and graduate learners  
- **Gender Distribution:** Balanced, with many learners preferring not to disclose  

---

## ⚙️ Tools & Technologies
- **Data Cleaning & Aggregation:** SQL  
- **Database Management:** PostgreSQL, PGAdmin 4  
- **Visualization:** Looker Studio  

---

Made with ❤️ by **Sama Ahmed**
