# secure_check
A Python-SQL Digital Ledger for Police Post Logs

# 🚓 SecureCheck: Police Check Post Analytics Dashboard

**SecureCheck** is a Python-SQL powered dashboard that enables real-time monitoring and predictive analytics for traffic stops, built using **Streamlit**, **PostgreSQL**, and **Plotly**. This project simulates a smart digital ledger used by law enforcement to track stops, record violations, and analyze officer performance and patterns in traffic law enforcement.

---

## 📦 Features

### 🔍 Public Overview
- View all historical police stops.
- Key metrics: total stops, arrests, warnings, drug-related stops.
- Visual dashboards:
  - Violation frequency (bar chart).
  - Driver gender distribution (pie chart).

### 📈 Deep Dive Analytics
- Choose between vehicle-based, demographic, time-based, violation, location, and complex insights.
- Predefined SQL-based analytics like:
  - Most searched vehicles.
  - Peak traffic stop times.
  - Common violations under age 25.
  - Arrest trends by country and demographics.

### 📝 New Entry + Prediction
- **Role-based access control**:
  - `Viewer`: can explore data and receive predictions.
  - `Officer`: login securely to insert new records into the database.
- Auto-prediction of:
  - Stop outcome (e.g., warning or arrest).
  - Likely violation (based on historical patterns).
- Insert driver, stop, and violation records into PostgreSQL.

---

## 🧰 Tech Stack

| Layer        | Technology                         |
|-------------|-------------------------------------|
| Frontend     | [Streamlit](https://streamlit.io)  |
| Backend      | Python (with `psycopg2`, `SQLAlchemy`) |
| Database     | PostgreSQL                         |
| Visualizations | Plotly Express                   |
| Others       | Pandas, Datetime, Role-based logic |

---
    
