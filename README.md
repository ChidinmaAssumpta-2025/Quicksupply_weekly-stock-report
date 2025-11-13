# 🏪 QuickSupply ETL Pipeline — End-to-End Data Engineering Project  

## 📊 Project Overview  
**QuickSupply** is a small business that supplies essential goods (like beverages, cleaning products, and food items) to local shops across different towns.  

Recently, the company has been facing:  
- Frequent **stockouts** on certain items  
- **Excess inventory** of slow-moving products  
- **Supplier reliability** concerns  

The management suspects that:  
- Demand differs across locations  
- Some products are **overstocked or understocked**  
- Certain suppliers are **inconsistent**  

This project provides a **data-driven solution** by building an **end-to-end ETL (Extract, Transform, Load)** pipeline to collect, process, and store stock and product data from **KoboToolbox** into **PostgreSQL** for analysis and decision-making.  

---

## 🚀 Project Goals  
- Collect data from field agents using **KoboToolbox forms**.  
- Automate **data extraction** using **Python** and environment variables (`.env`).  
- **Transform** and clean the collected CSV data for consistency and accuracy.  
- **Load** the processed data into a **PostgreSQL database** for analysis.  
- Enable QuickSupply management to identify:  
  - High-demand vs. low-demand products  
  - Stock imbalances across regions  
  - Supplier performance patterns  

---

## 🛠️ Tech Stack  

| Component | Technology |
|------------|-------------|
| **Data Source** | KoboToolbox |
| **Programming Language** | Python |
| **Database** | PostgreSQL |
| **Environment Management** | `.env` file via `python-dotenv` |
| **Libraries** | `pandas`, `requests`, `psycopg2`, `dotenv` |


## 🧩 ETL Pipeline Breakdown  

### 1️⃣ Extract  
Fetches live data from **KoboToolbox API** using secure authentication (via `.env` file).  

### 2️⃣ Transform  
Reads CSV data and handles missing or invalid rows using **pandas**.  
Cleans column names, formats dates, and prepares data for loading.  

### 3️⃣ Load  
Automatically creates schema and table in **PostgreSQL**.  
Inserts all transformed records into the target table (e.g., `customers_feedback`).  

---

## 🔐 Environment Variables  
Create a `.env` file in your project root.  



## ▶️ How to Run the Project  

### 1️⃣ Clone the repository
```bash
git clone https://github.com/yourusername/customers-feedback-etl.git
cd customers-feedback-etl
2️⃣ Install dependencies
bash
Copy code
pip install -r requirements.txt
3️⃣ Create a .env file
Fill in your KoboToolbox and PostgreSQL credentials.

4️⃣ Run the ETL pipeline
bash
Copy code
python main.py
✅ Once complete, your data will be available in PostgreSQL under:
Schema: chidinma_1
Table: customers_feedback

📈 Business Impact
This project enables QuickSupply to:

Monitor stock and product performance by region.

Detect stock management issues early.

Evaluate supplier reliability and consistency.

Reduce stockouts and overstocking through data-driven insights.

💡 Key Highlights
✅ Fully automated end-to-end ETL pipeline — no manual data entry.
✅ Follows best practices for secure credential management using .env.
✅ Demonstrates a complete data engineering workflow — from collection to storage.
✅ Scalable and adaptable for additional data sources or schemas.

👩‍💻 Author
Chidinma Assumpta Nnadi
Data Engineer | Data Analyst | Problem Solver
📧 [chidinmaassumpta53@gmail.com]



