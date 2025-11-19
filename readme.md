# 🚀 Data Warehouse ETL Pipeline: SCD Implementation

This project outlines an Extraction, Transformation, and Loading (ETL) pipeline designed to load customer and order data from a SQL Server source, applying various Slowly Changing Dimension (SCD) techniques to the **Customer Dimension** to manage data history and changes over time.

## 🎯 Goal
The primary objective is to demonstrate and apply the logic for **SCD Types 1, 2, 3, 4, and 5** directly using Python (Pandas) for data transformation logic and SQL Server (via `pyodbc`) for source extraction and potential T-SQL-based merging.

---

## 💾 Data Sources

The pipeline utilizes the following two main datasets and one incremental data file:

1.  **`customers` (Customer Dimension)**: The primary dimension table, representing the baseline customer records.
2.  **`orders` (Sales Fact)**: The transactional data linked to the customers.
3.  **`new_customers` (Staging/Incremental Data)**: A simulation of new or updated records received during a daily incremental load.

### Key Fields and SCD Usage

| Table | Field | Role in Dimensional Model | SCD Type Applied (in Code) |
| :--- | :--- | :--- | :--- |
| `customers` | `customer_id` | **Natural Key** | All SCD Types (for identification) |
| `customers` | `email`, `phone` | Descriptive Attribute | **SCD Type 1** (Overwrite) |
| `customers` | `address` | Descriptive Attribute | **SCD Type 2** (New Row) |
| `customers` | `loyalty_status` | Descriptive Attribute | **SCD Type 3** (New Column) |
| `orders` | `customer_id` | Foreign Key | Links to the appropriate `Customer_SKey` |

---

## 💻 Transformation and SCD Logic

The transformation logic is implemented entirely using Pandas operations within the functions defined in your script.

### 1. Data Cleaning and Standardization

The `correct_contact_format()` and `correct_format()` functions handle initial data quality by:
* **Missing/Incorrect Emails:** Replacing invalid or missing email addresses with a standardized format (e.g., `[name]@email.com`).
* **Missing/Incorrect Phones:** Replacing invalid or missing phone numbers with a placeholder (`000-000-0000`).
* **Date Formatting:** Converting the `registration_date` column to the correct datetime object.

### 2. Slowly Changing Dimension (SCD) Implementations

These methods simulate how a production ETL process updates the main dimension table when new data arrives (`new_customers`):

| SCD Type | Code Function | Core Logic | Preservation |
| :--- | :--- | :--- | :--- |
| **SCD Type 1: Overwrite** | `scd_type_1()` | Updates the existing record directly with new values (e.g., updating **Email** or **Phone**). | **None**. Old values are lost. |
| **SCD Type 2: Add New Row** | `scd_type_2()` | Expires the old record (`is_current = False`, sets `end_date`) and inserts the new record (`is_current = True`, sets `start_date`). | **Full History**. Every state is maintained as a separate row. |
| **SCD Type 3: New Column** | `scd_type_3()` | Uses an additional column (`prev_loyalty_status`) to track the immediately preceding value. | **Limited History** (only current and immediate past). |
| **SCD Type 4: History Table** | (Planned) | Keeps the `Customer_Dim` table as Type 1 or Type 3, but moves all historical versions to a separate `Customer_Dim_History` table. | **Full History** (in a separate table). |
| **SCD Type 5: Hybrid (Type 1 + 2)** | (Planned) | Combines Type 1 overwrites for certain attributes and Type 2 new rows for critical attributes. Includes a foreign key in the new row back to the *original* record. | **Full History** + **Immediate Access** to Type 1 attributes. |

---

## 🛠️ Execution and SQL Server Integration

Your environment setup assumes the use of `pyodbc` for interaction with the SQL Server database.

1.  **Extraction (`pyodbc`):** In a full pipeline, the Python script would use `pyodbc` to send `SELECT` statements to SQL Server (e.g., `SELECT * FROM SourceDB.dbo.Customers`) to fetch both the `customers` and `new_customers` data.
2.  **Staging (T-SQL):** For high-volume updates, the cleaned Python data would be loaded into a **staging table** in SQL Server.
3.  **Load/Merge (T-SQL):** Instead of using Pandas `concat`/`merge`, production efficiency demands a single **T-SQL `MERGE` statement** to apply the complex Type 2/3/4 logic directly on the server, ensuring transactional integrity. `pyodbc` would be used to execute this powerful `MERGE` command.

**Future Steps for Full Implementation:**
* Implement dedicated functions for **SCD Type 4** and **SCD Type 5** patterns, potentially by utilizing helper tables (e.g., a dedicated history table and a "current key" mapping table for Type 5).
.