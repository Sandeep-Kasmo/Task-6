import pyodbc
from transform import *
print(pyodbc.drivers())
def connectDB():
    conn=pyodbc.connect(
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=localhost\SQLEXPRESS;'  # OR 'Server=.\SQLEXPRESS;'
        'Trusted_Connection=yes;'
        'Database=PythonLearningDB;'
    )
    cursor=conn.cursor()
conn=pyodbc.connect(
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=localhost\SQLEXPRESS;'  # OR 'Server=.\SQLEXPRESS;'
        'Trusted_Connection=yes;'
        'Database=PythonLearningDB;'
    )
cursor=conn.cursor()
def LoadData():
    connectDB()
    conn.execute(
        '''CREATE TABLE Customer_Order_Wide (
    -- Surrogate Key (Primary Key for the Dimension/Version)
    Customer_SKey INT IDENTITY(1,1) PRIMARY KEY,

    -- Customer Dimension Attributes (Natural Key)
    customer_id INT NOT NULL, 
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    address VARCHAR(500),
    registration_date DATE,
    
    -- SCD Type 2 Attributes (Version Tracking)
    start_date DATE,
    end_date DATE,
    is_current BIT, -- 1 for current version, 0 for history

    -- SCD Type 3 Attribute
    loyalty_status VARCHAR(50), 
    prev_loyalty_status VARCHAR(50), 

    -- Fact/Order Attributes
    order_id VARCHAR(100) NOT NULL, -- Could be unique per transaction
    order_date DATE,
    order_amount DECIMAL(10, 2),
    order_status VARCHAR(50),
    product_category VARCHAR(100),
    
    -- Constraint to enforce uniqueness for the combination of customer version and order transaction
    UNIQUE (Customer_SKey, order_id)
)
            '''
    )
    conn.commit()

# def Insertdata():
#     for row in final_out_put.itertuples():
#         conn.execute(
#             '''
# insert into Customer_Order_Wide
# values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
# ''',
# row
    
#     )
#         conn.commit()

