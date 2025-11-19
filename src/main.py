import pandas as pd
import numpy as np
import pyodbc
from extract import *
from transform import *
from scd4 import *
from scd5 import *
from load import *

print('Original customers Dataset:')
print('========================================================')
print(customers)
print('========================================================')
print('Original orders dataset:')
print('========================================================')
print(orders)
print('========================================================')
print("Applying cleaning transformations")
print('========================================================')
correct_contact_format()
correct_format()
print(customers)
print('========================================================')
print('SCD TYPE-1')
print('========================================================')
print(scd_type_1())
print('========================================================')
print('SCD TYPE-2')
print('========================================================')
print(scd_type_2())
print('========================================================')
print('SCD TYPE-3')
print('========================================================')
print(scd_type_3())
print('========================================================')
print('Unified view')
print('========================================================')
# print(final_out_put().columns)

print(scd_type_4(customers,new_customers))
print(scd_type_5(customers,new_customers))
# LoadData()
# connectDB()
# Insertdata()
