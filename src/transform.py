import pandas as pd
from datetime import datetime
from extract import customers,orders,new_customers
customers=customers.drop_duplicates()

# function for replacing null values
# def null_replacing():
#     customers['email']=customers.apply(lambda row:f"{row['name'].lower().strip().replace(' ','_')}@email.com" if pd.isnull(row['email']) else row['email'],axis=1)
#     customers['phone']=customers.apply(lambda row:"000-000-0000" if pd.isnull(row['phone']) else row['phone'],axis=1)
#     return customers

# #function for checking correct format, set as null if not in correct format
# def format_check():
#     customers['email_flag']=customers['email'].astype(str).str.match(r'^[a-zA-Z0-9_.]+@[a-z]+\.[a-z]{2,}$',na=False)
#     customers['phone_flag']=customers['phone'].astype(str).str.match(r'^\d{3}-\d{3}-\d{4}$',na=False)
#     return customers

# def corect_format_contact():
#     customers['email']=customers.apply(lambda row:f"{row['name'].lower().strip().replace(' ','_')}@email.com" if row['email_flag']==False else row['email'],axis=1)
#     customers['phone']=customers.apply(lambda row:"000-000-0000" if row['phone_flag']==False else row['phone'],axis=1)
#     return customers

def correct_contact_format():
    customers['email_flag']=customers['email'].astype(str).str.match(r'^[a-zA-Z0-9_.]+@[a-z]+\.[a-z]{2,}$',na=False)
    customers['phone_flag']=customers['phone'].astype(str).str.match(r'^\d{3}-\d{3}-\d{4}$',na=False)
    customers['email']=customers.apply(lambda row:f"{row['name'].lower().strip().replace(' ','_')}@email.com" if (row['email_flag']==False) or (pd.isnull(row['email'])) else row['email'],axis=1)
    customers['phone']=customers.apply(lambda row:"000-000-0000" if (row['phone_flag']==False) or (pd.isnull(row['phone'])) else row['phone'],axis=1)
    customers.drop(columns=['phone_flag','email_flag'],inplace=True)
    return customers

def correct_format():
    customers['registration_date']=pd.to_datetime(customers['registration_date'],format='mixed',errors='coerce')
    return customers


#scd type-1
def scd_type_1():
    df_type_1 =customers.set_index('customer_id').combine_first(new_customers.set_index('customer_id')).reset_index()
    return df_type_1


#scd type-2
def scd_type_2():
    # Add SCD2 columns to existing customers table to track changes
    customers['start_date'] = pd.to_datetime(customers['registration_date'])
    customers['end_date'] = None
    customers['is_current'] = True

    # Simulate a change
    df_new = new_customers[new_customers['customer_id'].isin(customers['customer_id'])]

    # Close old records
    today = pd.to_datetime(datetime.today().date())
    customers.loc[customers['customer_id'].isin(df_new['customer_id']), 'end_date'] = today
    customers.loc[customers['customer_id'].isin(df_new['customer_id']), 'is_current'] = False

    # Add new records
    df_new['start_date'] = today
    df_new['end_date'] = None
    df_new['is_current'] = True

    df_type2 = pd.concat([customers, df_new], ignore_index=True)
    return df_type2

#scd type-3
def scd_type_3():
    if 'prev_loyalty_status' not in customers.columns:
        customers['prev_loyalty_status'] = None
    for idx, row in new_customers.iterrows():
        cid = row['customer_id']
    # Check if the customer exists in the base DataFrame
    if cid in customers['customer_id'].values:
        # Get current and new loyalty status
        current_status = customers.loc[customers['customer_id'] == cid, 'loyalty_status'].values[0]
        new_status = row['loyalty_status']
        # If status changed, update columns accordingly
        if current_status != new_status:
            # Store current as previous, update with new status
            customers.loc[customers['customer_id'] == cid, 'prev_loyalty_status'] = current_status
            customers.loc[customers['customer_id'] == cid, 'loyalty_status'] = new_status
    return customers

def final_out_put():
    unified_customer_view=customers.merge(orders,on='customer_id',how='left')
    return unified_customer_view
final_out_put=final_out_put()


