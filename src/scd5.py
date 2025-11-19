import pandas as pd
import numpy as np
from datetime import datetime
from extract import *
def scd_type_5(customers_df, new_customers_df):
    """
    SCD Type 5: Hybrid (Type 1 + Type 2 + Reference Key) pattern.
    The Reference_SKey points all versions of a customer to the active row's SKey.
    """

    customers_dim = customers_df.copy()

    # 1. Prepare/Initialize SCD columns (assuming base data requires initialization)
    if 'Customer_SKey' not in customers_dim.columns:
        customers_dim['Customer_SKey'] = np.arange(1001, 1001 + len(customers_dim))
        customers_dim['is_current'] = True
        customers_dim['start_date'] = customers_dim['registration_date'].apply(lambda x: x.date() if pd.notna(x) else datetime(2023, 1, 1).date())
        customers_dim['end_date'] = None
    if 'Reference_SKey' not in customers_dim.columns:
        customers_dim['Reference_SKey'] = customers_dim['Customer_SKey']

    active_customers = customers_dim[customers_dim['is_current'] == True].copy()
    
    new_records_list = []
    today = datetime.today().date()
    max_skey = customers_dim['Customer_SKey'].max()
    
    for _, new_row in new_customers_df.iterrows():
        cust_id = new_row['customer_id']
        old_row = active_customers[active_customers['customer_id'] == cust_id]
        
        if not old_row.empty:
            old_skey = old_row['Customer_SKey'].values[0]
            
            # Check 'address' as the SCD Type 2 (History-tracked) attribute trigger
            old_address = old_row['address'].values[0]
            new_address = new_row['address']
            
            if old_address != new_address:
                # --- Type 2 Change Triggered ---
                
                # a. Expire old row (update in the main dataframe)
                customers_dim.loc[customers_dim['Customer_SKey'] == old_skey, 'is_current'] = False
                customers_dim.loc[customers_dim['Customer_SKey'] == old_skey, 'end_date'] = today

                # b. Create new row
                new_skey = max_skey + 1
                max_skey = new_skey
                
                new_version = new_row.to_dict()
                new_version.update({
                    'Customer_SKey': new_skey,
                    'is_current': True,
                    'start_date': today,
                    'end_date': None,
                    'Reference_SKey': new_skey 
                })
                new_records_list.append(pd.DataFrame([new_version]))
                
                # c. SCD Type 5 Link Update: ALL records for this customer must point to the new active SKey.
                customers_dim.loc[customers_dim['customer_id'] == cust_id, 'Reference_SKey'] = new_skey
            
            else:
                # No Type 2 change, apply Type 1 attributes (email/phone) to the current row
                customers_dim.loc[customers_dim['Customer_SKey'] == old_skey, 'email'] = new_row['email']
                customers_dim.loc[customers_dim['Customer_SKey'] == old_skey, 'phone'] = new_row['phone']

    # Handle brand new customers
    new_only_df = new_customers_df[~new_customers_df['customer_id'].isin(customers_dim['customer_id'])].copy()
    if not new_only_df.empty:
        start_skey = customers_dim['Customer_SKey'].max() + 1
        new_only_df['Customer_SKey'] = np.arange(start_skey, start_skey + len(new_only_df))
        new_only_df['is_current'] = True
        new_only_df['start_date'] = datetime.today().date()
        new_only_df['end_date'] = None
        new_only_df['Reference_SKey'] = new_only_df['Customer_SKey']
        new_records_list.append(new_only_df)
        
    # Concatenate the main table with any new records
    customers_dim = pd.concat([customers_dim] + new_records_list, ignore_index=True)

    # Final cleanup and return
    cols_to_keep = [c for c in customers_dim.columns if c in customers_df.columns.tolist() or c in ['Customer_SKey', 'is_current', 'start_date', 'end_date', 'Reference_SKey']]
    return customers_dim[cols_to_keep].sort_values(by=['customer_id', 'start_date']).reset_index(drop=True)