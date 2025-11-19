import pandas as pd
from datetime import datetime
from extract import *

def scd_type_4(customers_df, new_customers_df):
    history_cols = customers_df.columns.tolist() + ['ChangeDate']
    customer_history = pd.DataFrame(columns=history_cols)
    
    changed_ids = new_customers_df[new_customers_df['customer_id'].isin(customers_df['customer_id'])]['customer_id'].tolist()

    old_records = customers_df[customers_df['customer_id'].isin(changed_ids)].copy()
    
    if not old_records.empty:
        old_records['ChangeDate'] = datetime.today().date()
        
        history_cols_to_keep = [col for col in customer_history.columns if col in old_records.columns]
        old_records = old_records[history_cols_to_keep]
        customer_history = pd.concat([customer_history, old_records], ignore_index=True)
        
    customers_current = customers_df.set_index('customer_id').combine_first(new_customers_df.set_index('customer_id')).reset_index()
    
    new_only_df = new_customers_df[~new_customers_df['customer_id'].isin(customers_df['customer_id'])].copy()
    customers_current = pd.concat([customers_current, new_only_df], ignore_index=True)

    return customers_current.drop_duplicates(subset=['customer_id'], keep='last'), customer_history.dropna(axis=1, how='all')