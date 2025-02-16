import pandas as pd

read_csv_path = '../raw-data-csvs/'
read_csv_name = 'activity.csv'
activity_df = pd.read_csv(read_csv_path + read_csv_name)

def get_first_last_dates(df):
    # Convert 'from_date' and 'to_date' to datetime if they're not already
    df['from_date'] = pd.to_datetime(df['from_date'])
    df['to_date'] = pd.to_datetime(df['to_date'])
    
    # Group by customer_id and subscription_id, then aggregate the first from_date and last to_date
    result_df = df.groupby(['customer_id', 'subscription_id'], as_index=False).agg(
        from_date=('from_date', 'min'),
        to_date=('to_date', 'max')
    )
    
    return result_df

def get_latest_subscription(df):
    # Convert 'from_date' and 'to_date' to datetime if they are not already
    df['from_date'] = pd.to_datetime(df['from_date'])
    df['to_date'] = pd.to_datetime(df['to_date'])
    
    # Sort by subscription_id to ensure we get the latest one
    df = df.sort_values('subscription_id', ascending=False)
    
    # Group by customer_id, from_date, and to_date and drop duplicates, keeping the latest subscription
    df = df.drop_duplicates(subset=['customer_id', 'from_date', 'to_date'], keep='first')
    
    return df

activity_clean_1_df = get_first_last_dates(activity_df)
activity_clean_2_df = get_latest_subscription(activity_clean_1_df)

write_csv_path = '../clean-data-csvs/'
write_csv_name = 'activity_clean.csv'

activity_clean_2_df.to_csv(write_csv_path + write_csv_name, index=False)