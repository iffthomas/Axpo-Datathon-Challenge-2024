# Will predict the network capacity to stay the same as it is currently

import pandas as pd
from datetime import timedelta

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_day(date, df):
    day_after = date + timedelta(days=1)

    mask = (df['timestamp'] >= date) & (df['timestamp'] < day_after)
    return df.loc[mask]

def predict_spot(tomorrow, df):
    allowed_capacities = df.loc[df['timestamp'] < tomorrow - timedelta(hours=12)]

    capacity_yesterday = get_day(tomorrow - timedelta(days=2), df)
    prediction = capacity_yesterday.copy()
    prediction['predicted_C'] = allowed_capacities['installed_pv_capacity'].iloc[-1]
    prediction["timestamp"] = capacity_yesterday.timestamp + pd.Timedelta(days=2)
    return prediction[['predicted_C', 'timestamp']]
