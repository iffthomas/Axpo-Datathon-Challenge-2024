# Will sell as much as was produced the last day

import pandas as pd
from datetime import datetime, timedelta
import capacity_model as C_predictor

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_day(date, df):
    day_after = date + timedelta(days=1)

    mask = (df['timestamp'] >= date) & (df['timestamp'] < day_after)
    return df.loc[mask]

def predict_spot(tomorrow, df):
    production_yesterday = get_day(tomorrow - timedelta(days=2), df)
    prediction = production_yesterday.copy()
    prediction.rename(columns={'actual_pv_production': 'E_sold_spot'}, inplace=True)
    prediction["timestamp"] = production_yesterday.timestamp + pd.Timedelta(days=2)
    return prediction

# expect dates in the form "YYYY-MM-DD"
def predict(date_from, date_to, df):
    date_format = "%Y-%m-%d"
    start_eval_period = datetime.strptime(date_from, date_format)
    end_eval_period = datetime.strptime(date_to, date_format) + timedelta(days=1)
    
    df = df[['timestamp', 'actual_pv_production', 'installed_pv_capacity']]
    df = df[df['timestamp'] > start_eval_period - timedelta(days=3)]

    predictions = []
    for date in daterange(start_eval_period, end_eval_period):

        # Spot Market prediction
        prediction = predict_spot(date, df)

        scaling = C_predictor.predict_spot(date, df)
        prediction['E_sold_spot'] = prediction['E_sold_spot'].div(scaling['predicted_C'])
        prediction['E_sold_spot'] = prediction['E_sold_spot'] * 10

        # prediction = (10/C) * prediction
        predictions.append(prediction[['timestamp', 'E_sold_spot']])

    res = pd.concat(predictions)
    res['E_sold_intraday'] = 0

    return res