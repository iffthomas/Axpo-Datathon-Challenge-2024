# Will sell as much as was produced the last day

import wapi
import pandas as pd
from datetime import datetime, timedelta

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_day(date, df):
    day_after = date + timedelta(days=1)

    mask = (df['timestamp'] >= date) & (df['timestamp'] < day_after)
    return df.loc[mask]

def predict(today, df):
    production_yesterday = get_day(today - timedelta(days=1), df)
    prediction = production_yesterday.copy()
    prediction["timestamp"] = production_yesterday.timestamp + pd.Timedelta(days=2)
    return prediction

# expect dates in the form "YYYY-MM-DD"
def main(date_from, date_to):
    session = wapi.Session(config_file='apiconfig.ini')
    curve = session.get_curve(name='pro ch spv mwh/h cet min15 a')

    data = curve.get_data(data_from='2018-01-01', data_to=date_to, frequency="h", function="AVERAGE")
    pd_obj = data.to_pandas()
    pd_dataframe = pd.DataFrame({'timestamp':pd_obj.index, 'value':pd_obj.values})
    pd_dataframe['timestamp'] = pd.to_datetime(pd_dataframe.timestamp).dt.tz_localize(None)


    # for day in range
    date_format = "%Y-%m-%d"
    start_eval_period = datetime.strptime(date_from, date_format)
    end_eval_period = datetime.strptime(date_to, date_format) + timedelta(days=1)

    predictions = []
    for date in daterange(start_eval_period, end_eval_period):
        # optional: compute dataframe with data up to 12:00 
        # potentially with mask

        # Spot Market prediction
        prediction = predict(date, pd_dataframe)
        predictions.append(prediction)

    return pd.concat(predictions)

if __name__ == '__main__':
    main('2018-01-01', '2018-01-05')