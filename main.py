# Specify what model is evaluated by changing which file is imported as 'model'
# Will run the main logic, i.e.
# 
# Create Dataframe
# Call model.predict(day, df) to get the production forecast
#   TODO: plus intraday adjustments
# Run the evaluation
# Write CSV with the relevant values

import wapi
import pandas as pd

if __name__ == '__main__':
    session = wapi.Session(config_file='apiconfig.ini')
    curve = session.get_curve(name='pro ch spv mwh/h cet min15 a')

    data = curve.get_data(data_from='2018-01-01', data_to=date_to, frequency="h", function="AVERAGE")
    pd_obj = data.to_pandas()
    pd_dataframe = pd.DataFrame({'timestamp':pd_obj.index, 'value':pd_obj.values})
    pd_dataframe['timestamp'] = pd.to_datetime(pd_dataframe.timestamp).dt.tz_localize(None)








# expect dates in the form "YYYY-MM-DD"
def main(date_from, date_to):
    


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