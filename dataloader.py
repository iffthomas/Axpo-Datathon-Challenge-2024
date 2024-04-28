import yaml
import wapi
import pandas as pd

class Dataloader():
    def __init__(self, config_path, start_date, end_date, api_id, api_secret):
        self.dl_config = self.load_config(config_path)['dataloader']
        
        self.client_id = api_id
        self.client_secret = api_secret
        self.start_date = start_date
        self.end_date = end_date

    def load_config(self, config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
        
    def load(self):
        time_series = {}
        start = self.start_date
        end = self.end_date

        session = wapi.Session(client_id=self.client_id, client_secret=self.client_secret, timeout=300)
        for signal_name in self.dl_config['features']:
            time_series[signal_name] = self.load_ts(session, self.dl_config['features'][signal_name], start, end)
            #print(time_series[signal_name])

        
        ts_df = self.merge_ts(time_series)    
        #ts_df.dropna(inplace=True)
        return ts_df
    
    def load_ts(self, session, signal_name, start, end):
        
        curve = session.get_curve(name=signal_name)
        ts = curve.get_data(data_from=start, data_to=end, frequency="h", function="AVERAGE")
        ts_df = ts.to_pandas().to_frame()
        ts_df['timestamp'] = ts_df.index
        ts_df['timestamp'] = ts_df['timestamp'].apply(lambda x: x.tz_localize(None))
        ts_df.rename(columns={signal_name: 'value'}, inplace=True)
        return ts_df
    
    def merge_ts(self, time_series):
        df_list = [df.rename(columns={'value': signal_name}) for signal_name, df in time_series.items()]
        ts_df = df_list[0]
        for df in df_list[1:]:
            ts_df = ts_df.merge(df, how='outer', on='timestamp')
        return ts_df
    
    