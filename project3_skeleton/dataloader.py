import yaml
import wapi
import pandas as pd

class Dataloader():
    def __init__(self, config_path, api_config_path):
        self.dl_config = self.load_config(config_path)['dataloader']
        
        self.api_config = self.load_config(api_config_path)
        self.client_id = self.api_config['id']
        self.client_secret = self.api_config['secret']
        

    def load_config(self, config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
        
    def load(self):
        time_series = {}
        start = self.dl_config['start']
        end = self.dl_config['end']
        for signal_name in self.dl_config['features']:
            time_series[signal_name] = self.load_ts(self.dl_config['features'][signal_name], start, end)
        
        ts_df = self.merge_ts(time_series)    
        #ts_df.dropna(inplace=True)
        return ts_df
    def load_ts(self, signal_name, start, end):
        session = wapi.Session(client_id=client_id, client_secret=client_secret)
        curve = session.get_curve(name=signal_name)
        ts = curve.get_data(data_from=start, data_to=end)
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
    
    