import pandas as pd
from dataloader import Dataloader
import yaml

class Evaluator():
    
    def __init__(self, start, end, evalution_config_path, api_config_path, result_df=None, csv_path=None):
        self.result_df = pd.read_csv(csv_path) if csv_path else result_df
        self.add_start_end(evalution_config_path, start, end)
        self.time_series = self.load_time_series(evalution_config_path, api_config_path)

    def load_time_series(self, evalution_config_path, api_config_path):
        dl = Dataloader(evalution_config_path, api_config_path)
        time_series = dl.load()
        time_series.fillna(0.0, inplace=True)
        mask = time_series['timestamp'].apply(lambda row:  True if row.minute == 0  else False)
        time_series = time_series[mask]
        time_series.reset_index(drop=True, inplace=True)
        return time_series
    def add_start_end(self, evalution_config_path, start, end):
        with open(evalution_config_path, 'r') as f:
            eval_config = yaml.safe_load(f)
        eval_config['dataloader']['start'] = start
        eval_config['dataloader']['end'] = end
        with open(evalution_config_path, 'w') as f:
            yaml.dump(eval_config, f)
            
    def evaluate(self):
        self.evaluation_df = pd.DataFrame()
        
        self.evaluation_df["imbalance"], self.evaluation_df['actual_pv_production'] = self.compute_imbalance()
        self.evaluation_df["pnl_spot"] = self.compute_pnl_spot()
        self.evaluation_df["pnl_id"] = self.compute_pnl_id()
        self.evaluation_df["pnl_imbalance"] = self.compute_pnl_imbalance()
        self.evaluation_df['pnl'] = self.evaluation_df.apply(lambda row: row.pnl_spot + row.pnl_id + row.pnl_imbalance, axis=1)
        result = pd.concat([self.result_df, self.evaluation_df], axis=1)
        return result
    def compute_imbalance(self):
        imbalance_df = pd.DataFrame(self.time_series[['actual_pv_production']])
        imbalance_df['installed_pv_capacity'] = self.time_series['installed_pv_capacity']
        imbalance_df['our_actual_pv_production'] = imbalance_df.apply(lambda row: row.actual_pv_production * 10.0/row.installed_pv_capacity, axis=1)
        imbalance_df['E_sold_spot'] = self.result_df['E_sold_spot']
        imbalance_df['E_sold_intraday'] = self.result_df['E_sold_intraday']
        imbalance = imbalance_df.apply(lambda row: row.our_actual_pv_production - row.E_sold_spot - row.E_sold_intraday, axis=1)
        return imbalance, imbalance_df['our_actual_pv_production']
        
    def compute_pnl_spot(self):
        pnl_spot_df = pd.DataFrame(self.time_series[['spot_price']])
        pnl_spot_df['E_sold_spot'] = self.result_df['E_sold_spot']
        pnl_spot = pnl_spot_df.apply(lambda row: row.spot_price * row.E_sold_spot, axis=1)
        return pnl_spot
    
    def compute_pnl_id(self):
        pnl_id_df = pd.DataFrame(self.time_series[['intraday_price']])
        pnl_id_df['E_sold_intraday'] = self.result_df['E_sold_intraday']
        pnl_id = pnl_id_df.apply(lambda row: row.intraday_price * row.E_sold_intraday, axis=1)
        return pnl_id
    
    def compute_pnl_imbalance(self):
        pnl_imbalance_df = pd.DataFrame(self.evaluation_df['imbalance'])
        pnl_imbalance_df['spot_price'] = self.time_series[['spot_price']]
        pnl_imbalance = pnl_imbalance_df.apply(lambda row: row.imblance * row.spot_price * 1.3 if row.imbalance > 0 else row.imbalance * row.spot_price * 0.7, axis=1)
        return pnl_imbalance
    
