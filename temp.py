
import baseline as model
from dataloader import Dataloader
from evaluation import Evaluator
import os

dl_config_path = os.path.join('config', 'config.yml')
api_config_path = os.path.join('config', 'api_config.yml')
eval_config_path = os.path.join('config', 'evaluation_config.yml')
api_id = "yf24xCq95gJkorzBWb16GAP4eOxQNVZR"
api_secret = "sWkUzN3a8uC8QDu1V64bH3bAtCdduf74Xh8JnT_OrYZf_x2ObWdiNkOga59cB2C5Q9us9tW0bg4zFg4BfeOR7mRzui3ROZ1QHI0i"

def f(date_from, date_to, API):

    E_sold_spot = model.main(date_from, date_to, api_id, api_secret)

    
    print(E_sold_spot)
    return E_sold_spot



def b():
    e_sold = f('2019-01-01', '2019-02-01', "baum")

    return Evaluator(
        start= '2020-08-01',
        end = '2020-08-02', 
        evalution_config_path=eval_config_path, 
        api_config_path=api_config_path, 
        result_df=e_sold
        )