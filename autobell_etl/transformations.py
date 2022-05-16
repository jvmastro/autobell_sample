from datetime import datetime
import logging
import pandas as pd


class AutoBellTranformations:
    
    def __init__(self, car_wash_data,
                 test_period_start_date: str,
                 test_period_end_date: str,
                 test_period_start_meter_cubic: int,
                 test_period_end_meter_cubic: int,
                 test_period_cars: int, 
                 conversion_factor = None):
        self._logger = logging.getLogger(__name__)
        self.car_wash_data = car_wash_data
        self.test_period_start_date = test_period_start_date
        self.test_period_end_date = test_period_end_date
        self.test_period_start_meter_cubic = test_period_start_meter_cubic
        self.test_period_end_meter_cubic = test_period_end_meter_cubic
        self.test_period_cars = test_period_cars
        self.conversion_factor = conversion_factor


    
    def extract(self):
        
        with open(self.car_wash_data, 'r') as file:
            source_car_wash_data_df = pd.read_csv(file, index_col=False, parse_dates=['start_period', 'end_period'])
        
        
        return source_car_wash_data_df
    


        
        
    
    
    
    