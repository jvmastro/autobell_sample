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
    
    def transform(self, car_wash_data_df: pd.DataFrame):
        
        #Check if test period data has been captured
        car_wash_data_df = self._test_period_data_check(car_wash_data_df, self.test_period_start_date, 
                                                        self.test_period_end_date, self.test_period_start_meter_cubic, 
                                                        self.test_period_end_meter_cubic, self.test_period_cars)
        

        # Calculate days
        car_wash_data_df['days'] = (car_wash_data_df['start_date'] - car_wash_data_df['end_date']).days
        
        # Calculate Consumption in Cubic Feet
        car_wash_data_df['consumption_cubic'] = car_wash_data_df['start_meter_cubic'] - car_wash_data_df['end_meter_cubic']
        
        # Calculate meter reads in gallons
        car_wash_data_df['start_meter_gal'] = round(car_wash_data_df['start_meter_cubic'] * self.conversion_factor, 2)
        car_wash_data_df['end_meter_gal'] = round(car_wash_data_df['end_meter_cubic'] * self.conversion_factor, 2)
        
        # Calculate Water Consumption in Gallons
        car_wash_data_df['consumption_gal'] = round(car_wash_data_df['consumption_cubic'] * self.conversion_factor,2)
        
        
        
        
    def _test_period_data_check(self, car_wash_data_df: pd.DataFrame,
                test_period_start_date: str, test_period_end_date: str,
                test_period_start_meter_cubic: int, test_period_end_meter_cubic: int, test_period_cars: int):
        
        #Convert dates to correct date time format and calculate days
        test_period_start_datetime = datetime.strptime(test_period_start_date, '%m/%d/%y').strftime('%m/%d/%y')
        test_period_end_datetime = datetime.strptime(test_period_end_date, '%m/%d/%y').strftime('%m/%d/%y')
        days = (test_period_start_datetime - test_period_end_datetime).days
        
        #Check DF for occurance of test data
        test_period_date_check = car_wash_data_df[
            (car_wash_data_df['start_date'] == test_period_start_datetime) & 
            (car_wash_data_df['end_date'] == test_period_end_datetime)
            ]
        #Add data if it does nt exist
        if test_period_date_check.empty:
            test_period_data_df= pd.DataFrame([test_period_start_datetime,test_period_end_datetime, days, test_period_start_meter_cubic, test_period_end_meter_cubic, test_period_cars], columns=car_wash_data_df.columns.to_list())
            car_wash_data_df = pd.concat([car_wash_data_df, test_period_data_df], ignore_index=True)
            return car_wash_data_df
        else:
            return car_wash_data_df

        
        
    
    
    
    