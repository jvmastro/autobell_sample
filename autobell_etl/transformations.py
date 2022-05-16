from cgi import test
from datetime import datetime
import logging
import pandas as pd
from pyparsing import col


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
        self.test_period_start_date = datetime.strptime(test_period_start_date, '%m/%d/%y').strftime('%m/%d/%y')
        self.test_period_end_date = datetime.strptime(test_period_end_date, '%m/%d/%y').strftime('%m/%d/%y')
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
        
        # Calculate Water Consumption in Gallons
        car_wash_data_df['gallons_car'] = round(car_wash_data_df['consumption_gal'] / car_wash_data_df['cars'],2)
        
        #Create Test Period DF
        test_period_df = car_wash_data_df[(car_wash_data_df['start_date'] == self.test_period_start_date) & (car_wash_data_df['end_date']==self.test_period_end_date)]
    
        
        #Create Pre-Install Period DF
        pre_install_df = car_wash_data_df[(car_wash_data_df['start_date'] < self.test_period_start_date) & (car_wash_data_df['end_date'] < self.test_period_end_date)]
        pre_install_days_mean = pre_install_df['days'].mean()
        pre_install_consumption_gal_mean = pre_install_df['consumption_gal'].mean()
        pre_install_cars_mean = pre_install_df['cars'].mean()
        pre_install_gallons_car_mean = pre_install_df['gallons_car'].mean()
        
        # Calculate Post-Install Comparison Stats
        
        cars_serviced_diff = pre_install_cars_mean - test_period_df['cars']
        cars_serviced_diff_percentage = (cars_serviced_diff/pre_install_cars_mean) * 100
        
        consumption_diff =  pre_install_consumption_gal_mean - test_period_df['consumption_gal']
        consumption_diff_percentage = (consumption_diff / pre_install_consumption_gal_mean) * 100
        
        gallons_car_diff =  pre_install_gallons_car_mean - test_period_df['gallons_car']
        gallons_car_diff_percentage = (gallons_car_diff / pre_install_gallons_car_mean) * 100
        
        columns = ['Test Period Days',
                   'Cars (Pre-Install)', 'Cars (Test Period)', 'Difference',
                   'Consumption (Pre-Install)', 'Consumption (Test Period)','Difference',
                   'Gallons/Car (Pre-Install)', 'Gallons/Car (Test Period)','Difference']
        
        comparison_df = pd.DataFrame([test_period_df['days'],
                                      pre_install_cars_mean, test_period_df['cars'], cars_serviced_diff_percentage,
                                      pre_install_consumption_gal_mean, test_period_df['consumption_gal'], gallons_car_diff_percentage,
                                      pre_install_gallons_car_mean, test_period_df['gallons_car'], gallons_car_diff_percentage],
                                     columns = columns)
        
        return test_period_df, pre_install_df, comparison_df
    
    def load():
        pass
        
        
    def _test_period_data_check(self, car_wash_data_df,
                                test_period_start_date: str,
                                test_period_end_date: str,
                                test_period_start_meter_cubic: int,
                                test_period_end_meter_cubic: int,
                                test_period_cars: int):
        
        #Check dataframe for occurance of test data
        test_period_date_check = car_wash_data_df[
            (car_wash_data_df['start_date'] == test_period_start_date) & 
            (car_wash_data_df['end_date'] == test_period_end_date)
            ]
        #Add data if it does not exist
        if test_period_date_check.empty:
            test_period_data_df= pd.DataFrame([test_period_start_date,test_period_end_date, test_period_start_meter_cubic, test_period_end_meter_cubic, test_period_cars], columns=car_wash_data_df.columns.to_list())
            car_wash_data_df = pd.concat([car_wash_data_df, test_period_data_df], ignore_index=True)
            return car_wash_data_df
        else:
            return car_wash_data_df
        
        
    def _savings_breakeven_report(self, annual_water_cost, savings_rate, fluidlytix_cost):
        annual_savings = annual_water_cost * savings_rate
        monthly_savings = round(annual_savings / 12, 2)
        savings_ten_year = round(annual_savings * 10, 2)
        breakeven_point_months = round(fluidlytix_cost / monthly_savings, 2)
        
        columns = ['Annual Water Bill','Savings Rate','Montly Savings','Annual Savings','10-Year Savings','Breakeven Point (Months)','Water Savings Solution']
        
        savings_breakeven_df = pd.DataFrame([annual_savings, savings_rate, monthly_savings, 
                                                annual_savings, savings_ten_year, breakeven_point_months, fluidlytix_cost],
                                            columns=columns)
        
        return savings_breakeven_df
    
    def _cash_flow_report(self, n, fluidlytix_cost, annual_savings):
        
        cost = -abs(fluidlytix_cost)
        
        if n == 0:
            return cost
        else:
            return cost + (n * annual_savings)
        

        
        
    
    
    
    