from datetime import datetime
import logging
import pandas as pd
import numpy as np

from typing import NamedTuple

class CommonTargetConfigs(NamedTuple):
    """
    Class for common configs shared between target dataframs/reports
    
    Params:
        
        trg_start_date_col (str): start date column name for target dataframe
        trg_end_date_col (str): end date column name for target  dataframe
        trg_start_meter_cubic_col (str): start cubic meter column name for target  dataframe
        trg_end_meter_cubic_col (str): end cubic meter column name for target  dataframe
        trg_cars_col (str): cars column name for target  dataframe
        trg_prelim_cols (list): list of columns names to build base target dataframe
        trg_days_col (str): days column name for target  dataframe
        trg_consumption_cubic_col (str): consumption cubic meter column name for target  dataframe
        trg_start_meter_gallon_col (str): start gallons meter column name for target  dataframe
        trg_end_meter_gallon_col (str): end gallons meter column name for target  dataframe
        trg_gallons_car_col (str): gallons per car column name for target  dataframe
        trg_conversion_factor (float): conversion factor for cubic meters to gallons provided by client
        trg_pre_install_annual_water_expense (float): annual water bill as determined during contracting
        trg_contract_savings_rate (float): pre-determined savings rate the contract was built upon
        trg_primary_fluidlytix_cost (float): primary price of fluidlytix project detailed in contrac
        trg_secondary_fluidlytix_cost (float): secondary fluidlytix cost if detailed in contract - can be None
    """
    
    trg_start_date_col: str
    trg_end_date_col: str
    trg_start_meter_cubic_col: str
    trg_end_meter_cubic_col: str
    trg_cars_col: str
    trg_prelim_cols: list
    trg_days_col: str
    trg_consumption_cubic_col: str
    trg_start_meter_gallon_col: str
    trg_end_meter_gallon_col: str
    trg_gallons_car_col: str
    trg_conversion_factor: float 
    trg_pre_install_annual_water_expense: float
    trg_contract_savings_rate: float
    trg_primary_fluidlytix_cost: float
    trg_secondary_fluidlytix_cost: float 

class TestPeriodConfigs(NamedTuple):
    """
    Class for test period reporting configs
    
    Params:
        test_period_start_date (str): start date of test period
        test_period_end_date (str): end date of test period
        test_period_start_meter_cubic (float): meter read at start of test period in cubic meters
        test_period_end_meter_cubic (float): meter read at end of test period in cubic meters
        test_period_cars (float): total number of cars 
    """
    test_period_start_date: str
    test_period_end_date: str
    test_period_start_meter_cubic: float
    test_period_end_meter_cubic: float
    test_period_cars: float


class DateFormatConfigs(NamedTuple):
    """Class for formating configs

    Params:
        src_date_format (str): format of source dates
        trg_date_format (str): format for target dataframes/reports

    """
    src_date_format: str
    trg_date_format: str
    
class ComparisonReportConfigs(NamedTuple):
    """Class for comaprison report configs

    Params:
        test_period_days_col (str): column name for test period days
        pre_install_cars_col (str): column name for pre-install mean cars
        test_period_cars_col (str): column name for test period cars
        percent_diff_cars_col (str): column name for percentage difference cars
        pre_install_consumption_col (str): column name for pre-install mean consumption
        test_period_consumption_col (str): column name for test period consumption
        percent_diff_consumption_col (str): column name for percentage difference consumption
        pre_install_gallons_car_col (str): column name for pre-install mean gallons per car
        test_period_gallons_car_col (str): column name for test period gallons per car
        percent_diff_gallons_car_col (str): column name for percentage difference gallons per car

    """
    test_period_days_col: str
    pre_install_cars_col: str
    test_period_cars_col: str
    percent_diff_cars_col: str
    pre_install_consumption_col: str
    test_period_consumption_col: str
    percent_diff_consumption_col: str
    pre_install_gallons_car_col: str
    test_period_gallons_car_col: str
    percent_diff_gallons_car_col: str

class SavingsBreakevenReportConfigs(NamedTuple):
    """Class for savings + breakeven report configs

    Params:
        annual_water_bill_col (str): column name for annual water bill
        savings_rate_col (str): column name for savings rate
        monthly_savings_col (str): column name for monthly savings
        annual_savings_col (str): column name for annual savings
        ten_year_savings_col (str): column name for 10 year savings
        reakeven_point_col (str): column name for breakeven point in months
        fluidlytix_cost_col (str): column name for fluidlytix project cost 

    """
    annual_water_bill_col: str
    savings_rate_col: str
    monthly_savings_col: str
    annual_savings_col: str
    ten_year_savings_col: str
    reakeven_point_col: str
    fluidlytix_cost_col: str

        
    

class AutoBellETL:
    
    
    def __init__(self, car_wash_data,
                 test_period_start_period: str,
                 test_period_end_period: str,
                 test_period_start_meter_cubic: int,
                 test_period_end_meter_cubic: int,
                 test_period_cars: int, 
                 conversion_factor = None):
        self._logger = logging.getLogger('AutoBellETL')
        self.car_wash_data = car_wash_data
        self.test_period_start_period = datetime.strptime(test_period_start_period, '%m/%d/%Y').strftime('%Y-%m-%d')
        self.test_period_end_period = datetime.strptime(test_period_end_period, '%m/%d/%Y').strftime('%Y-%m-%d')
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
        car_wash_data_df = self._test_period_data_check(car_wash_data_df, self.test_period_start_period, 
                                                        self.test_period_end_period, self.test_period_start_meter_cubic, 
                                                        self.test_period_end_meter_cubic, self.test_period_cars)

        # Calculate days
        car_wash_data_df['days'] = (car_wash_data_df['end_period'] - car_wash_data_df['start_period']).dt.days
        
        # Calculate Consumption in Cubic Feet
        car_wash_data_df['consumption_cubic'] = car_wash_data_df['end_meter_cubic'] - car_wash_data_df['start_meter_cubic']
        
        # Calculate meter reads in gallons
        car_wash_data_df['start_meter_gal'] = round(car_wash_data_df['start_meter_cubic'] * self.conversion_factor, 2)
        car_wash_data_df['end_meter_gal'] = round(car_wash_data_df['end_meter_cubic'] * self.conversion_factor, 2)
        
        # Calculate Water Consumption in Gallons
        car_wash_data_df['consumption_gal'] = round((car_wash_data_df['end_meter_gal'] - car_wash_data_df['start_meter_gal']), 2)
        
        # Calculate Water Consumption in Gallons
        car_wash_data_df['gallons_car'] = round(car_wash_data_df['consumption_gal'] / car_wash_data_df['cars'],2)
        
        #Create Test Period data
        test_period_df = car_wash_data_df[(car_wash_data_df['start_period'] == self.test_period_start_period) & (car_wash_data_df['end_period']==self.test_period_end_period)].reset_index(drop=True)
    
        
        #Create Pre-Install Period DF
        pre_install_df = car_wash_data_df[(car_wash_data_df['start_period'] < self.test_period_start_period) & (car_wash_data_df['end_period'] < self.test_period_end_period)].reset_index(drop=True)
        
        pre_install_days_mean = pre_install_df['days'].mean()
        pre_install_consumption_gal_mean = round(pre_install_df['consumption_gal'].mean(),2)
        pre_install_cars_mean = round(pre_install_df['cars'].mean(),2)
        pre_install_gallons_car_mean = round(pre_install_df['gallons_car'].mean(),2)
        
        # Calculate Post-Install Comparison Stats
        cars_serviced_diff =  test_period_df['cars'] - pre_install_cars_mean
        cars_serviced_diff_percentage = round((cars_serviced_diff / pre_install_cars_mean) * 100, 2)
        
        consumption_diff =  test_period_df['consumption_gal'] - pre_install_consumption_gal_mean 
        consumption_diff_percentage = round((consumption_diff / pre_install_consumption_gal_mean) * 100, 2)
        
        gallons_car_diff = test_period_df['gallons_car'] - pre_install_gallons_car_mean
        gallons_car_diff_percentage = round((gallons_car_diff / pre_install_gallons_car_mean) * 100,2)
        
        # Create comaprison dataframe
        comparison_df = pd.DataFrame({'Test Period Days': test_period_df['days'],
                                      'Cars (Pre-Install)': pre_install_cars_mean,
                                      'Cars (Test Period)': test_period_df['cars'],
                                      'Percentage Difference (Cars)': cars_serviced_diff_percentage,
                                      'Consumption (Pre-Install)': pre_install_consumption_gal_mean,
                                      'Consumption (Test Period)': test_period_df['consumption_gal'],
                                      'Percentage Difference (Consumption)': consumption_diff_percentage,
                                      'Gallons/Car (Pre-Install)': pre_install_gallons_car_mean,
                                      'Gallons/Car (Test Period)': test_period_df['gallons_car'],
                                      'Percentage Difference (Gallons/Car)': gallons_car_diff_percentage})
        
        # Create savings + breakeven report
        annual_savings = annual_water_cost * (savings_rate/ 100)
        monthly_savings = round(annual_savings / 12, 2)
        savings_ten_year = round(annual_savings * 10, 2)
        breakeven_point_months = round(fluidlytix_cost / monthly_savings, 2)

        savings_breakeven_df = pd.DataFrame({'Annual Water Bill': annual_water_cost,
                                      'Savings Rate': savings_rate,
                                      'Montly Savings': monthly_savings,
                                      'Annual Savings': annual_savings,
                                      '10-Year Savings': savings_ten_year,
                                      'Breakeven Point (Months)': breakeven_point_months,
                                      'Water Savings Solution': fluidlytix_cost})
        
        
        
        return test_period_df, pre_install_df, comparison_df
    
    def load():
        pass
        
        
    def _test_period_data_check(self, car_wash_data_df,
                                test_period_start_period: str,
                                test_period_end_period: str,
                                test_period_start_meter_cubic: int,
                                test_period_end_meter_cubic: int,
                                test_period_cars: int):
        
        #Check dataframe for occurance of test data
        test_period_date_check = car_wash_data_df[
            (car_wash_data_df['start_period'] == test_period_start_period) & 
            (car_wash_data_df['end_period'] == test_period_end_period)
            ]
        #Add data if it does not exist
        if test_period_date_check.empty:
            self._logger.info('Test Period data is not included in the dataframe...')
            test_period_data_df= pd.DataFrame(np.array([[test_period_start_period, test_period_end_period,
                                               test_period_start_meter_cubic, test_period_end_meter_cubic,
                                               test_period_cars]]), columns=['start_period','end_period', 'start_meter_cubic', 'end_meter_cubic','cars'])
            test_period_data_df['start_period']=pd.to_datetime(test_period_data_df.start_period)
            test_period_data_df['end_period']=pd.to_datetime(test_period_data_df.end_period)
            test_period_data_df['start_meter_cubic']=test_period_data_df['start_meter_cubic'].astype('float64')
            test_period_data_df['end_meter_cubic']=test_period_data_df['end_meter_cubic'].astype('float64')
            test_period_data_df['cars']=test_period_data_df['cars'].astype('float64')
            car_wash_data_df = pd.concat([car_wash_data_df, test_period_data_df], ignore_index=True)
            return car_wash_data_df
        else:
            self._logger.info('Test Period data is included in the dataframe...')
            return car_wash_data_df
        
        
    def _savings_breakeven_report(self, annual_water_cost, savings_rate, fluidlytix_cost):
        annual_savings = annual_water_cost * (savings_rate/ 100)
        monthly_savings = round(annual_savings / 12, 2)
        savings_ten_year = round(annual_savings * 10, 2)
        breakeven_point_months = round(fluidlytix_cost / monthly_savings, 2)

        savings_breakeven_df = pd.DataFrame({'Annual Water Bill': annual_water_cost,
                                      'Savings Rate': savings_rate,
                                      'Montly Savings': monthly_savings,
                                      'Annual Savings': annual_savings,
                                      '10-Year Savings': savings_ten_year,
                                      'Breakeven Point (Months)': breakeven_point_months,
                                      'Water Savings Solution': fluidlytix_cost})
        
        return savings_breakeven_df
    
    def _cash_flow_sequence(self,n, fluidlytix_cost, annual_savings):
        
        cost = -abs(fluidlytix_cost)
        
        if n == 0:
            return cost
        else:
            return round(cost + (n * annual_savings),2)
        
import subprocess
result = subprocess.run(["heroku", "config:get","DATABASE_URL"], stdout=subprocess.PIPE)
result = result.stdout.decode('utf-8')
result = str.replace(result, 'postgres','postgresql')
print(result)