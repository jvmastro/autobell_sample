from datetime import datetime, date
import logging
import pandas as pd
import numpy as np

from typing import NamedTuple
from etl.db_conn import HerokuDBConnection


class CommonTargetConfigs(NamedTuple):
    """
    Class for common configs shared between target dataframs/reports
    
    Params:
        car_wash_csv (str): file path for csv of car wash meter data
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
        trg_consumption_gallon_col (str) column name for consumption in gallons for target datframe
        trg_gallons_car_col (str): gallons per car column name for target  dataframe
        trg_conversion_factor (float): conversion factor for cubic meters to gallons provided by client
        trg_pre_install_annual_water_expense (float): annual water bill as determined during contracting
        trg_contract_savings_rate (float): pre-determined savings rate the contract was built upon
        trg_primary_fluidlytix_cost (float): primary price of fluidlytix project detailed in contrac
        trg_secondary_fluidlytix_cost (float): secondary fluidlytix cost if detailed in contract - can be None
    """
    car_wash_csv: str
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
    trg_consumption_gallon_col: str 
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
        report_date_col (str): column naem for report date 
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
    report_date_col: str
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
        report_date_col (str): column naem for report date 
        annual_water_bill_col (str): column name for annual water bill
        savings_rate_col (str): column name for savings rate
        monthly_savings_col (str): column name for monthly savings
        annual_savings_col (str): column name for annual savings
        ten_year_savings_col (str): column name for 10 year savings
        reakeven_point_col (str): column name for breakeven point in months
        fluidlytix_cost_col (str): column name for fluidlytix project cost 

    """
    report_date_col: str
    annual_water_bill_col: str 
    savings_rate_col: str 
    monthly_savings_col: str 
    annual_savings_col: str 
    ten_year_savings_col: str 
    breakeven_point_col: str 
    fluidlytix_cost_col: str 

class CashFlowReportConfigs(NamedTuple):
    """Class for cash flow report configs

    Params:
        report_date_col (str): column naem for report date 
        year_col (str): column  name for year
        cash_flow_col (str): column name for cash flow
        year_col_values (list): list of values for year col

    """
    report_date_col: str
    year_col: str 
    cash_flow_col: str 
    year_col_values: list 
    

class AutoBellETL:
    
    def __init__(self,
                 common_trg_configs: CommonTargetConfigs,
                 test_period_configs: TestPeriodConfigs,
                 date_format_configs: DateFormatConfigs,
                 comparison_report_configs: ComparisonReportConfigs,
                 savings_bep_configs: SavingsBreakevenReportConfigs,
                 cash_flow_configs: CashFlowReportConfigs):
        self._logger = logging.getLogger('AutoBellETL')
        self.common_trg_configs = common_trg_configs
        self.test_period_configs = test_period_configs
        self.date_format_configs = date_format_configs
        self.comparison_report_configs = comparison_report_configs
        self.savings_bep_configs = savings_bep_configs
        self.cash_flow_configs = cash_flow_configs


    def extract(self):
        
        with open(self.common_trg_configs.car_wash_csv, 'r') as file:
            src_car_wash_data_df = pd.read_csv(file,
                                                  index_col=False,
                                                  parse_dates=
                                                  [
                                                      self.common_trg_configs.trg_start_date_col,
                                                      self.common_trg_configs.trg_end_date_col
                                                      ]
                                                  )
        
        
        return src_car_wash_data_df
    
    def transform(self, src_car_wash_data_df: pd.DataFrame):
        
        #Convert test_period dates strings to datetime
        test_period_start_date = datetime.strptime(self.test_period_configs.test_period_start_date, self.date_format_configs.src_date_format).strftime(self.date_format_configs.trg_date_format)
        test_period_end_date = datetime.strptime(self.test_period_configs.test_period_end_date, self.date_format_configs.src_date_format).strftime(self.date_format_configs.trg_date_format)
        
        #Check if test period data has been captured
        trg_car_wash_data_df = self._test_period_data_check(src_car_wash_data_df,
                                                        self.test_period_configs.test_period_start_date, 
                                                        self.test_period_configs.test_period_end_date,
                                                        self.test_period_configs.test_period_start_meter_cubic, 
                                                        self.test_period_configs.test_period_end_meter_cubic,
                                                        self.test_period_configs.test_period_cars)

        # Create days column for all data df
        trg_car_wash_data_df[self.common_trg_configs.trg_days_col] = (trg_car_wash_data_df[self.common_trg_configs.trg_end_date_col] - trg_car_wash_data_df[self.common_trg_configs.trg_start_date_col]).dt.days
        
        # Create consumption (cubic feet) column for all data df
        trg_car_wash_data_df[self.common_trg_configs.trg_consumption_cubic_col] = trg_car_wash_data_df[self.common_trg_configs.trg_end_meter_cubic_col] - trg_car_wash_data_df[self.common_trg_configs.trg_start_meter_cubic_col]
        
        # Create start and end meter read (gallons) columns for all data df
        trg_car_wash_data_df[self.common_trg_configs.trg_start_meter_gallon_col] = round(trg_car_wash_data_df[self.common_trg_configs.trg_start_meter_cubic_col] * self.common_trg_configs.trg_conversion_factor, 2)
        trg_car_wash_data_df[self.common_trg_configs.trg_end_meter_gallon_col] = round(trg_car_wash_data_df[self.common_trg_configs.trg_end_meter_cubic_col] * self.common_trg_configs.trg_conversion_factor, 2)
        
        # Create consumption (gallons) column for all data df
        trg_car_wash_data_df[self.common_trg_configs.trg_consumption_gallon_col] = round((trg_car_wash_data_df[self.common_trg_configs.trg_end_meter_gallon_col] - trg_car_wash_data_df[self.common_trg_configs.trg_start_meter_gallon_col]), 2)
        
        # Create gallons per car column for all data df
        trg_car_wash_data_df[self.common_trg_configs.trg_gallons_car_col] = round(trg_car_wash_data_df[self.common_trg_configs.trg_consumption_gallon_col] / trg_car_wash_data_df[self.common_trg_configs.trg_cars_col], 2)
        
        #Create Test Period Report
        test_period_report = trg_car_wash_data_df[(trg_car_wash_data_df[self.common_trg_configs.trg_start_date_col] == test_period_start_date) & (trg_car_wash_data_df[self.common_trg_configs.trg_end_date_col] == test_period_end_date)].reset_index(drop=True)
    
        
        #Create Pre-Install Report
        
        pre_install_report = trg_car_wash_data_df[(trg_car_wash_data_df[self.common_trg_configs.trg_start_date_col] < test_period_start_date) & (trg_car_wash_data_df[self.common_trg_configs.trg_end_date_col] < test_period_end_date)].reset_index(drop=True)
        
        ## Calculate pre-install stats
        pre_install_days_mean = pre_install_report[self.common_trg_configs.trg_days_col].mean() # save for later use
        pre_install_consumption_gal_mean = round(pre_install_report[self.common_trg_configs.trg_consumption_gallon_col].mean(),2)
        pre_install_cars_mean = round(pre_install_report[self.common_trg_configs.trg_cars_col].mean(),2)
        pre_install_gallons_car_mean = round(pre_install_report[self.common_trg_configs.trg_gallons_car_col].mean(),2)
        
        # Calculate Post-Install Comparison Stats
        cars_serviced_diff =  test_period_report[self.common_trg_configs.trg_cars_col][0] - pre_install_cars_mean
        #cars_serviced_diff_percentage = round((cars_serviced_diff / pre_install_cars_mean) * 10.000, 2)
        
        consumption_diff =  test_period_report[self.common_trg_configs.trg_consumption_gallon_col][0] - pre_install_consumption_gal_mean 
        #consumption_diff_percentage = round((consumption_diff / pre_install_consumption_gal_mean) * 100.00, 2)
        
        gallons_car_diff = test_period_report[self.common_trg_configs.trg_gallons_car_col][0] - pre_install_gallons_car_mean
        gallons_car_diff_percentage = round((gallons_car_diff / pre_install_gallons_car_mean) * 100.00,2)
    
        
        # Create comaprison dataframe
        comparison_report = pd.DataFrame(
            {
                self.comparison_report_configs.report_date_col: date.today(),
                self.comparison_report_configs.test_period_days_col: test_period_report[self.common_trg_configs.trg_days_col],
                self.comparison_report_configs.pre_install_cars_col: pre_install_cars_mean,
                self.comparison_report_configs.test_period_cars_col: test_period_report[self.common_trg_configs.trg_cars_col],
                self.comparison_report_configs.percent_diff_cars_col: cars_serviced_diff,
                self.comparison_report_configs.pre_install_consumption_col: pre_install_consumption_gal_mean,
                self.comparison_report_configs.test_period_consumption_col: test_period_report[self.common_trg_configs.trg_consumption_gallon_col],
                self.comparison_report_configs.percent_diff_consumption_col: consumption_diff,
                self.comparison_report_configs.pre_install_gallons_car_col: pre_install_gallons_car_mean,
                self.comparison_report_configs.test_period_gallons_car_col: test_period_report[self.common_trg_configs.trg_gallons_car_col],
                self.comparison_report_configs.percent_diff_gallons_car_col: gallons_car_diff_percentage})
        
        # Create standard savings + breakeven reports for contractual savings rate and test period savings rate
        savings_bep_report_contract_rate = self._savings_breakeven_report(self.common_trg_configs.trg_pre_install_annual_water_expense,
                                                                  self.common_trg_configs.trg_contract_savings_rate,
                                                                  self.common_trg_configs.trg_primary_fluidlytix_cost) 
        
        
        savings_bep_report_test_rate = self._savings_breakeven_report(self.common_trg_configs.trg_pre_install_annual_water_expense,
                                                                  abs(gallons_car_diff_percentage),
                                                                  self.common_trg_configs.trg_primary_fluidlytix_cost)
        
        # Create 10 year cash flow report for contractual rate & test period rate
        cash_flow_year_col_vals_contract_rate = self.cash_flow_configs.year_col_values
        cash_flow_inflow_values_contract_rate = [self._cash_flow_sequence(i,
                                                                  self.common_trg_configs.trg_pre_install_annual_water_expense,
                                                                  self.common_trg_configs.trg_contract_savings_rate,
                                                                  self.common_trg_configs.trg_primary_fluidlytix_cost) for i in range(0,11)]
        
        cash_flow_report_data_contract_rate = [[date.today(),cash_flow_year_col_vals_contract_rate[i], cash_flow_inflow_values_contract_rate[i]] for i in range(0,11)]
        cash_flow_report_contract_rate = pd.DataFrame(data=cash_flow_report_data_contract_rate,
                                                      columns = [self.cash_flow_configs.report_date_col,self.cash_flow_configs.year_col, self.cash_flow_configs.cash_flow_col])
        
        cash_flow_year_col_vals_test_rate = self.cash_flow_configs.year_col_values
        cash_flow_inflow_values_test_rate = [self._cash_flow_sequence(i,
                                                                  self.common_trg_configs.trg_pre_install_annual_water_expense,
                                                                  abs(gallons_car_diff_percentage),
                                                                  self.common_trg_configs.trg_primary_fluidlytix_cost
                                                                  ) for i in range(0,11)]
        
        cash_flow_report_data_test_rate = [[date.today(), cash_flow_year_col_vals_test_rate[i], cash_flow_inflow_values_test_rate[i]] for i in range(0,11)]
        cash_flow_report_test_rate = pd.DataFrame(data=cash_flow_report_data_test_rate,
                                                      columns = [self.cash_flow_configs.report_date_col,self.cash_flow_configs.year_col, self.cash_flow_configs.cash_flow_col])

        
        # Create additonal savings + bep report & cash flow report if secondary cost
        if self.common_trg_configs.trg_secondary_fluidlytix_cost:
            savings_bep_report_secondary = self._savings_breakeven_report(self.common_trg_configs.trg_pre_install_annual_water_expense,
                                                                          abs(gallons_car_diff_percentage),
                                                                          self.common_trg_configs.trg_secondary_fluidlytix_cost)
            
            cash_flow_year_col_vals_secondary_cost = self.cash_flow_configs.year_col_values
            cash_flow_inflow_values_secondary_cost = [self._cash_flow_sequence(i,
                                                                          self.common_trg_configs.trg_pre_install_annual_water_expense,
                                                                          abs(gallons_car_diff_percentage),
                                                                          self.common_trg_configs.trg_secondary_fluidlytix_cost) for i in range(0,11)]
        
            cash_flow_report_data_secondary_cost = [[date.today(),cash_flow_year_col_vals_secondary_cost[i], cash_flow_inflow_values_secondary_cost[i]] for i in range(0,11)]
            cash_flow_report_secondary_cost = pd.DataFrame(data=cash_flow_report_data_secondary_cost,
                                                      columns = [self.cash_flow_configs.report_date_col,self.cash_flow_configs.year_col, self.cash_flow_configs.cash_flow_col])
            
            return {'comp_report': comparison_report, 'save_bep_test': savings_bep_report_test_rate, 'save_bep_secondary': savings_bep_report_secondary, 'cashflow_report_a': cash_flow_report_test_rate, 'cashflow_report_b': cash_flow_report_secondary_cost}
        else:
            return {'comp_report': comparison_report, 'save_bep_test': savings_bep_report_test_rate, 'cashflow_report_a':cash_flow_report_test_rate} 
            

    
    def load(self, comp_report=None, save_bep_test=None, cashflow_report_a=None, save_bep_secondary=None, cashflow_report_b=None):
        db_connection = HerokuDBConnection()
        engine = db_connection.engine
        
        with engine.begin() as conn:
            if save_bep_secondary.empty and cashflow_report_b.empty:
                comp_report.to_sql('comparison_report', conn, if_exists='replace', index=False)
                save_bep_test.to_sql('savings_bep_report', conn, if_exists='replace', index=False)
                cashflow_report_a.to_sql('cash_flow_report1', conn, if_exists='replace', index=False)
                    
            else:
                comp_report.to_sql('comparison_report', conn, if_exists='replace', index=False)
                save_bep_test.to_sql('savings_bep_report', conn, if_exists='replace', index=False)
                cashflow_report_a.to_sql('cash_flow_report1', conn, if_exists='replace', index=False)
                save_bep_secondary.to_sql('savings_bep_report', conn, if_exists='append', index=False)
                cashflow_report_b.to_sql('cash_flow_report1', conn, if_exists='replace', index=False)
            
            return True
            
    def autobell_reports(self):
        # Extraction from csv
        src_car_wash_df = self.extract()
        # Transformation to get target reports
        report_df_dict = self.transform(src_car_wash_df)
        # Load into heroku postgres db
        self.load(**report_df_dict)
        
        return True
        

        
        
    # Helper methods     
    def _test_period_data_check(self, car_wash_data_df,
                                test_period_start_date: str,
                                test_period_end_date: str,
                                test_period_start_meter_cubic: int,
                                test_period_end_meter_cubic: int,
                                test_period_cars: int):
        
        #Convert test dates string to datetime
        test_period_start_date = datetime.strptime(test_period_start_date, self.date_format_configs.src_date_format).strftime(self.date_format_configs.trg_date_format)
        test_period_end_date = datetime.strptime(test_period_end_date, self.date_format_configs.src_date_format).strftime(self.date_format_configs.trg_date_format)
        
        #Check dataframe for occurance of test data
        test_period_date_check = car_wash_data_df[
            (car_wash_data_df[self.common_trg_configs.trg_start_date_col] == test_period_start_date) & 
            (car_wash_data_df[self.common_trg_configs.trg_end_date_col] == test_period_end_date)]
        
        #Add data if it does not exist
        if test_period_date_check.empty:
            test_period_data_df= pd.DataFrame(data=[
                [test_period_start_date,
                 test_period_end_date,
                 test_period_start_meter_cubic,
                 test_period_end_meter_cubic,
                 test_period_cars]
                ],columns=self.common_trg_configs.trg_prelim_cols)
            test_period_data_df[self.common_trg_configs.trg_start_date_col]=pd.to_datetime(test_period_data_df.start_date)
            test_period_data_df[self.common_trg_configs.trg_end_date_col]=pd.to_datetime(test_period_data_df.end_date)
            test_period_data_df[self.common_trg_configs.trg_start_meter_cubic_col]=test_period_data_df[self.common_trg_configs.trg_start_meter_cubic_col].astype('float64')
            test_period_data_df[self.common_trg_configs.trg_end_meter_cubic_col]=test_period_data_df[self.common_trg_configs.trg_end_meter_cubic_col].astype('float64')
            test_period_data_df[self.common_trg_configs.trg_cars_col]=test_period_data_df[self.common_trg_configs.trg_cars_col].astype('float64')
            car_wash_data_df = pd.concat([car_wash_data_df, test_period_data_df], ignore_index=True)
            return car_wash_data_df
        else:
            return car_wash_data_df
        
        
    def _savings_breakeven_report(self, annual_water_cost, savings_rate, fluidlytix_cost):
        
        # Create savings + breakeven report
        annual_savings = annual_water_cost * (savings_rate / 100.00)
        monthly_savings = round(annual_savings / 12.00, 2)
        savings_ten_year = round((annual_savings * 10.00) - fluidlytix_cost, 2)
        breakeven_point_months = round(fluidlytix_cost / monthly_savings, 2)

        savings_breakeven_report = pd.DataFrame(
            {
                self.savings_bep_configs.report_date_col: [date.today()],
                self.savings_bep_configs.annual_water_bill_col: [annual_water_cost],
                self.savings_bep_configs.savings_rate_col: [savings_rate],
                self.savings_bep_configs.monthly_savings_col: [monthly_savings],
                self.savings_bep_configs.annual_savings_col: [annual_savings],
                self.savings_bep_configs.ten_year_savings_col: [savings_ten_year],
                self.savings_bep_configs.breakeven_point_col: [breakeven_point_months],
                self.savings_bep_configs.fluidlytix_cost_col: [fluidlytix_cost]
                }
            )
        return savings_breakeven_report
    
    def _cash_flow_sequence(self, n, annual_water_cost, savings_rate , fluidlytix_cost):
        
        annual_savings = annual_water_cost * (savings_rate / 100)
        
        cost = -abs(fluidlytix_cost)
        
        if n == 0:
            return cost
        else:
            return round(cost + (n * annual_savings), 2)
