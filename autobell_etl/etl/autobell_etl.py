import pandas as pd
from datetime import datetime, date

class AutobellETL:
    """
    ETL class for hotel data for Fluidlytix post-install reporting
    """    
    INSTALL_DATE = datetime.strptime('4/7/2022','%m/%d/%Y').strftime('%Y-%m-%d')
    
    def __init__(self, db_connector, pre_post_item, payback_item, cashflow_item):
        self._connector = db_connector
        self._pre_post_item = pre_post_item
        self._payback_item = payback_item
        self._cashflow_item = cashflow_item
    
    def extract(self):
        with self._connector.Session as session:
            pre_data = pd.read_sql(f"SELECT * FROM autobell_complete_data WHERE start_date < '{self.INSTALL_DATE}';", session.connection())
            post_data = pd.read_sql(f"SELECT * FROM autobell_complete_data WHERE start_date = '{self.INSTALL_DATE}';", session.connection())
            
        return pre_data, post_data
        
    def transform(self, pre_data, post_data):
        
        pre_cars_mean = pre_data.cars.mean()
        pre_usage_mean_with_reclaim = (pre_data.consumption_gal.sum() - 8800.00) / 13
        pre_gal_car_with_reclaim = pre_usage_mean_with_reclaim / pre_data.cars.mean()

        # Calcuate pre vs post delta
        cars_diff = post_data.cars[0] - pre_cars_mean
        usage_diff = post_data.consumption_gal[0] - pre_usage_mean_with_reclaim
        gal_car_pct_change = abs((post_data.gallons_car[0] - pre_gal_car_with_reclaim) / pre_gal_car_with_reclaim * 100)
        
        ## Prepare pre vs. post dict
        pre_post_keys = ['date_added', 'pre_cars','post_cars','total_change_cars', 'pre_usage', 'post_usage', 'total_change_usage','pre_gal_cal', 'post_gal_car', 'pct_change_gal_car']
        pre_post_values = [pre_cars_mean, post_data.cars[0], cars_diff, pre_usage_mean_with_reclaim, post_data.consumption_gal[0], usage_diff, pre_gal_car_with_reclaim, post_data.gallons_car[0], gal_car_pct_change]
        pre_post_values = list(map('{:.2f}'.format, pre_post_values))
        pre_post_values.insert(0, date.today())
        pre_post_dict = dict(zip(pre_post_keys, pre_post_values))
        pre_post_dict
        
        #Prepare updated paypack dict
        payback_dict = self._payback_report(31876.00, gal_car_pct_change, 5500.00)
        
        #Prepare updated cashflow dict
        cashflow_values = [self._cashflow_sequence(i, 31876.00, gal_car_pct_change, 5500.00) for i in range(0,11)]
        cashflow_values.insert(0, date.today())
        cashflow_keys = ['date_added', 'project_install', 'year_1', 'year_2', 'year_3', 'year_4', 'year_5', 'year_6', 'year_7', 'year_8','year_9', 'year_10']
        cashflow_dict = dict(zip(cashflow_keys, cashflow_values))
        
        return pre_post_dict, payback_dict, cashflow_dict
    
    
    def load(self, pre_post_dict, payback_dict, cashflow_dict):
        
        with self._connector.Session as session:
            
            pre_post_data = self._pre_post_item(**pre_post_dict)
            payback_data = self._payback_item(**payback_dict)
            cashflow_data = self._cashflow_item(**cashflow_dict)
            
            try:
                session.add(pre_post_data)
                session.add(payback_data)
                session.add(cashflow_data)
                session.commit()
            except:
                session.rollback()
            

    def etl_reports(self):
        """
        Extract, transform and load to create reports
        """

        # Extrcation
        pre_data, post_data = self.extract()
        # Transformation
        pre_post_dict, payback_dict, cashflow_dict = self.transform(pre_data, post_data)
        # Load
        self.load(pre_post_dict, payback_dict, cashflow_dict)
        
        return True
    
    def _payback_report(self, annual_water_cost, savings_rate, fluidlytix_cost):
        
        # Create savings + breakeven report
        annual_savings = annual_water_cost * (savings_rate / 100.00)
        monthly_savings = round(annual_savings / 12.00, 2)
        savings_ten = round((annual_savings * 10.00) - fluidlytix_cost, 2)
        breakeven_months = round(fluidlytix_cost / monthly_savings, 2)
        
        payback_keys = ['date_added', 'annual_water_cost', 'savings_rate', 'monthly_savings', 'annual_savings', 'savings_ten','breakeven_months', 'fluidlytix_cost']
        payback_values = [annual_water_cost, savings_rate, monthly_savings, annual_savings, savings_ten, breakeven_months, fluidlytix_cost]
        payback_values = list(map('{:.2f}'.format, payback_values))
        payback_values.insert(0, date.today())
        payback_dict = dict(zip(payback_keys,payback_values))
        
        return payback_dict
    
    def _cashflow_sequence(self, n, annual_water_cost, savings_rate , fluidlytix_cost):
        
        annual_savings = annual_water_cost * (abs(savings_rate) / 100)
        
        cost = -abs(fluidlytix_cost)
        
        if n == 0:
            return cost
        else:
            return round(cost + (n * annual_savings), 2)

    
   
