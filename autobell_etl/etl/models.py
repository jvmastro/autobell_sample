from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Float, String, Column

Base = declarative_base()

class ComparativeReport(Base):

    __tablename__ = 'comparison_report'

    test_period_days = Column('test_period_days', Float)
    cars_pre_install = Column('cars_pre_install', Float)
    cars_test_period = Column('cars_test_period', Float)
    percent_diff_cars = Column('percent_diff_cars', Float)
    consumption_pre_install= Column('consumption_pre_install', Float)
    consumption_test_period= Column('consumption_test_period', Float)
    percent_diff_consumption= Column('percent_diff_consumption', Float)
    gallons_car_pre_install= Column('gallons_car_pre_install', Float)
    gallons_car_test_period= Column('gallons_car_test_period', Float)
    percent_diff_gallons_car= Column('percent_diff_gallons_car', Float)

class SavingsBEP(Base):
    
    __tablename__= 'savings_bep_report'
    
    
    annual_water_bill = Column('annual_water_bill ', Float)
    savings_rate = Column('savings_rate', Float)
    monthly_savings = Column('monthly_savings', Float)
    annual_savings = Column('annual_savings', Float)
    savings_10_year = Column('savings_10_year', Float)
    bep_months = Column('bep_months', Float)
    fluidlytix_project_cost = Column('fluidlytix_project_cost', Float)

class CashFlowA(Base):
    
    __tablename__ = 'cash_flow_report1'
    
    year = Column('year', String(20))
    cash_flow = Column('cash_flow', Float)

class CashFlowB(Base):
    
    __tablename__ = 'cash_flow_report12'
    
    year = Column('year', String(20))
    cash_flow = Column('cash_flow', Float)

