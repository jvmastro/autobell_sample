from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Float, String, Column

Base = declarative_base()

class ComparativeReport(Base):

    __tablename__ = 'comparison_report'

    test_period_days = Column('test_period_days', Float, nullable=False)
    cars_pre_install = Column('cars_pre_install', Float, nullable=False)
    cars_test_period = Column('cars_test_period', Float, nullable=False)
    percent_diff_cars = Column('percent_diff_cars', Float, nullable=False)
    consumption_pre_install= Column('consumption_pre_install', Float, nullable=False)
    consumption_test_period= Column('consumption_test_period', Float, nullable=False)
    percent_diff_consumption= Column('percent_diff_consumption', Float, nullable=False)
    gallons_car_pre_install= Column('gallons_car_pre_install', Float, nullable=False)
    gallons_car_test_period= Column('gallons_car_test_period', Float, nullable=False)
    percent_diff_gallons_car= Column('percent_diff_gallons_car', Float, nullable=False)

class SavingsBEP(Base):
    
    __tablename__= 'savings_bep_report'
    
    annual_water_bill = Column('annual_water_bill ', Float, nullable=False)
    savings_rate = Column('savings_rate', Float, nullable=False)
    monthly_savings = Column('monthly_savings', Float, nullable=False)
    annual_savings = Column('annual_savings', Float, nullable=False)
    savings_10_year = Column('savings_10_year', Float, nullable=False)
    bep_months = Column('bep_months', Float, nullable=False)
    fluidlytix_project_cost = Column('fluidlytix_project_cost', Float, nullable=False)

class CashFlowA(Base):
    
    __tablename__ = 'cash_flow_report1'
    
    year = Column('year', String(20), nullable=False)
    cash_flow = Column('cash_flow', Float, nullable=False)

class CashFlowB(Base):
    
    __tablename__ = 'cash_flow_report12'
    
    year = Column('year', String(20), nullable=False)
    cash_flow = Column('cash_flow', Float, nullable=False)

