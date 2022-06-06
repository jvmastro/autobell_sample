from sqlalchemy import Column, Date, Integer, Numeric
from sqlalchemy.orm import declarative_base


Base = declarative_base()

def create_tables(engine):
    """"""
    Base.metadata.create_all(engine)

class AutobellItem(Base):
    
    __tablename__ = 'autobell_complete_data'
    
    start_date = Column('start_date', Date, primary_key=True)
    end_date = Column('end_date', Date, unique=True, nullable=False)
    start_meter_cubic = Column('start_meter_cubic', Numeric(scale= 2), nullable=False)
    end_meter_cubic = Column('end_meter_cubic', Numeric(scale= 2), nullable=False)
    cars = Column('cars', Numeric(scale= 2), nullable=False)
    days = Column('days', Integer, nullable=False)
    consumption_cubic = Column('consumption_cubic', Numeric(scale= 2), nullable=False)
    start_meter_gal = Column('start_meter_gal', Numeric(scale= 2), nullable=False)
    end_meter_gal = Column('end_meter_gal', Numeric(scale= 2), nullable=False)
    consumption_gal = Column('consumption_gal', Numeric(scale= 2), nullable=False)
    gallons_car = Column('gallons_car', Numeric(scale= 2), nullable=False)

class AutobellPrePostItem(Base):
    
    __tablename__ = 'pre_post_reports'
    
    id = Column('id', Integer, primary_key = True)
    date_added = Column('date_added', Date, nullable=False)
    pre_cars = Column('pre_cars', Numeric(scale= 2), nullable = False)
    post_cars= Column('post_cars', Numeric(scale= 2), nullable = False)
    total_change_cars= Column('total_change_cars', Numeric(scale= 2), nullable = False)
    pre_usage= Column('pre_usage', Numeric(scale= 2), nullable = False)
    post_usage= Column('post_usage', Numeric(scale= 2), nullable = False)
    total_change_usage= Column('total_change_usage', Numeric(scale= 2), nullable = False)
    pre_gal_cal= Column('pre_gal_cal', Numeric(scale= 2), nullable = False)
    post_gal_car= Column('post_gal_car', Numeric(scale= 2), nullable = False)
    pct_change_gal_car= Column('pct_change_gal_car', Numeric(scale= 2), nullable = False)
    

class AutobellPaybackItem(Base):
    
    __tablename__ = 'payback_reports'
    
    id = Column('id', Integer, primary_key = True)
    date_added = Column('date_added', Date, nullable=False)
    annual_water_cost = Column('annual_water_cost', Numeric(scale= 2), nullable = False)
    savings_rate = Column('savings_rate', Numeric(scale= 2), nullable = False)
    monthly_savings = Column('monthly_savings', Numeric(scale= 2), nullable = False)
    annual_savings= Column('annual_savings', Numeric(scale= 2), nullable = False)
    savings_ten= Column('savings_ten', Numeric(scale= 2), nullable = False)
    breakeven_months= Column('breakeven_months', Numeric(scale= 2), nullable = False)
    fluidlytix_cost = Column('fluidlytix_cost', Numeric(scale= 2), nullable = False)
    

class AutobellCashFlowItem(Base):
    
    __tablename__ = 'cashflow_reports'
    
    id = Column('id', Integer, primary_key = True)
    date_added = Column('date_added', Date, nullable=False)
    project_install = Column('project_install', Numeric(scale= 2), nullable = False)
    year_1 = Column('year_1', Numeric(scale= 2), nullable = False)
    year_2 = Column('year_2', Numeric(scale= 2), nullable = False)
    year_3 = Column('year_3', Numeric(scale= 2), nullable = False)
    year_4 = Column('year_4', Numeric(scale= 2), nullable = False)
    year_5 = Column('year_5', Numeric(scale= 2), nullable = False)
    year_6 = Column('year_6', Numeric(scale= 2), nullable = False)
    year_7 = Column('year_7', Numeric(scale= 2), nullable = False)
    year_8 = Column('year_8', Numeric(scale= 2), nullable = False)
    year_9 = Column('year_9', Numeric(scale= 2), nullable = False)
    year_10 = Column('year_10', Numeric(scale= 2), nullable = False)