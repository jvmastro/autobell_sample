from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Float, Column

Base = declarative_base()

class ComparativeReportTable(Base):

    __tablename__ = 'comparison_report'

    test_period_days = Column('test_period_days', Float, primary_key=True)
    cars_pre_install = Column('cars_pre_install', Float, primary_key=True)
    cars_test_period = Column('cars_test_period', Float, primary_key=True)
    percent_diff_cars = Column('percent_diff_cars', Float, primary_key=True)
    consumption_pre_install= Column('consumption_pre_install', Float, primary_key=True)
    consumption_test_period= Column('consumption_test_period', Float, primary_key=True)
    percent_diff_consumption= Column('percent_diff_consumption', Float, primary_key=True)
    gallons_car_pre_install= Column('gallons_car_pre_install', Float, primary_key=True)
    gallons_car_test_period= Column('gallons_car_test_period', Float, primary_key=True)
    percent_diff_gallons_car= Column('percent_diff_gallons_car', Float, primary_key=True)
    

