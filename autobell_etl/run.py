""" Runnning the Autobell ETL application"""
import argparse
import logging
import logging.config

import yaml

from etl.etl_code import AutoBellETL, CommonTargetConfigs, TestPeriodConfigs, DateFormatConfigs, ComparisonReportConfigs, SavingsBreakevenReportConfigs, CashFlowReportConfigs


def main():
    """Entry point to run the Autobell ETL job
    """
    # Parsing YAML file
    parser = argparse.ArgumentParser(description='Run the Autobell ETL job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))
    
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    
    # reading common target configurations
    common_target_configs = CommonTargetConfigs(**config['target'])
    # reading test period configurations
    test_period_cofigs = TestPeriodConfigs(**config['test'])
    # reading date configurations
    date_configs = DateFormatConfigs(**config['date'])
    # reading comaprionson report configs
    comnparioson_report_configs = ComparisonReportConfigs(**config['comparison'])
    # reading savings + breakeven report configs
    savings_bep_configs = SavingsBreakevenReportConfigs(**config['save-bep'])
    # reading cash flow report configs
    cash_flow_report_configs = CashFlowReportConfigs(**config['cashflow'])
    # creating XetraETL class instance
    logger.info('AutoBell ETL job started')
    autobell_etl = AutoBellETL(common_target_configs,
                               test_period_cofigs, date_configs,
                               comnparioson_report_configs,
                               savings_bep_configs,
                               cash_flow_report_configs)
    # running etl job
    autobell_etl.autobell_reports()
    logger.info('Autobell ETL job finished')


if __name__ == '__main__':
    main()