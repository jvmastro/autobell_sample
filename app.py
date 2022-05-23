from click import style
from dash import Dash, html, dcc, dash_table

import dash_bootstrap_components as dbc 
import plotly.express as px
import pandas as pd

from autobell_etl.etl.db_conn import HerokuDBConnection
from functools import lru_cache


from sqlalchemy.ext.automap import automap_base

db_connection = HerokuDBConnection()
session = db_connection.session

Base = automap_base()
Base.prepare(db_connection.engine, reflect=True)

TestPreInstall = Base.classes.comparison_report
PaybackDetail = Base.classes.savings_bep_report
CashFlowDetail = Base.classes.cash_flow_report


@lru_cache()
def get_key_stats(session, mapped_class, result_column_name, comp_column_name=None, diff_column_name=None):
    with session as s:
        query = s.query(mapped_class).first()
        
        if result_column_name == 'test_period_days':
            return '{:.0f}'.format(getattr(query, result_column_name))
        
        result_stat = '{:,.2f}'.format(getattr(query, result_column_name))
        comp_stat = '{:,.2f}'.format(getattr(query, comp_column_name))
        
        if diff_column_name == 'percent_diff_gallons_car':
            diff = '{:.2f}%'.format(getattr(query, diff_column_name))
        else:
            diff = '{:,.2f}'.format(getattr(query, diff_column_name))
    return result_stat, comp_stat, diff


days = get_key_stats(session, TestPreInstall, 'test_period_days')
cars_result, cars_comp, cars_diff  = get_key_stats(session, TestPreInstall, 'cars_test_period', 'cars_pre_install', 'diff_cars')
usage_result, usage_comp, usage_diff  = get_key_stats(session, TestPreInstall, 'consumption_test_period','consumption_pre_install', 'diff_consumption')
gal_car_result, gal_car_comp, gal_car_diff  = get_key_stats(session, TestPreInstall, 'gallons_car_test_period', 'gallons_car_pre_install', 'percent_diff_gallons_car')


def construct_payback_table(session, fluidlytix_cost):
    with session as s:
        payback_df = pd.read_sql(f"""
                                 SELECT annual_water_bill,savings_rate, monthly_savings,annual_savings, savings_10_year,bep_months, fluidlytix_project_cost 
                                 FROM savings_bep_report WHERE fluidlytix_project_cost = {fluidlytix_cost}
                                 """, s.connection())
    payback_df[['annual_water_bill', 'monthly_savings', 'annual_savings', 'savings_10_year', 'fluidlytix_project_cost']] = payback_df[['annual_water_bill', 'monthly_savings', 'annual_savings', 'savings_10_year', 'fluidlytix_project_cost']].applymap('${:,.2f}'.format)
    payback_df['savings_rate']=payback_df['savings_rate'].map('{:.2f}%'.format)
    payback_df = payback_df.rename(columns={"annual_water_bill": "Annual Water Bill", 
                            "savings_rate": "Savings Rate",
                            "monthly_savings": "Monthly Savings",
                            "annual_savings": "Annual Savings",
                            "savings_10_year": "10-Year Savings",
                            "bep_months": "Breakeven Point (Months)",
                            "fluidlytix_project_cost": "Water Savings Solution"})
    payback_table = dbc.Table.from_dataframe(payback_df, striped=True, bordered=True, hover=True, className='mt-4 mb-4')
    
    return payback_table

current_payback_table = construct_payback_table(session, 5500.0)

def construct_cash_flow_graph(session, fluidlytix_cost):
    with session as s:
        cash_flow_df = pd.read_sql(f"""
                                 SELECT project_install, year_1, year_2, year_3, year_4, year_5, year_6, year_7, year_8, year_9, year_10
                                 FROM cash_flow_report WHERE project_install = {fluidlytix_cost}
                                 """, s.connection())
        cash_flow_graph = px.bar(cash_flow_df.loc[0], x = cash_flow_df.columns.to_list(), y= cash_flow_df.loc[0])
        cash_flow_graph.update_layout(
            xaxis=dict(
                title = None,
                tickmode = 'array',
                tickvals = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                ticktext = [x.title().replace('_', ' ') for x in cash_flow_df.columns.to_list()],
                tickangle= 45
            ),
            yaxis=dict(
                title = 'Cash Flow ($)',
                showticklabels = True,
                hoverformat = '$,.2f',
                tickformat = ',.0f'
        ),
            margin=dict(l=20, r=20, t=20, b=20)
    )
    return cash_flow_graph

current_cash_flow_graph = construct_cash_flow_graph(session, -5500.00)
        
        


external_stylesheets = [dbc.themes.COSMO]
app = Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server


fluidlytix_logo = "https://static.wixstatic.com/media/160184_ad4c1492eb71433cab12f62ad924a4ef~mv2.png/v1/crop/x_0,y_0,w_600,h_499/fill/w_210,h_174,al_c,q_85,usm_0.66_1.00_0.01,enc_auto/FluidLytix-Logo.png"
pws_logo = "https://pristineworldsolutions.com/wp-content/uploads/2021/03/big-logo.png"

navbar_header = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=fluidlytix_logo, height="75px")),
                        dbc.Col(dbc.NavbarBrand("Post-Installation Report", className="ml-2", style={"color":"#FFFFFF"})),
                    ],
                    align="center",
                    )
                ),
            ]
        ),
    color="primary",
    dark=True,
    className="mb-4",
)

navbar_footer = dbc.Navbar(
    dbc.Container(
        [
            html.A([
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=pws_logo, height="75px")),
                        dbc.Col(dbc.NavbarBrand("Pristine World Solutions: Channel Partner", className="ml-2", style={"color":"#FFFFFF"})),
                        ], align="center"),
                ]
                ),
            ]
        ),
    color="primary",
    dark=True,
    className="mb-4",
)

install_details_tab_content = dbc.Card(
    dbc.CardBody(
        [
            dcc.Markdown('''
                        **Key Result:** _On average, post-install total water usage & gallons per car **decreased**, despite an increase in total cars washed._\n
                         **Test Period Start:** April 7, 2022\n
                         **Test Period End:** May 5, 2022\n
                         
                         **Location:**\n
                         Autobell Carwash 25\n
                         8525 Hankins Road,
                         Charlotte, NC 28269
                         
                         **Valve Details:** (1) 2" Fluidlytix Valve
                        
                        '''),
            dbc.CardImg(src="https://bloximages.newyork1.vip.townnews.com/statesville.com/content/tncms/assets/v3/editorial/7/2d/72d254e4-5f1c-5269-92eb-b1fd803969ce/5e90e5d3a2f8a.image.png?crop=1439%2C1439%2C0%2C0&resize=1439%2C1439&order=crop%2Cresize")
        ]
    ),
    className="mt-3",
)

days_card=dbc.Card(
    [
        dbc.CardHeader("Test Period Days", className="card-header"),
        dbc.CardBody(
            [
                dcc.Markdown(
                    f'''
                    ## {days}
                    ''',
                    style={'textAlign': 'center'}
                )
            ]
        ),
    ],
     className="card border-success mt-3",
)

cars_card=dbc.Card(
    [
        dbc.CardHeader("Cars Serviced", className="card-header"),
        dbc.CardBody(
            [
                dcc.Markdown(
                    f'''
                    ## {cars_result}
                    ''',
                    style={'textAlign': 'center'}
                )
            ]
        ),
        dbc.CardFooter(f"↑ {cars_diff} ({cars_comp})", className="card-footer"),
    ],
     className="card border-success mt-3",
)

usage_card=dbc.Card(
    [
        dbc.CardHeader("Usage Gallons", className="card-header"),
        dbc.CardBody(
            [
                dcc.Markdown(
                    f'''
                    ## {usage_result}
                    ''',
                    style={'textAlign': 'center'}
                )
            ]
        ),
        dbc.CardFooter(f"↓ {usage_diff} ({usage_comp})", className="card-footer"),
    ],
     className="card border-success mt-3",
)

gal_car_card=dbc.Card(
    [
        dbc.CardHeader("Gallons/Car", className="card-header"),
        dbc.CardBody(
            [
                dcc.Markdown(
                    f'''
                    ## {gal_car_result}
                    ''',
                    style={'textAlign': 'center'})
            ]
        ),
        dbc.CardFooter(f"↓ {gal_car_diff} ({gal_car_comp})", className="card-footer"),
    ],
     className="card border-success mt-3",
)

current_payback_tab_content = dbc.Col(
    [
        current_payback_table,
        dbc.Row([
            dbc.Col([
                dbc.Alert(" ⓘ Hover over columns to display total cash flow", color="primary")
                ], width = 6 , align = 'end')
            ], justify = 'center'),
        dcc.Graph(figure = current_cash_flow_graph)
        ]
    )

app.layout = html.Div([
    navbar_header,
    dbc.Container([
        dbc.Row([
            dbc.Col(width=4, children=[
                dbc.Tabs(
                    [
                        dbc.Tab(install_details_tab_content, label='Pilot Installation Details'),
                    ]
                    )
                ]),
            dbc.Col(width=8, children=[
                dbc.Tabs(
                    [
                        dbc.Tab(label='Key Statistics', children=[
                            dbc.Row([
                                dbc.Col(days_card, width = 3),
                                dbc.Col(cars_card, width = 3),
                                dbc.Col(usage_card, width = 3),
                                dbc.Col(gal_car_card, width = 3)
                                ], className='mb-4'),
                            dbc.Row([
                                dbc.Tabs(
                                    [
                                        dbc.Tab(current_payback_tab_content, label = 'Cash Flow Schedule - Autobell 25')
                                    ])
                                ])
                            ])
                        ]
                    )
                ]),
            ])
        ]),
    navbar_footer
    ])
                
if __name__== '__main__':
    app.run_server(debug=True)
    
