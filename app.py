from dash import Dash, html, dcc

import dash_bootstrap_components as dbc 
import plotly.express as px
import pandas as pd

from common.heroku_psql import InternalHerokuDBConnector


db_connection = InternalHerokuDBConnector()
Session = db_connection.Session

all_query = "SELECT start_date, end_date, cars, consumption_cubic, consumption_gal, gallons_car FROM autobell_complete_data;"
pre_post_query = "SELECT * FROM pre_post_reports ORDER BY id DESC LIMIT 1;"
payback_query = """
        SELECT annual_water_cost, savings_rate, monthly_savings, annual_savings, savings_ten, breakeven_months, fluidlytix_cost 
        FROM 
            (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY date_added DESC) rank FROM payback_reports) sub
        WHERE sub.rank = 1;"""
cashflow_query = """
        SELECT project_install, year_1, year_2, year_3, year_4, year_5, year_6, year_7, year_8, year_9, year_10 
        FROM 
            (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY date_added DESC) rank FROM cashflow_reports) sub
        WHERE sub.rank = 1;"""

with Session as session:
    all_df = pd.read_sql(all_query, session.connection())
    pre_post_df = pd.read_sql(pre_post_query, session.connection())
    payback_df = pd.read_sql(payback_query, session.connection())
    cashflow_df = pd.read_sql(cashflow_query, session.connection())
    

def constrcut_all_table(all_df):
    all_df['start_date'] = all_df.start_date.dt.strftime('%Y-%m-%d')
    all_df['end_date'] = all_df.end_date.dt.strftime('%Y-%m-%d')
    all_df[['cars','consumption_cubic', 'consumption_gal', 'gallons_car']] = all_df[['cars','consumption_cubic', 'consumption_gal', 'gallons_car']].applymap('{:,.2f}'.format)
    all_df.columns = ['Start Date', 'End Date', 'Cars', 'Usage (Cubic)', 'Usage (Gallon)', 'Gal/Car']

    all_data_table = dbc.Table.from_dataframe(all_df, striped=True, bordered=True, hover=True)
    
    return all_data_table

all_data_tbl = constrcut_all_table(all_df)
    
def construct_payback_table(payback_df):
        
    payback_df[['annual_water_cost', 'monthly_savings', 'annual_savings','savings_ten', 'fluidlytix_cost']] = payback_df[['annual_water_cost', 'monthly_savings', 'annual_savings','savings_ten', 'fluidlytix_cost']].applymap('${:,.2f}'.format)
    payback_df['savings_rate']=payback_df['savings_rate'].map('{:.2f}%'.format)
    payback_df = payback_df.rename(columns=
                                        {'annual_water_cost': 'Annual Water Bill', 
                                            'savings_rate': 'Savings Rate',
                                            'monthly_savings': 'Monthly Savings',
                                            'annual_savings': 'Annual Savings',
                                            'savings_ten': '10-Year Savings',
                                            'breakeven_months': 'Breakeven Point (Months)',
                                            'fluidlytix_cost': 'Water Savings Solution'})
    payback_table = dbc.Table.from_dataframe(payback_df, striped=True, bordered=True, hover=True, className='mt-4 mb-4')
    
    return payback_table

payback_table = construct_payback_table(payback_df)

def construct_cashflow_graph(cashflow_df):
    
    cashflow_graph = px.bar(cashflow_df.loc[0] , x = cashflow_df.columns.to_list(), y= cashflow_df.loc[0])
    cashflow_graph.update_layout(
        xaxis=dict(
            title = None,
            tickmode = 'array',
            tickvals = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ticktext = [x.title().replace('_', ' ') for x in cashflow_df.columns.to_list()],
            tickangle= 45
        ),
        yaxis=dict(
            title = 'Cash Flow ($)',
            showticklabels = True,
            hoverformat = '$,.2f',
            tickformat = ',.0f'),
        margin=dict(l=20, r=20, t=20, b=20))
    
    return cashflow_graph

cashflow_graph = construct_cashflow_graph(cashflow_df)        


external_stylesheets = [dbc.themes.COSMO]
app = Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server


fluidlytix_logo = "https://static.wixstatic.com/media/160184_ad4c1492eb71433cab12f62ad924a4ef~mv2.png/v1/crop/x_0,y_0,w_600,h_499/fill/w_210,h_174,al_c,q_85,usm_0.66_1.00_0.01,enc_auto/FluidLytix-Logo.png"
pws_logo = "https://pristineworldsolutions.com/wp-content/uploads/2021/03/big-logo.png"

navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=fluidlytix_logo, height="75px")),
                        dbc.Col(dbc.NavbarBrand("Fluidlytix Post-Installation Report", className="ml-2", style={"color":"#FFFFFF"})),
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

navbar2 = dbc.Navbar(
    dbc.Container(
        [
            html.A([
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=pws_logo, height="75px")),
                        dbc.Col(dbc.NavbarBrand("Pristine World Solutions: Fluidlytix Channel Partner", className="ml-2", style={"color":"#FFFFFF"})),
                        ], align="center"),
                ]
                ),
            ]
        ),
    color="primary",
    dark=True,
    className="mb-4",
)

install_content = dbc.Card([
    dbc.CardHeader('Installation Details', className="card-header"),
    dbc.CardBody([
        dcc.Markdown(f"""
                     **Valve:** (1) 2" Fluidlytix Valve
                     
                     **Install Date:** March  11, 2021  
                     
                     **Post-Install Reporting Start Date:** April 7, 2021
                     
                     **Location:**  
                     
                    _Autobell Carwash 25_  
                    
                    _8525 Hankins Road, Charlotte, NC 28269_  
                     
                     
                     """),
        html.Hr(),
        dbc.CardImg(src='https://bloximages.newyork1.vip.townnews.com/statesville.com/content/tncms/assets/v3/editorial/7/2d/72d254e4-5f1c-5269-92eb-b1fd803969ce/5e90e5d3a2f8a.image.png?crop=1439%2C1439%2C0%2C0&resize=1439%2C1439&order=crop%2Cresize', className='mt-3 mb-3'),
        html.Hr()
        ])
    ], className='mt-3')

savings_card=dbc.Card([
    dbc.CardHeader("Fluidlytix Savings Rate", className="card-header"),
    dbc.CardBody([
        dcc.Markdown(
            f'''
            ## {'{:.2f}%'.format(pre_post_df.pct_change_gal_car[0])}
            ''',style={'textAlign': 'center'}
            ),
        ]),
    dbc.CardFooter('Target: 15.00%'),
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
                    ## {'{:,.2f}'.format(pre_post_df.post_cars[0])}
                    ''',
                    style={'textAlign': 'center'}
                )
            ]
        ),
        dbc.CardFooter(f"↑ {'{:,.2f}'.format(abs(pre_post_df.total_change_cars[0]))} ({'{:,.2f}'.format(pre_post_df.pre_cars[0])})", className="card-footer"),
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
                    ## {'{:,.2f}'.format(pre_post_df.post_usage[0])}
                    ''',
                    style={'textAlign': 'center'}
                )
            ]
        ),
        dbc.CardFooter(f"↓ {'{:,.2f}'.format(abs(pre_post_df.total_change_usage[0]))} ({'{:,.2f}'.format(pre_post_df.pre_usage[0])})", className="card-footer"),
    ],
     className="card border-success mt-3",
)

gal_car_card=dbc.Card(
    [
        dbc.CardHeader("Gallons per Car", className="card-header"),
        dbc.CardBody(
            [
                dcc.Markdown(
                    f'''
                    ## {'{:.2f}'.format(pre_post_df.post_gal_car[0])}
                    ''',
                    style={'textAlign': 'center'})
            ]
        ),
        dbc.CardFooter(f"↓ {'{:.2f}%'.format(pre_post_df.pct_change_gal_car[0])} ({'{:.2f}'.format(pre_post_df.pre_gal_cal[0])})", className="card-footer"),
    ],
     className="card border-success mt-3",
)

app.layout = html.Div([
    navbar,
    dbc.Container([
        dbc.Row([
            dbc.Col(width = 12, children =[
                dbc.Tabs([
                    dbc.Tab(label= 'Autobell 25',
                            label_class_name = 'fw-bold',
                            children=[
                                dbc.Row([
                                    dbc.Col(install_content,width=4),
                                    dbc.Col([
                                        dbc.Row([
                                            dbc.Col(savings_card, width = 3),
                                            dbc.Col(cars_card, width = 3),
                                            dbc.Col(usage_card, width = 3),
                                            dbc.Col(gal_car_card, width = 3)
                                            ]),
                                        dbc.Row([
                                            dbc.Col([
                                                dbc.Tabs([
                                                    dbc.Tab(label = 'Cash Flow Reports',
                                                            label_class_name = 'fw-bold mt-3', children=[
                                                                payback_table,
                                                                dcc.Graph(figure = cashflow_graph)
                                                                ]),
                                                    dbc.Tab(label = 'Meter Reads',
                                                            label_class_name = 'fw-bold mt-3',
                                                            children = [html.Div(all_data_tbl, className='scrollit')]
                                                            )
                                                    ])
                                                ])
                                            ])
                                        ]),
                                    ]),
                                ]
                            ),
                    ])
                ])
            ])
        ]),
    navbar2
    ])

if __name__== '__main__':
    app.run_server(debug=True)