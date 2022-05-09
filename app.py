from ssl import get_default_verify_paths
from dash import Dash, html, dcc

import dash_bootstrap_components as dbc 
import plotly.express as px
import pandas as pd

from sqlalchemy import create_engine

external_stylesheets = [dbc.themes.COSMO]
engine = create_engine('postgresql://xgzayqdpuwhzqg:ec55eebdf235fa13d20b498f899768edd745db7e2cad6bb64668014987e3334d@ec2-54-158-247-210.compute-1.amazonaws.com:5432/d924dsh401okkp')

cash_flow_5500 = pd.read_sql('SELECT * FROM cash_flow', engine)
savings = pd.read_sql("SELECT * FROM savings WHERE cost = '$5,500.00'", engine)

app = Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server


savings = savings.rename(columns={"bill_annual": "Annual Water Bill", 
                            "savings_rate": "Savings Rate",
                            "savnigs_annual": "Annual Savings",
                            "savings_10_net": "10-Year Savings",
                            "savings_monthly": "Monthly Savings",
                            "bep_months": "Breakeven Point (Months)",
                            "cost": "Project Cost"})
    
table = dbc.Table.from_dataframe(savings, striped=True, bordered=True, hover=True)

graph = px.bar(cash_flow_5500, x='Year', y='Cash Flow', hover_data={'Cash Flow':':,.2f'})
graph.update_layout(xaxis=dict(tickmode='array',
                  tickvals= [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                  ticktext = ['Project Installation', 'Year 1', 'Year 2', 
                              'Year 3','Year 4', 'Year 5', 
                              'Year 6', 'Year 7', 'Year 8', 
                              'Year 9', 'Year 10']
                  ))

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

tab1_content = dbc.Card(
    dbc.CardBody(
        [
            dcc.Markdown('''
                        **Key Results:**\n
                         _On average, post-install total water usage & gallons per car **decreased**, despite an increase in total cars washed._\n
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
                    '''
                    ## 27
                    ''',
                    style={'textAlign': 'center'}
                )
            ]
        ),
        dbc.CardFooter("↓ 4 (31)", className="card-footer"),
    ],
     className="card border-success mt-3",
)

cars_card=dbc.Card(
    [
        dbc.CardHeader("Cars Serviced", className="card-header"),
        dbc.CardBody(
            [
                dcc.Markdown(
                    '''
                    ## 11,684
                    ''',
                    style={'textAlign': 'center'}
                )
            ]
        ),
        dbc.CardFooter("↑ 2,331 (9,352)", className="card-footer"),
    ],
     className="card border-success mt-3",
)

usage_card=dbc.Card(
    [
        dbc.CardHeader("Usage Gallons", className="card-header"),
        dbc.CardBody(
            [
                dcc.Markdown(
                    '''
                    ## 220,674.75
                    ''',
                    style={'textAlign': 'center'}
                )
            ]
        ),
        dbc.CardFooter("↓ 2,877.12 (223,551.87)", className="card-footer"),
    ],
     className="card border-success mt-3",
)

gal_car_card=dbc.Card(
    [
        dbc.CardHeader("Gal/Car", className="card-header"),
        dbc.CardBody(
            [
                dcc.Markdown(
                    '''
                    ## 18.89
                    ''',
                    style={'textAlign': 'center'})
            ]
        ),
        dbc.CardFooter("↓ 21.04% (24)", className="card-footer"),
    ],
     className="card border-success mt-3",
)

app.layout = html.Div([
    navbar,
    dbc.Container([
        dbc.Row([
            dbc.Col(width=4, children=[
                dbc.Tabs(
                    [
                        dbc.Tab(tab1_content, label='Pilot Installation Details'),
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
                                ]),
                            dbc.Row([
                                html.H4('Cash Flow Schedules', className='mt-4 mb-4'),
                                html.Hr(),
                                dbc.Col(table, width=12),
                                dcc.Graph(id='cash-flows', figure= graph)
                                ])
                            ]),
                        ]
                    )
                ]),
            ])
        ]),
    navbar2
    ])
                
if __name__== '__main__':
    app.run_server(debug=True)
    
