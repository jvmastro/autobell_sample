from dash import Dash, html, dcc

import dash_bootstrap_components as dbc 
import plotly.express as px
import pandas as pd

external_stylesheets = [dbc.themes.COSMO]

app = Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

with open('https://raw.githubusercontent.com/jvmastro/autobell_sample/main/assets/data/projections_autobell%20copy.csv?token=GHSAT0AAAAAABT5OWKCT2XSMOLLQJ44WS2IYTYXYJA', 'rb') as f1:
    projections = pd.read_csv(f1)
with open('https://raw.githubusercontent.com/jvmastro/autobell_sample/main/assets/data/irr_autobell.csv?token=GHSAT0AAAAAABT5OWKC36JUYXFMNENPAEX2YTYXXIA', 'rb') as f2:
    irr = pd.read_csv(f2)
    
projections_55k=projections[projections['cost']=='$5,500.00']

projections_55k.rename(columns={"bill_annual": "Annual Water Bill", 
                            "savings_rate": "Observed Savings Rate",
                            "savnigs_annual": "Annual Savings",
                            "savings_10_net": "10-Year Savings",
                            "savings_monthly": "Montly Savings",
                            "bep_months": "Breakeven Point (Months)",
                            "cost": "Project Cost"},
                       inplace=True)
    
table = dbc.Table.from_dataframe(projections_55k, striped=True, bordered=True, hover=True)

graph = px.bar(irr, x='Year', y='Cash Flow')

fluidlytix_logo = "https://static.wixstatic.com/media/160184_ad4c1492eb71433cab12f62ad924a4ef~mv2.png/v1/crop/x_0,y_0,w_600,h_499/fill/w_210,h_174,al_c,q_85,usm_0.66_1.00_0.01,enc_auto/FluidLytix-Logo.png"

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

tab1_content = dbc.Card(
    dbc.CardBody(
        [
            dcc.Markdown('''
                        **Key Result:**\n
                         _On average, post-install total water usage & gallons per car **decreased**, despite an increase in total cars washed._\n
                         **Test Period Start:** April 7, 2022\n
                         **Test Period End:** May 5, 2022\n
                         
                         **Location:**\n
                         Autobell Carwash 25\n
                         8525 Hankins Rd,
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
                        dbc.Tab(tab1_content, label='30-Day Test Details'),
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
        ])
    ])
                
if __name__== '__main__':
    app.run_server(debug=True)