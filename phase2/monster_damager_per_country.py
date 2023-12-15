import json
import random
import pandas as pd
import plotly.express as px
from confluent_kafka import Consumer, KafkaError
import dash
from dash import html, dcc
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'b-1.monstercluster1.6xql65.c3.kafka.eu-west-2.amazonaws.com:9092',  
    'group.id': 'your_group_id',
    'auto.offset.reset': 'earliest',
}
consumer = Consumer(conf)

# Function to consume messages
def consume_message():
    consumer.subscribe(['monster-damage']) # pulling data from the topic 'monster-damage'
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        yield msg.value().decode('utf-8')

# Create the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the app layout
app.layout = html.Div([
    html.H1("Monster Attacks"),
    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  # in milliseconds
        n_intervals=0
    )
])

# Callback for live data update
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):
    sample_rate = 0.1
    sampled_data = []
    for message in consume_message():
        data = json.loads(message)
        if random.random() < sample_rate:
            sampled_data.append(data)
            break  # Only read one message per interval
    df = pd.DataFrame(sampled_data)
    figure = px.bar(df, x='country', y='damage', color='monster_name', title='Monster Damage by Country')
    return figure

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')