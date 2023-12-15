import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd

app = dash.Dash(__name__)

conf = {
    'bootstrap.servers': 'b-1.monstercluster1.6xql65.c3.kafka.eu-west-2.amazonaws.com:9092',  
    'group.id': 'your_group_id',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(conf)

def consume_message():
    consumer.subscribe(['monster-damage'])
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                yield msg.value().decode('utf-8')
        except KeyboardInterrupt:
            break


try:
    sample_rate = 0.1  # 10%
    sampled_data = []
    current_window_start = datetime.now()
    current_window_end = current_window_start + timedelta(minutes=2)

    for message in consume_message():
        data = json.loads(message) 
        timestamp = datetime.strptime(data['ts'], '%Y-%m-%d %H:%M:%S.%f')
        if random.random() < sample_rate and current_window_start <= timestamp < current_window_end:
            sampled_data.append(data)
            df = pd.DataFrame(sampled_data) 
        else:
            country_counts = {}
            for entry in sampled_data:
                country = entry['country']
                country_counts[country] = country_counts.get(country, 0) + 1


            # print(f"Monsters from {current_window_start} to {current_window_end}:")
            # for country, count in country_counts.items():
            #     print(f"{country}: {count} monsters")

            current_window_start = current_window_end
            current_window_end = current_window_start + timedelta(minutes=10)
            sampled_data = [data]

finally:
    consumer.close()

data = {
    "country": ["USA", "Canada", "UK"],
    "monster_name": ["Dragon", "Goblin", "Troll"],
    "damage": [10000, 5000, 15000]
}
df = pd.DataFrame(data)

# Create the Dash app
app = dash.Dash(__name__)

# Define the app layout
app.layout = html.Div([
    html.H1("Monster Attacks"),
    dcc.Graph(
        figure=px.bar(df, x='country', y='damage', color='monster_name', title='Monster Damage by Country')
    )
])

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')