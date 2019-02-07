# Alpaca Markets Simple Trading Algorithm

One of the most basic algorithmic trading strategies, to buy stocks that are most bought.
Built for use with https://alpaca.markets/

Easily pushed to heroku Server.
To configure Kubernetes Server create these environment variables:
    alpaca_key_id -> Alpaca Key Id
    alpaca_secret_key -> Alpaca Secret Key
    alpaca_url -> Alpaca URL



To run daily, schedule a heroku daily task
python /src/trade_written.py
