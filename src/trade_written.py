import alpaca_trade_api as tradeapi 
from datetime import datetime as dt
import json
import bs4 as bs
import pickle
import requests
import finsymbols as symbols
import logging
import os
import pandas as pd
import time
import operator
LOG_FILENAME = 'alpaca.log'
NY = 'US/Eastern'
logging.basicConfig(filename=LOG_FILENAME) 
logger = logging.getLogger(__name__)

api = tradeapi.REST(
    key_id = os.environ.get('alpaca_key_id'),
    secret_key= os.environ.get('alpaca_secret_key'),
    base_url=os.environ.get('alpaca_url')
)

def save_sp500_tickers():
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, "lxml")
    table= soup.find('table', {'class':'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)

    with open("sp500tickers.pickle", "wb") as f:
        pickle.dump(tickers,f)
    
    return tickers

def main():
    # empty!
    

    #Getting universe of sp500
    sp500 = save_sp500_tickers()
    count = 0
    while count < 500:
        clock = api.get_clock()
        now = clock.timestamp
        done = ""
        if clock.is_open and done != now.strftime('%Y-%m-%d'):
            print("Executing trades")
            
            price_df = prices(save_sp500_tickers())
            scores = calc_scores(price_df)
            orders = get_orders(api, price_df, position_size = 1000, max_positions = 10))
            trade(orders


            done = now.strftime('%Y-%m-%d')
            logger.info(f'done for {done}')
        else:
            price_df = prices(save_sp500_tickers())
            # scores = calc_scores(price_df)
            # orders = get_orders(api, price_df)
            # trade(orders)
            print('markets arent open yet')
        time.sleep(1)
        count = count + 1

# Parameters: symbols -     The stock ticker of the stocks you are getting information for
def prices(symbols):
    '''Get the map of prices in DataFrame with the symbol name key.'''
    now = pd.Timestamp.now(tz=NY)
    end_dt = now
    if now.time() >= pd.Timestamp('09:30', tz=NY).time():
        today = dt.now()
        wanted = today.strftime('%H:%M:%S')
        end_dt = now - \
            pd.Timedelta(wanted) - pd.Timedelta('1 minute')
        print(end_dt)
    return _get_prices(symbols, end_dt)





def _get_prices(symbols, end_dt, max_workers=5):
    '''Get the map of DataFrame price data from ALpaca's data API.'''
    start_dt = end_dt - pd.Timedelta('50 days')
    start = pd.Timestamp(start_dt).isoformat()
    end = end_dt.isoformat()
    # end = end_dt
    print(start)
    print(end)


    def get_barset(symbols):
        return api.get_barset(
            symbols,
            'day',
            limit = 50,
            start=pd.to_datetime(start),
            end=pd.to_datetime(end)
        )

    # The maximum number of symbols we can request at once is 200.
    barset = None
    idx = 0
    while idx <= len(symbols) - 1:
        if barset is None:
            barset = get_barset(symbols[idx:idx+200])
        else:
            barset.update(get_barset(symbols[idx:idx+200]))
        idx += 200

    return get_df(barset)



# Ranks all the stocks by the (price - EMA) difference.
# Diff is a difference between last price and 10-day EMA as a 
# Percentage of last price
# This value can be negative or positive, with a negative diff
# Indicating the price dropped recently.
def calc_scores(info_dict, dayindex=-1):
    diffs = {}
    params = 10
    for symbol in info_dict:
        df = info_dict[symbol]
        diff = []
        if len(df) <= 0:
            continue
        if len(df.Close.values) <= params:
            continue
        ema = df.Close.ewm(span=params).mean()
        # print("EMA for ", symbol, "\n", ema)
        last = df.Close.values[dayindex]
        # print(last)
        diff = (last - ema) / last
        # print(diff)
        diffs[symbol] = diff[1]
        # print(symbol, " : ", diff[1])

    # Print(sorted(diffs.items(), key=operator.itemgetter(1)))
    return sorted(diffs.items(), key=operator.itemgetter(1))





# Gets the stock information from the BarSet.
def get_df(symbols):
    #First getting array of dictionaries.
    diction = {}
    for s in symbols:
        d = []
        for stock in symbols.get(s):
            d.append({'Time':stock.t, 'Open':stock.o, 'High':stock.h, 'Low':stock.l, 'Close':stock.c, 'Volume':stock.v})
        # Converting List to a dataframe
        diction[s] = pd.DataFrame(d)
    return diction


def get_orders(api, price_df, position_size = 100, max_positions = 5):
    # Rank the stocks based on the indicators.
    ranked = calc_scores(price_df)
    to_buy = set()
    to_sell = set()
    account = api.get_account()

    # Take the top one twentieth out of ranking,
    # excluding stocks too expensive to buy a share
    for symbol in ranked[:len(ranked) // 20]:
        print(symbol)
        print(symbol[0])
        # print(price_df[symbol[0]])
        price = float(price_df[symbol[0]].Close.values[-1])
        print("Price of: ", symbol[0], " : ", price)
        if price > float(account.cash):
            continue
        to_buy.add(symbol[0])

    # Now getting the current positions and seeing what to buy,
    # What to sell jto transition to today's desired portfolio.
    positions = api.list_positions()
    # print("Positions: \n", positions)
    holdings = set()
    for p in positions:
        holdings.add(p)
    # print(holdings)
    holding_symbol = holdings
    to_sell = holding_symbol - to_buy
    # print("To Sell: ", to_sell)
    to_buy = to_buy - holding_symbol
    orders = []


    # if a stock is in the portfolio, and not in the desired
    # portfolio, sell it
    # TODO: Hasn't been tested yet.
    for symbol in to_sell:
        shares = holdings[symbol].qty
        orders.append({
            'symbol': symbol,
            'qty': shares,
            'side': 'sell',
        })
        logger.info(f'order(sell): {symbol} for {shares}')


    # likewise, if the portfolio is missing stocks from the
    # desired portfolio, buy them. We sent a limit for the total
    # position size so that we don't end up holding too many positions.
    max_to_buy = max_positions - (len(positions) - len(to_sell))
    for symbol in to_buy:
        if max_to_buy <= 0:
            break
        # Calculating if we can buy the stock.
        # Will buy the number shares
        shares = position_size // float(price_df[symbol].Close.values[-1])
        if shares == 0.0:
            continue
        orders.append({
            'symbol': symbol,
            'qty': shares,
            'side': 'buy',
        })
        print(f'order(buy): {symbol} for {shares}')
        max_to_buy -= 1
    return orders

def trade(orders, wait=30):
    #TODO: This hasn't been tested.
    '''This is where we actually submit the orders and wait for them to fill.
    Waiting is an important step since the orders aren't filled automatically,
    which means if your buys happen to come before your sells have filled,
    the buy orders will be bounced. In order to make the transition smooth,
    we sell first and wait for all the sell orders to fill before submitting
    our buy orders.
    '''

    # process the sell orders first
    sells = [o for o in orders if o['side'] == 'sell']
    for order in sells:
        try:
            print(f'submit(sell): {order}')
            api.submit_order(
                symbol=order['symbol'],
                qty=order['qty'],
                side='sell',
                type='market',
                time_in_force='day',
            )
        except Exception as e:
            logger.error(e)
    count = wait
    while count > 0:
        pending = api.list_orders()
        if len(pending) == 0:
            print(f'all sell orders done')
            break
        print(f'{len(pending)} sell orders pending...')
        time.sleep(1)
        count -= 1

    # process the buy orders next
    buys = [o for o in orders if o['side'] == 'buy']
    for order in buys:
        try:
            print(f'submit(buy): {order}')
            api.submit_order(
                symbol=order['symbol'],
                qty=order['qty'],
                side='buy',
                type='market',
                time_in_force='day',
            )
        except Exception as e:
            print(e)
    count = wait
    while count > 0:
        pending = api.list_orders()
        if len(pending) == 0:
            print(f'all buy orders done')
            break
        print(f'{len(pending)} buy orders pending...')
        time.sleep(1)
        count -= 1


if __name__ == '__main__':
    main()
        