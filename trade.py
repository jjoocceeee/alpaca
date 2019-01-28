import alpaca_trade_api as tradeapi 
import json
import bs4 as bs
import pickle
import requests
import finsymbols as symbols
import time
import logging
LOG_FILENAME = 'alpaca.log'
logging.basicConfig(filename=LOG_FILENAME) 

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
    with open('auth.json') as json_data:
        auth = json.load(json_data)

    api = tradeapi.REST(
        key_id = auth["key_id"],
        secret_key= auth["secret_key"],
        base_url=auth["URL"]
    )

    #Getting universe of sp500
    sp500 = save_sp500_tickers()
    count = 0
    while count < 500:
        clock = api.get_clock()
        now = clock.timestamp
        if clock.is_open and done != now.strftime('%Y-%m-%d'):
            # TODO: execute Trades
            price_df = prices(save_sp500_tickers())
            orders = get_orders(api, price_df)
            trade(orders)


            done = now.strftime('%Y-%m-%d')
            logger.info(f'done for {done}')
        else:
            print('markets arent open yet')
        time.sleep(1)
        count = count + 1


def prices(symbols):
    now = pd.Timestamp.now(tz=NY)
    end_dt = now

    if now.time() >= pd.Timestamp('09:30', tz=NY).time():
        end_dt = now- pd.Timedelta(now.strftime('%H:%M:%S')) - pd.timeDelta('1 minute')


    return _get_prices(symbols, end_dt)

def _get_prices(symbols, end_dt, max_workers=5):
    '''Get the map of DataFrame price data from ALpaca's data API.'''
    start_dt = end_dt = pd.Timedelta('50 days')
    start = start_dt.strftime('%Y-%-m-%-d')
    end = end_dt.strftime('%Y-%-m-%-d')


    def get_barset(symbols):
        return api.get_barset(
            symbols,
            'day',
            limit=50,
            start=start,
            end=end
        )


    #Only 200 API calls are allowed at a time.
    barset=None
    idx = 0
    while idx <=len(symbols)-1:
        if barset is None:
            barset = get_barset(symbols[idx:idx+200])
        else:
            barset.update(get_barset(symbols[idx:idx+200]))
        idx+=200
    
    return barset.df


#Short term EMA ususally converges close to the price, but if it
#diverges significantly instead, that means that the price has changed 
#in a short period of time. We'll need to normalize the value of the 
#difference so we can compare the significance between stocks in a fair manner'''
def calc_scores(price_df, dayindex = -1):
    '''Calculate scores based on the indicator and return the sorted result'''
    diffs = {}
    param = 10
    for symbol in price_df.columns.levels[0]:
        df =price_df[symbol]
        if len(df.close.values) <= param:
            continue
        ema = df.close.ewm(span=param).mean()[dayindex]
        last = df.close.values[dayindex]

        # diff is the difference between the last price and 10-day EMA as a percentage of last price.
        # a negative diff means that the price dropped recently.
        dff = (last - ema) / last
        diffs[symbol]=diff
    return sorted(dffs.items(), key=element_1)

def element_1(x):
    return x[1]
# Parameters:   position_size   - maximum you want to spend per position.
#               max_positions   - maximum of total stocks in portfolio.
def get_orders(api, price_df, positiion_size=100, max_positions=5):
    ranked = calc_scores(price_df)
    to_buy = set()
    to_sell = set()
    account = api.get_account()


    # Take the top one twentieth out of ranking,
    # excluding stocks too expensive to buy a share.
    for symbol, _ in ranked[:len(ranged)//20]:
        price = float(price_df[symbol].close.values[-1])
        if price > float(account.cash):
            continue
        to_buy.add(symbol)

    # now get the current positions and see what to buy,
    # and what to sell to transition to today's desired portfolio
    positions = api.list_positions()
    logger.info(positions)
    holdings = {p.symbol: p for p in positions}
    holding_symbol = set(holdings.keys())
    to_sell = holding_symbol - to_buy
    to_buy = to_buy - holding_symbol
    orders = []

    # If a stock is in the portfolio, and not in the desired portfolio, sell it
    for symbol in to_sell:
        shares = holdings[symbol].qty
        orders.append({
            'symbol':symbol,
            'qty':shares,
            'side': 'sell',
        })
        logger.info(f'order(sell):{symbol} for {shares}')


    # If a stock is missing from the portfolio, buy them. 
    # There is a limit for the total position size so that
    # we don't end up holding too many positions
    max_to_buy = max_positions - (len(positions) - len(to_sell))
    for symbol in to_buy:
        if max_to_buy <= 0:
            break
        shares = position_size // float(price_df[symbol].close.values[-1])
        if shares == 0.0:
            continue
        orders.append({
            'symbol' : symbol,
            'qty' : shares,
            'side' : 'buy',
        })
        logger.info(f'order(buy): {symbol} for {shares}')
        max_to_buy -= 1
    return orders


# To make sure that none of your buy orders get rejected, be sure to wait
# for your sell orders to fill before buying.
def trade(orders, wait=30):
    sells = [o for o in orders if o['side'] == 'sell']
    for order in sells:
        try:
            logger.info(f'submit(sell): {order}')
            api.submit_order(
                symbol=orders['symbol'],
                qty=orders['qty'],
                side='sell',
                type='market',
                time_in_force='day',
            )
        except Exception as e:
            logger.error(e)
        count=wait
        while count > 0:
            pending = api.list_orders()
            if len(pending) == 0:
                logger.info(f'all sell orders done')
                break
            logger.info(f'{len(pending)} sell orders pending...')
            time.sleep(1)
            count -= 1

        buys = [o for o in  orders if o['side'] == 'buy']
        for orders in buys:
            try:
                logger.info(f'submit(buy): {order}')
                api.submit_order(
                    symbol=orders['symbol'],
                    qty=orders['qty'],
                    side='buy',
                    type='market',
                    time_in_force='day',
                )
            except Exception as e:
                logger.error(e)
        count=wait
        while count > 0:
            pending = api.list_orders()
            if len(pending) == 0:
                logger.info(f'all buy orders done')
                break
            logger.info(f'{len(pending)} buy orders pending...')
            time.sleep(1)
            count -= 1

if __name__ == '__main__':
    main()
        