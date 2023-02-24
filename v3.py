import signal
import requests
from time import sleep
import pandas as pd
from multiprocessing import Pool
import sys

shutdown = False
API_KEY = {'X-API-key': 'LO17Z6GL'}
securities = ["BULL", "BEAR"]
security_name = ""

# Spread should be dynamic and we should figure out why we start at 5 secs
SPEEDBUMP = 0.5
MAX_VOLUME = 5000
MAX_ORDERS = 5
SPREAD = 0.05


class ApiException(Exception):
    # handler for api errors
    pass


class MathException(Exception):
    # handler for mathematical discrepancies
    pass


# What does this do?
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True  # global var to be ref later


def main(security_name):
    def get_tick(session):
        obj = session.get('http://localhost:9999/v1/case')
        if obj.ok:
            case = obj.json()
            return case['tick']
        raise ApiException('tick -> api error')

    def bid_ask(session, ticker):
        payload = {'ticker': ticker}
        obj = session.get('http://localhost:9999/v1/securities/book', params=payload)
        if obj.ok:
            book = obj.json()
            return book['bids'][0]['price'], book['asks'][0]['price']
        raise ApiException('bid, ask -> api error')

    def get_open_sells(session):
        resp = session.get('http://localhost:9999/v1/orders?status=OPEN')
        data = []
        if resp.ok:
            orders = resp.json()
            for order in orders:
                if order['action'] == "SELL":
                    row = {}
                    row["order_id"] = order["order_id"]
                    row["volume_filled"] = order["quantity_filled"]
                    row["order_vol"] = order["quantity"]
                    row["price"] = order["price"]
                    data.append(row)

        df = pd.DataFrame(data)
        open_vol = 0
        try:
            open_vol = df["order_vol"].sum()
        except Exception as e:
            pass

        return open_vol, df

    def get_open_buys(session):
        resp = session.get('http://localhost:9999/v1/orders?status=OPEN')
        data = []
        if resp.ok:
            orders = resp.json()
            for order in orders:
                if order['action'] == "BUY":
                    row = {}
                    row["order_id"] = order["order_id"]
                    row["volume_filled"] = order["quantity_filled"]
                    row["order_vol"] = order["quantity"]
                    row["price"] = order["price"]
                    data.append(row)

        df = pd.DataFrame(data)
        open_vol = 0
        try:
            open_vol = df["order_vol"].sum()
        except Exception as e:
            pass

        return open_vol, df

    def buy_sell(session, sell_price, buy_price):
        try:
            for i in range(MAX_ORDERS):  # rate limit
                # sell
                session.post(
                    'http://localhost:9999/v1/orders',
                    params={
                        'ticker': security_name,
                        'type': 'LIMIT',
                        'quantity': MAX_VOLUME,
                        'price': sell_price,
                        'action': 'SELL'}
                )

                # buy
                session.post(
                    'http://localhost:9999/v1/orders',
                    params={
                        'ticker': security_name,
                        'type': 'LIMIT',
                        'quantity': MAX_VOLUME,
                        'price': buy_price,
                        'action': 'BUY'}
                )
        except Exception as e:
            raise ApiException(f'buy_sell -> {e}')

    def re_order(session, vols_filled, price, action, df):
        for i in range(len(df)):
            vol_filled = df.loc[i, "volume_filled"]
            volume = df.loc[i, "order_vol"]

            if vol_filled != 0:
                volume = MAX_VOLUME - vol_filled

            delete_obj = session.delete('http://localhost:9999/v1/orders/{}'.format(id))
            if delete_obj.ok:
                session.post(
                    'http://localhost:9999/v1/orders',
                    params={
                        'ticker': security_name,
                        'type': 'LIMIT',
                        'quantity': volume,
                        'price': price,
                        'action': action
                    }
                )

    volume_filled_buys = []
    open_buys_volume = 0

    volume_filled_sells = []
    open_sells_volume = 0

    single_side_filled = False
    single_side_transaction_time = 0

    with requests.Session() as s:
        s.headers.update(API_KEY)
        tick = get_tick(s)

        while (tick > 5) and (tick < 295) and not shutdown:
            open_sells, sells_df = get_open_sells(s)
            open_buys, buys_df = get_open_buys(s)
            bid, ask = bid_ask(s, security_name)

            if (open_sells_volume == 0) and (open_buys_volume == 0):
                # no open orders, so go through with arbitrage
                single_side_filled = False
                spread = ask - bid
                sell_price = ask
                buy_price = bid

                if spread >= SPREAD:
                    buy_sell(s, sell_price, buy_price)
                    sleep(SPEEDBUMP)  # speedbump should be dynamic

            else:
                if not single_side_filled and (open_buys_volume == 0 or open_sells_volume == 0):
                    single_side_filled = True
                    single_side_transaction_time = tick

                if open_sells_volume == 0:
                    if buy_price == bid:  # orders at top of block
                        continue

                    elif tick - single_side_transaction_time >= 3:  # threshold should be dynamic
                        next_buy_price = bid + 0.01
                        potential_profit = sell_price - next_buy_price - 0.02

                        if potential_profit >= 0.01 or tick - single_side_transaction_time >= 6:  # Why are  they choosing these random times?
                            action = 'BUY'
                            buy_price = bid + 0.01
                            price = buy_price
                            volumes_filled = volume_filled_buys

                            re_order(s, volumes_filled, price, action, buys_df)
                            sleep(SPEEDBUMP)

                elif open_buys_volume == 0:
                    if sell_price == ask:
                        continue

                    elif tick - single_side_transaction_time >= 3:  # threshold should by dynamic
                        next_sell_price = ask - 0.01
                        potential_profit = next_sell_price - buy_price - 0.02

                        if potential_profit >= 0.01 or tick - single_side_transaction_time >= 6:  # Why are  they choosing these random times?
                            action = 'SELL'
                            sell_price = ask - 0.01
                            price = sell_price
                            volumes_filled = volume_filled_sells

                            re_order(s, volumes_filled, price, action, sells_df)
                            sleep(SPEEDBUMP)

            tick = get_tick(s)  # update sessions




if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    with Pool() as pool:
        result = pool.map(main,securities)

