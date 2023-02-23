import signal
import requests
from time import sleep
import sys

shutdown = False
API_KEY = {'X-API-key': 'TCDVA40Y'}


class ApiException(Exception):
    # handler for api errors
    pass


class MathException(Exception):
    # handler for mathematical discrepancies
    pass


def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True  # global var to be ref later


SPEEDBUMP = 0.5
MAX_VOLUME = 5000
MAX_ORDERS = 5
SPREAD = 0.05


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
    if resp.ok:
        open_vol = 0
        ids = []
        prices = []
        order_vols = []
        volume_filled = []

        orders = resp.json()
        for order in orders:
            if order['action'] == "SELL":
                volume_filled.append(order['quantity_filled'])
                order_vols.append(order['quantity'])
                open_vol += order['quantity']
                prices.append(order['price'])
                ids.append(order['order_id'])

    return volume_filled, open_vol, ids, prices, order_vols


def get_open_buys(session):
    resp = session.get('http://localhost:9999/v1/orders?status=OPEN')
    if resp.ok:
        open_vol = 0
        ids = []
        prices = []
        order_vols = []
        volume_filled = []

        orders = resp.json()
        for order in orders:
            if order['action'] == "BUY":
                volume_filled.append(order['quantity_filled'])
                order_vols.append(order['quantity'])
                open_vol += order['quantity']
                prices.append(order['price'])
                ids.append(order['order_id'])

    return volume_filled, open_vol, ids, prices, order_vols


def buy_sell(session, sell_price, buy_price):
    try:
        for i in range(MAX_ORDERS):  # rate limit
            # sell
            session.post(
                'http://localhost:9999/v1/orders',
                params={
                    'ticker': 'BULL',
                    'type': 'LIMIT',
                    'quantity': MAX_VOLUME,
                    'price': sell_price,
                    'action': 'SELL'}
            )

            # buy
            session.post(
                'http://localhost:9999/v1/orders',
                params={
                    'ticker': 'BULL',
                    'type': 'LIMIT',
                    'quantity': MAX_VOLUME,
                    'price': buy_price,
                    'action': 'BUY'}
            )
    except Exception as e:
        raise ApiException(f'buy_sell -> {e}')


def re_order(session, num_orders, ids, vols_filled, volumes, price, action):
    for i in range(num_orders):
        id = ids[i]
        volume = volumes[i]
        vol_filled = vols_filled[i]

        if vol_filled != 0:
            volume = MAX_VOLUME - vol_filled

        delete_obj = session.delete('http://localhost:9999/v1/orders/{}'.format(id))
        if delete_obj.ok:
            session.post(
                'http://localhost:9999/v1/orders',
                params={
                    'ticker': 'BULL',
                    'type': 'LIMIT',
                    'quantity': volume,
                    'price': price,
                    'action': action
                }
            )


def main():
    buy_ids = []
    buy_prices = []
    buy_volumes = []
    volume_filled_buys = []
    open_buys_volume = 0

    sell_ids = []
    sell_prices = []
    sell_volumes = []
    volume_filled_sells = []
    open_sells_volume = 0

    single_side_filled = False
    single_side_transaction_time = 0

    with requests.Session() as s:
        s.headers.update(API_KEY)
        tick = get_tick(s)

        while (tick > 5) and (tick < 295) and not shutdown:
            volume_sells, open_sells, sell_ids, sell_prices, sell_volumes = get_open_sells(s)
            volume_buys, open_buys, buy_ids, buy_prices, buy_volumes = get_open_buys(s)
            bid, ask = bid_ask(s, 'BULL')

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

                    elif tick - single_side_transaction_time >= 3:  # threshold should by dynamic
                        next_buy_price = bid + 0.01
                        potential_profit = sell_price - next_buy_price - 0.02

                        if potential_profit >= 0.01 or tick - single_side_transaction_time >= 6:  # Why are  they choosing these random times?
                            action = 'BUY'
                            order_count = len(buy_ids)
                            buy_price = bid + 0.01
                            price = buy_price
                            ids = buy_ids
                            volumes = buy_volumes
                            volumes_filled = volume_filled_buys

                            re_order(s, order_count, ids, volumes_filled, volumes, price, action)
                            sleep(SPEEDBUMP)

                elif open_buys_volume == 0:
                    if sell_price == ask:
                        continue

                    elif tick - single_side_transaction_time >= 3:  # threshold should by dynamic
                        next_sell_price = ask - 0.01
                        potential_profit = next_sell_price - buy_price - 0.02

                        if potential_profit >= 0.01 or tick - single_side_transaction_time >= 6:  # Why are  they choosing these random times?
                            action = 'SELL'
                            order_count = len(sell_ids)
                            sell_price = ask - 0.01
                            price = sell_price
                            ids = sell_ids
                            volumes = sell_volumes
                            volumes_filled = volume_filled_sells

                            re_order(s, order_count, ids, volumes_filled, volumes, price, action)
                            sleep(SPEEDBUMP)

            tick = get_tick(s)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()
