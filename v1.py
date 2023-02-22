import signal
import requests
from time import sleep
import sys


shutdown = False


class ApiException(Exception):
    # handler for api errors
	pass


class MathException(Exception):
	# handler for mathematical discrepancies
	pass


def signal_handler(signum, frame):
	global shutdown
	signal.signal(signal.SIGHT, signal.SIG_DFL)
	shutdown = True                              # global var to be ref later


SPEEDBUMP = 0.5
MAX_VOLUME = 5000
MAX_ORDERS = 5
SPREAD = 0.5


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


def open_sells(session):
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


def open_buys(session):
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
	for i in range(MAX_ORDERS):	 # rate limit

















