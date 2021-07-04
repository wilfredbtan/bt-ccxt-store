#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015, 2016, 2017 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import json
import threading
import time

from termcolor import colored
from datetime import datetime
from functools import wraps
from pprint import pprint

from backtrader import BrokerBase, OrderBase, Order
from backtrader.position import Position
from backtrader.utils.py3 import queue, with_metaclass

from ccxt.base.errors import NetworkError, ExchangeError

from .ccxtstore import CCXTStore


class CCXTOrder(OrderBase):
    def __init__(self, owner, data, ccxt_order):
        self.owner = owner
        self.data = data
        self.ccxt_order = ccxt_order
        self.executed_fills = []
        self.ordtype = self.Buy if ccxt_order['side'] == 'buy' else self.Sell
        self.size = float(ccxt_order['amount'])

        super(CCXTOrder, self).__init__()


class MetaCCXTBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaCCXTBroker, cls).__init__(name, bases, dct)
        CCXTStore.BrokerCls = cls


class CCXTBroker(with_metaclass(MetaCCXTBroker, BrokerBase)):
    '''Broker implementation for CCXT cryptocurrency trading library.
    This class maps the orders/positions from CCXT to the
    internal API of ``backtrader``.

    Broker mapping added as I noticed that there differences between the expected
    order_types and retuned status's from canceling an order

    Added a new mappings parameter to the script with defaults.

    Added a get_balance function. Manually check the account balance and update brokers
    self.cash and self.value. This helps alleviate rate limit issues.

    Added a new get_wallet_balance method. This will allow manual checking of the any coins
        The method will allow setting parameters. Useful for dealing with multiple assets

    Modified getcash() and getvalue():
        Backtrader will call getcash and getvalue before and after next, slowing things down
        with rest calls. As such, th

    The broker mapping should contain a new dict for order_types and mappings like below:

    broker_mapping = {
        'order_types': {
            bt.Order.Market: 'market',
            bt.Order.Limit: 'limit',
            bt.Order.Stop: 'stop-loss', #stop-loss for kraken, stop for bitmex
            bt.Order.StopLimit: 'stop limit'
        },
        'mappings':{
            'closed_order':{
                'key': 'status',
                'value':'closed'
                },
            'canceled_order':{
                'key': 'result',
                'value':1}
                }
        }

    Added new private_end_point method to allow using any private non-unified end point

    '''

    order_types = {Order.Market: 'market',
                   Order.Limit: 'limit',
                   Order.Stop: 'stop',  # stop-loss for kraken, stop for bitmex
                   Order.StopLimit: 'stop limit'}

    mappings = {
        'closed_order': {
            'key': 'status',
            'value': 'closed'
        },
        'canceled_order': {
            'key': 'status',
            'value': 'canceled'}
    }

    (TRADE, FILLED, PARTIALLY_FILLED, NEW, CANCELED, BUY, SELL) = ('TRADE', 'FILLED', 'PARTIALLY_FILLED', 'NEW', 'CANCELED', 'BUY', 'SELL')

    def __init__(self, broker_mapping=None, debug=False, **kwargs):
        super(CCXTBroker, self).__init__()

        if broker_mapping is not None:
            try:
                self.order_types = broker_mapping['order_types']
            except KeyError:  # Might not want to change the order types
                pass
            try:
                self.mappings = broker_mapping['mappings']
            except KeyError:  # might not want to change the mappings
                pass

        self.store = CCXTStore(**kwargs)

        self.currency = self.store.currency

        self.positions = collections.defaultdict(Position)

        self.debug = debug
        self.indent = 4  # For pretty printing dictionaries

        self.notifs = queue.Queue()  # holds orders which are notified

        # self.open_orders = list()
        self.open_orders = {}

        self.get_balance()
        self.startingcash = self.store._cash
        self.startingvalue = self.store._value

        self._lock_orders = threading.Lock()  # control access
    
    def start(self):
        super(CCXTBroker, self).start()
        self.store.start(broker=self)

    def stop(self):
        super(CCXTBroker, self).stop()
        self.get_balance()
        self.store.stop()

    def get_balance(self):
        self.store.get_balance()
        self.cash = self.store._cash
        self.value = self.store._value
        return self.cash, self.value

    def get_wallet_balance(self, currency, params={}):
        balance = self.store.get_wallet_balance(currency, params=params)
        cash = balance['free'][currency] if balance['free'][currency] else 0
        value = balance['total'][currency] if balance['total'][currency] else 0
        return cash, value

    def getcash(self):
        # Get cash seems to always be called before get value
        # Therefore it makes sense to add getbalance here.
        # return self.store.getcash(self.currency)
        # self.store.get_balance()
        self.cash = self.store._cash
        return self.cash

    def getvalue(self, datas=None):
        # return self.store.getvalue(self.currency)
        self.value = self.store._value
        return self.value

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            return None

    def notify(self, order):
        self.notifs.put(order.clone())
        # self.notifs.put(order)

    def getposition(self, data, clone=True):
        # return self.o.getposition(data._dataname, clone=clone)
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()
        return pos
    

    def get_order_trades(self, order_id, symbol):
        '''
        Trade Structure
        {
            'amount': 0.1, 
            'cost': 3468.67, 
            'datetime': '2021-07-01T08:56:12.103Z', 
            'fee': {'cost': 1.387468, 'currency': 'USDT'}, 
            'id': '185368459', 
            'info': {
                'buyer': False, 
                'commission': '1.38746800', 
                'commissionAsset': 'USDT',
                'id': '185368459',
                'maker': False,
                'marginAsset': 'USDT',
                'orderId': '2731575134',
                'positionSide': 'BOTH',
                'price': '34686.70',
                'qty': '0.100',
                'quoteQty': '3468.67000',
                'realizedPnl': '-0.04092333',
                'side': 'SELL',
                'symbol': 'BTCUSDT',
                'time': '1625129772103'
            },
            'order': '2731575134',
            'price': 34686.7,
            'side': 'sell',
            'symbol': 'BTC/USDT',
            'takerOrMaker': 'taker',
            'timestamp': 1625129772103,
            'type': None
        }
        '''
        ccxt_my_trades = self.store.fetch_my_trades(symbol)
        # order_trades = list(filter(lambda d: d['order'] in ccxt_my_trades))
        order_trades = [d for d in ccxt_my_trades if d['order'] == order_id]
        return order_trades
    
    def next(self):
        self.notifs.put(None)
        if self.debug:
            print('Broker next() called')

    def _submit(self, owner, data, exectype, side, amount, price, params):
        with self._lock_orders:
            order_type = self.order_types.get(exectype) if exectype else 'market'
            created = int(data.datetime.datetime(0).timestamp()*1000)
            # Extract CCXT specific params if passed to the order
            params = params['params'] if 'params' in params else params
            params['created'] = created  # Add timestamp of order creation for backtesting
            ret_ord = self.store.create_order(symbol=data.p.dataname, order_type=order_type, side=side,
                                            amount=amount, price=price, params=params)

            _order = self.store.fetch_order(ret_ord['id'], data.p.dataname)

            order = CCXTOrder(owner, data, _order)
            order.price = ret_ord['price']

            self.open_orders[order.ccxt_order['id']] = order

            self.notify(order)
            return order

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        del kwargs['parent']
        del kwargs['transmit']
        print("======= BUY SIZE: ", size)
        return self._submit(owner, data, exectype, 'buy', size, price, kwargs)

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        del kwargs['parent']
        del kwargs['transmit']
        print("======= SELL SIZE: ", size)
        return self._submit(owner, data, exectype, 'sell', size, price, kwargs)

    def cancel(self, order):

        with self._lock_orders:
            oID = order.ccxt_order['id']

            if self.debug:
                print('Broker cancel() called')
                print('Fetching Order ID: {}'.format(oID))

            # check first if the order has already been filled otherwise an error
            # might be raised if we try to cancel an order that is not open.
            ccxt_order = self.store.fetch_order(oID, order.data.p.dataname)

            if self.debug:
                print(json.dumps(ccxt_order, indent=self.indent))

            if ccxt_order[self.mappings['closed_order']['key']] == self.mappings['closed_order']['value']:
                return order

            ccxt_order = self.store.cancel_order(oID, order.data.p.dataname)

            if self.debug:
                print(json.dumps(ccxt_order, indent=self.indent))
                print('Value Received: {}'.format(ccxt_order[self.mappings['canceled_order']['key']]))
                print('Value Expected: {}'.format(self.mappings['canceled_order']['value']))

            if ccxt_order[self.mappings['canceled_order']['key']] == self.mappings['canceled_order']['value']:
                self.open_orders.pop(oID)
                order.cancel()
                self.notify(order)

            return order

    def get_orders_open(self, safe=False):
        return self.store.fetch_open_orders()

    def private_end_point(self, type, endpoint, params):
        '''
        Open method to allow calls to be made to any private end point.
        See here: https://github.com/ccxt/ccxt/wiki/Manual#implicit-api-methods

        - type: String, 'Get', 'Post','Put' or 'Delete'.
        - endpoint = String containing the endpoint address eg. 'order/{id}/cancel'
        - Params: Dict: An implicit method takes a dictionary of parameters, sends
          the request to the exchange and returns an exchange-specific JSON
          result from the API as is, unparsed.

        To get a list of all available methods with an exchange instance,
        including implicit methods and unified methods you can simply do the
        following:

        print(dir(ccxt.hitbtc()))
        '''
        endpoint_str = endpoint.replace('/', '_')
        endpoint_str = endpoint_str.replace('{', '')
        endpoint_str = endpoint_str.replace('}', '')

        method_str = 'private_' + type.lower() + endpoint_str.lower()

        return self.store.private_end_point(type=type, endpoint=method_str, params=params)


    # Mapping of message: https://binance-docs.github.io/apidocs/futures/en/#event-order-update
    def push_trade_message(self, msg):
        '''
        CCXT Order Structure
        {'amount': 0.1,
        'average': 34687.4,
        'clientOrderId': 'x-xcKtGhcudb397e43d4127eacb1f16d',
        'cost': 3468.74,
        'datetime': '2021-07-01T09:33:42.659Z',
        'fee': None,
        'fees': [],
        'filled': 0.1,
        'id': '2731578280',
        'info': {'avgPrice': '34687.40000',
                'clientOrderId': 'x-xcKtGhcudb397e43d4127eacb1f16d',
                'closePosition': False,
                'cumQuote': '3468.74000',
                'executedQty': '0.100',
                'orderId': '2731578280',
                'origQty': '0.100',
                'origType': 'MARKET',
                'positionSide': 'BOTH',
                'price': '0',
                'priceProtect': False,
                'reduceOnly': False,
                'side': 'BUY',
                'status': 'FILLED',
                'stopPrice': '0',
                'symbol': 'BTCUSDT',
                'time': '1625132022659',
                'timeInForce': 'GTC',
                'type': 'MARKET',
                'updateTime': '1625132022659',
                'workingType': 'CONTRACT_PRICE'},
        'lastTradeTimestamp': None,
        'postOnly': False,
        'price': 34687.4,
        'remaining': 0.0,
        'side': 'buy',
        'status': 'closed',
        'stopPrice': None,
        'symbol': 'BTC/USDT',
        'timeInForce': 'GTC',
        'timestamp': 1625132022659,
        'trades': [...], # Fetched in fetch_my_trades()
        'type': 'market'}
        '''
        # print("===== trade message received in broker")
        # pprint(msg)

        with self._lock_orders:
            pushed_order = msg['o']
            # E.g. FILLED, PARTIALLY_FILLED or NEW
            status = pushed_order['X']
            # E.g. TRADE or NEW
            exec_type = pushed_order['x']

            order_id = str(pushed_order['i'])

            if order_id not in self.open_orders:
                if self.debug:
                    print("===== Invalid order id:, ", order_id)
                return

            if self.debug:
                print("====== Execute: ", status)

            order = self.open_orders[order_id]

            if exec_type != self.TRADE:
                if self.debug:
                    print("===== Non-trade order of type: ", exec_type)
                return

            if status != self.FILLED and status != self.PARTIALLY_FILLED:
                if self.debug:
                    print("====== Neither filled nor partially filled. staus: ", status)
                return

            data = order.data
            pos = self.getposition(data, clone=False)

            side = pushed_order['S']
            size = float(pushed_order['l']) if side == self.BUY else -float(pushed_order['l'])
            price = float(pushed_order['L'])

            # psize, pprice, opened, closed = pos.update(order.size, order.price)
            psize, pprice, opened, closed = pos.update(size, price)
            if self.debug:
                txt = colored(f"position in push_trade_message: ", 'red')
                print(txt)
                print(self.getposition(order.data, clone=False))

            comminfo = self.getcommissioninfo(order.data)

            if closed:
                closedvalue = float(pushed_order['L']) * closed
                if 'n' in pushed_order:
                    closedcomm = float(pushed_order['n'])
                else:
                    closedcomm = 0
            else:
                closedvalue = closedcomm = 0
            
            if opened:
                openedvalue = float(pushed_order['L']) * opened
                if 'n' in pushed_order:
                    openedcomm = float(pushed_order['n'])
                else:
                    openedcomm = 0
            else:
                openedvalue = openedcomm = 0

            if status == self.PARTIALLY_FILLED:
                order.partial()
            elif status == self.FILLED:
                order.completed()

            ccxt_order_trades = self.get_order_trades(order_id, order.data.p.dataname)
            order.ccxt_order['trades'] = ccxt_order_trades

            margin = 0
            if comminfo.margin:
                margin = comminfo['margin'] if comminfo['margin'] is not None else 0

            execsize = closed + opened
            # Realized profit
            pnl = float(pushed_order['rp'])

            if self.debug:
                print("====== Pushed Order in push trade message =======")
                pprint(pushed_order)

                print("====== order exec =======")
                print("psize", psize)
                print("pprice", pprice)
                print("opened", opened)
                print("closed", closed)
                print("execsize (opened+closed): ", execsize)
                print("amount", order.ccxt_order['amount'])

            '''
            def execute(self, dt, size, price,
            closed, closedvalue, closedcomm,
            opened, openedvalue, openedcomm,
            margin, pnl,
            psize, pprice):
            '''
            order.execute(
                order.ccxt_order['datetime'], 
                # order['amount'], 
                execsize,
                # order.ccxt_order['price'], 
                price,
                closed, closedvalue, closedcomm, 
                opened, openedvalue, openedcomm, 
                margin, pnl,
                psize, pprice
            )
            order.addcomminfo(comminfo)
            self.notify(order)
            
            self.get_balance()

