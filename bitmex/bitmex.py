"""BitMEX API Connector."""
from __future__ import absolute_import

import hashlib
import hmac
# python  3+ and 2+
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import requests
import time
import datetime
import json
import logging

from requests.auth import AuthBase

PROTOCOL = "https"
HOST = "www.bitmex.com/api"
VERSION = "v1"
CONSTANT = 100000000
BASE_URL = 'https://www.bitmex.com/api/v1/'
SYMBOL = 'XBTUSD'


class APIKeyAuth(AuthBase):
    """Attaches API Key Authentication to the given Request object."""

    def __init__(self, apiKey, apiSecret):
        """Init with Key & Secret."""
        self.apiKey = apiKey
        self.apiSecret = apiSecret

    def __call__(self, r):
        """Called when forming a request - generates api key headers."""
        # modify and return the request
        nonce = generate_nonce()
        r.headers['api-nonce'] = str(nonce)
        r.headers['api-key'] = self.apiKey
        r.headers['api-signature'] = generate_signature(self.apiSecret, r.method, r.url, nonce, r.body or '')

        return r


def generate_nonce():
    return int(round(time.time() * 10000))


def generate_signature(secret, verb, url, nonce, data):
    """Generate a request signature compatible with BitMEX.

    Generates an API signature.
    A signature is HMAC_SHA256(secret, verb + path + nonce + data), hex encoded.
    Verb must be uppercased, url is relative, nonce must be an increasing 64-bit integer
    and the data, if present, must be JSON without whitespace between keys.

    For example, in psuedocode (and in real code below):

    verb=POST
    url=/api/v1/order
    nonce=1416993995705
    data={"symbol":"XBTZ14","quantity":1,"price":395.01}
    signature = HEX(HMAC_SHA256(secret, 'POST/api/v1/order1416993995705{"symbol":"XBTZ14","quantity":1,"price":395.01}'))
"""
    # Parse the url so we can remove the base and extract just the path.
    parsedURL = urlparse(url)
    path = parsedURL.path
    if parsedURL.query:
        path = path + '?' + parsedURL.query

    if isinstance(data, (bytes, bytearray)):
        data = data.decode('utf8')

    # print "Computing HMAC: %s" % verb + path + str(nonce) + data
    message = verb + path + str(nonce) + data

    signature = hmac.new(bytes(secret, 'utf8'), bytes(message, 'utf8'), digestmod=hashlib.sha256).hexdigest()
    return signature


class APIKeyAuthWithExpires(AuthBase):
    """Attaches API Key Authentication to the given Request object. This implementation uses `expires`."""

    def __init__(self, apiKey, apiSecret):
        """Init with Key & Secret."""
        self.apiKey = apiKey
        self.apiSecret = apiSecret

    def __call__(self, r):
        """
        Called when forming a request - generates api key headers. This call uses `expires` instead of nonce.

        This way it will not collide with other processes using the same API Key if requests arrive out of order.
        For more details, see https://www.bitmex.com/app/apiKeys
        """
        # modify and return the request
        expires = int(round(time.time()) + 5)  # 5s grace period in case of clock skew
        r.headers['api-expires'] = str(expires)
        r.headers['api-key'] = self.apiKey
        r.headers['api-signature'] = generate_signature(self.apiSecret, r.method, r.url, expires, r.body or '')

        return r


class Client:
    def __init__(self):
        self.logger = logging.getLogger('root')
        self.base_url = BASE_URL
        self.symbol = SYMBOL
        self.retries = 0  # initialize counter

        # Prepare HTTPS session
        self.session = requests.Session()
        # These headers are always sent
        self.session.headers.update({'user-agent': 'liquidbot-1'})
        self.session.headers.update({'content-type': 'application/json'})
        self.session.headers.update({'accept': 'application/json'})

    def server(self):
        return u"{0:s}://{1:s}/{2:s}".format(PROTOCOL, HOST, VERSION)

    #
    # Public methods
    #
    def symbols(self):
        """
        curl -X GET --header 'Accept: application/json' 'https://www.bitmex.com/api/v1/instrument/active'
        :return:[XBTUSD,..] list of symbols
        """
        endpoint = 'instrument/active'
        data = self._curl_bitmex(path=endpoint, verb="GET")
        symbols = list(map(lambda i: i['symbol'], data))
        return symbols

    def ticker(self, symbol=None):
        """Get ticker data."""
        if symbol is None:
            symbol = self.symbol
        endpoint = 'instrument'
        postdict = {'symbol': symbol}
        instrument = self._curl_bitmex(path=endpoint, postdict=postdict, verb="GET")[0]
        # If this is an index, we have to get the data from the last trade.
        if instrument['symbol'][0] == '.':
            ticker = {}
            ticker['mid'] = ticker['buy'] = ticker['sell'] = ticker['last'] = instrument['markPrice']
        # Normal instrument
        else:
            bid = instrument['bidPrice'] or instrument['lastPrice']
            ask = instrument['askPrice'] or instrument['lastPrice']
            ticker = {
                "last": instrument['lastPrice'],
                "buy": bid,
                "sell": ask,
                "mid": (bid + ask) / 2
            }
        return ticker

    def instrument(self, symbol):
        """Get an instrument's details."""

        endpoint = 'instrument'
        postdict = {'symbol': symbol}
        return self._curl_bitmex(path=endpoint, postdict=postdict, verb="GET")[0]

    def today(self, symbol):
        """
        {"low":"550.09","high":"572.2398","volume":"7305.33119836"}
        """
        endpoint = 'instrument'
        postdict = {'symbol': symbol}

        data = self._curl_bitmex(path=endpoint, postdict=postdict, verb="GET")[0]

        return {
            "low": data['lowPrice'],
            "high": data['highPrice'],
            "volume": data['volume']
        }

    def order_book(self, symbol, depth=25):
        """Get market depth / orderbook.
        [
              {
                "symbol": "string",
                "id": 0,
                "side": "string",
                "size": 0,
                "price": 0
              },....
        ]
        """
        endpoint = 'orderBook/L2'
        postdict = {
            'symbol': symbol,
            'depth': depth
        }
        return self._curl_bitmex(path=endpoint, postdict=postdict, verb="GET")

    def recent_trades(self, symbol):
        """Get recent trades.

        Returns
        -------
        A list of dicts:
               {
                "timestamp": "2017-01-01T00:00:44.952Z",
                "symbol": "XBTUSD",
                "side": "Buy",
                "size": 25,
                "price": 968.74,
                "tickDirection": "PlusTick",
                "trdMatchID": "9453668a-aeb0-502e-54e1-e24cc692d2c2",
                "grossValue": 2580675,
                "homeNotional": 0.02580675,
                "foreignNotional": 25
              },

        """
        endpoint = 'trade'
        postdict = {
            'symbol': symbol
        }

        return self._curl_bitmex(path=endpoint, postdict=postdict, verb="GET")


# https://www.bitmex.com/api/explorer/
class TradeClient(Client):
    """BitMEX API Connector."""

    def __init__(self, acc ):
        """Init connector."""

        self.apiKey = acc.apiKey
        self.apiSecret = acc.apiSecret
        self.client = Client()

        # Create websocket for streaming data
        # self.ws = BitMEXWebsocket()
        # self.ws.connect(base_url, symbol, shouldAuth=shouldWSAuth)

    #
    # Authentication required methods
    #
    def authentication_required(fn):
        """Annotation for methods that require auth."""

        def wrapped(self, *args, **kwargs):
            if not (self.apiKey):
                msg = "You must be authenticated to use this method"
                raise msg
            else:
                return fn(self, *args, **kwargs)

        return wrapped

    @authentication_required
    def balances(self):
        """Get your current balance.
        list of currency with balance that you have
        {currency: XBt, marginbalance:41937127}
        """
        endpoint = 'user/margin'
        postdict = {
            'currency': 'XBt'
        }
        data = self._curl_bitmex_private(path=endpoint, postdict=postdict, verb="GET", private=True)
        # print(data)
        if isinstance(data, dict):
            return {'currency': data['currency'], 'marginBalance': data['marginBalance'],
                    'availableMargin': data['availableMargin']}

    def Xbt_to_XBT(self, xbt):
        return xbt / CONSTANT

    @authentication_required
    def position(self):
        """Get your open position.
        return a list of positions"""
        endpoint = 'position'

        return self._curl_bitmex_private(path=endpoint, verb="GET", private=True)

    @authentication_required
    def close_position(self, symbol, price=None):
        """close position if price is given then position close at that price otherwise close @ market price"""
        endpoint = 'order/closePosition'
        postdict = {'symbol': symbol}
        if price:
            postdict.update({'price': price})

        return self._curl_bitmex_private(path=endpoint, postdict=postdict, verb="POST", private=True)

    @authentication_required
    def isolate_margin(self, symbol, leverage, rethrow_errors=False):
        """Set the leverage on an isolated margin position"""
        path = "position/leverage"
        postdict = {
            'symbol': symbol,
            'leverage': leverage
        }
        return self._curl_bitmex_private(path=path, postdict=postdict, verb="POST", rethrow_errors=rethrow_errors, private=True)

    @authentication_required
    def history(self, symbol=None):
        """

        :param symbol:
        :return:
        order history list if currency provided then for that currency otherwise for show all orders
        """
        endpoint = 'execution/tradeHistory'
        if symbol:
            postdict = {'symbol': symbol}
            return self._curl_bitmex_private(path=endpoint, postdict=postdict, verb="GET", private=True)
        return self._curl_bitmex_private(path=endpoint, verb="GET", private=True)

    @authentication_required
    def delta(self):
        return self.position(self.symbol)['homeNotional']

    @authentication_required
    def buy(self, symbol, quantity, ordertpye, price=None, stopPx=None):
        """Place a buy order.

        Returns order object. ID: orderID
        """
        return self.place_order(symbol, quantity, ordertpye, price=None, stopPx=None)

    @authentication_required
    def sell(self, symbol, quantity, ordertpye, price=None, stopPx=None):
        """Place a sell order.

        Returns order object. ID: orderID
        """
        quantity = - quantity
        return self.place_order(symbol, quantity, ordertpye, price=None, stopPx=None)

    @authentication_required
    def place_order(self, symbol, quantity, ordertpye, price=None, stopPx=None):
        """

        :param symbol:
        :param quantity:
        :param ordertpye:
        :param price: optional when place market order no need to give price
        :param stopPx: when place stop order need to give this value also
        :return:
        """
        postdict = {}
        if ordertpye != "Market":
            if price < 0:
                raise Exception("Price must be positive.")
            else:
                postdict = {'price': price}
                if ordertpye in ['StopLimit', 'LimitIfTouched']:
                    postdict.update({'stopPx': stopPx})

        endpoint = "order"
        # Generate a unique clOrdID with our prefix so we can identify it.
        postdict.update({
            'symbol': symbol,
            'orderQty': quantity,
            'orderType': ordertpye
        })
        return self._curl_bitmex_private(path=endpoint, postdict=postdict, verb="POST", private=True)


    @authentication_required
    def amend_bulk_orders(self, orders):
        """Amend multiple orders."""
        # Note rethrow; if this fails, we want to catch it and re-tick
        return self._curl_bitmex_private(path='order/bulk', postdict={'orders': orders}, verb='PUT', rethrow_errors=True,
                                         private=True)


    @authentication_required
    def create_bulk_orders(self, orders):
        """Create multiple orders."""
        for order in orders:
            order['symbol'] = self.symbol
        return self._curl_bitmex_private(path='order/bulk', postdict={'orders': orders}, verb='POST', private=True)

    @authentication_required
    def active_orders(self):
        """Get open orders via HTTP. Used on close to ensure we catch them all."""
        path = "order"
        orders = self._curl_bitmex_private(
            path=path,
            query={
                'filter': json.dumps({"open": True, 'symbol': self.symbol}),
                'count': 500
            },
            verb="GET",
            private=True
        )
        # Only return orders that start with our clOrdID prefix.
        return [o for o in orders]

    @authentication_required
    def cancel(self, orderID):
        """Cancel an existing order."""
        path = "order"
        postdict = {
            'orderID': orderID,
        }
        return self._curl_bitmex_private(path=path, postdict=postdict, verb="DELETE", private=True)

    @authentication_required
    def withdraw(self, amount, fee, address):
        path = "user/requestWithdrawal"
        postdict = {
            'amount': amount,
            'fee': fee,
            'currency': 'XBt',
            'address': address
        }
        return self._curl_bitmex_private(path=path, postdict=postdict, verb="POST", max_retries=0, private=True)

    def _curl_bitmex_private(self, path, query=None, postdict=None, timeout=7, verb=None, rethrow_errors=False,
                     max_retries=None, private=None):
        """Send a request to BitMEX Servers."""
        # Handle URL
        url = self.client.base_url + path

        # Default to POST if data is attached, GET otherwise
        if not verb:
            verb = 'POST' if postdict else 'GET'

        # By default don't retry POST or PUT. Retrying GET/DELETE is okay because they are idempotent.
        # In the future we could allow retrying PUT, so long as 'leavesQty' is not used (not idempotent),
        # or you could change the clOrdID (set {"clOrdID": "new", "origClOrdID": "old"}) so that an amend
        # can't erroneously be applied twice.
        if max_retries is None:
            max_retries = 0 if verb in ['POST', 'PUT'] else 3

        # Auth: API Key/Secret

        def exit_or_throw(e):
            if rethrow_errors:
                raise e
            else:
                exit(1)

        def retry():
            self.retries += 1
            if self.retries > max_retries:
                raise Exception("Max retries on %s (%s) hit, raising." % (path, json.dumps(postdict or '')))
            return self._curl_bitmex_private(path, query, postdict, timeout, verb, rethrow_errors, max_retries, private)

        # Make the request
        response = None
        try:
            self.client.logger.info("sending req to %s: %s" % (url, json.dumps(postdict or query or '')))
            if private:
                auth = APIKeyAuthWithExpires(self.apiKey, self.apiSecret)
                req = requests.Request(verb, url, json=postdict, auth=auth, params=query)
            else:
                req = requests.Request(verb, url, json=postdict, params=query)
            prepped = self.client.session.prepare_request(req)
            response = self.client.session.send(prepped, timeout=timeout)
            # Make non-200s throw
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            if response is None:
                raise e

            # 401 - Auth error. This is fatal.
            if response.status_code == 401:
                self.client.logger.error("API Key or Secret incorrect, please check and restart.")
                self.client.logger.error("Error: " + response.text)
                if postdict:
                    self.client.logger.error(postdict)
                # Always exit, even if rethrow_errors, because this is fatal
                exit(1)

            # 404, can be thrown if order canceled or does not exist.
            elif response.status_code == 404:
                if verb == 'DELETE':
                    self.client.logger.error("Order not found: %s" % postdict['orderID'])
                    return
                self.client.logger.error("Unable to contact the BitMEX API (404). " +
                                  "Request: %s \n %s" % (url, json.dumps(postdict)))
                exit_or_throw(e)

            # 429, ratelimit; cancel orders & wait until X-Ratelimit-Reset
            elif response.status_code == 429:
                self.client.logger.error("Ratelimited on current request. Sleeping, then trying again. Try fewer " +
                                  "order pairs or contact support@bitmex.com to raise your limits. " +
                                  "Request: %s \n %s" % (url, json.dumps(postdict)))

                # Figure out how long we need to wait.
                ratelimit_reset = response.headers['X-Ratelimit-Reset']
                to_sleep = int(ratelimit_reset) - int(time.time())
                reset_str = datetime.datetime.fromtimestamp(int(ratelimit_reset)).strftime('%X')

                # We're ratelimited, and we may be waiting for a long time. Cancel orders.
                self.client.logger.warning("Canceling all known orders in the meantime.")
                self.cancel([o['orderID'] for o in self.active_orders()])

                self.client.logger.error("Your ratelimit will reset at %s. Sleeping for %d seconds." % (reset_str, to_sleep))
                time.sleep(to_sleep)

                # Retry the request.
                return retry()

            # 503 - BitMEX temporary downtime, likely due to a deploy. Try again
            elif response.status_code == 503:
                self.client.logger.warning("Unable to contact the BitMEX API (503), retrying. " +
                                    "Request: %s \n %s" % (url, json.dumps(postdict)))
                time.sleep(3)
                return retry()

            elif response.status_code == 400:
                error = response.json()['error']
                message = error['message'].lower() if error else ''

                # Duplicate clOrdID: that's fine, probably a deploy, go get the order(s) and return it
                if 'duplicate clordid' in message:
                    orders = postdict['orders'] if 'orders' in postdict else postdict

                    IDs = json.dumps({'clOrdID': [order['clOrdID'] for order in orders]})
                    orderResults = self._curl_bitmex('/order', query={'filter': IDs}, verb='GET')

                    for i, order in enumerate(orderResults):
                        if (
                                order['orderQty'] != abs(postdict['orderQty']) or
                                order['side'] != ('Buy' if postdict['orderQty'] > 0 else 'Sell') or
                                order['price'] != postdict['price'] or
                                order['symbol'] != postdict['symbol']):
                            raise Exception(
                                'Attempted to recover from duplicate clOrdID, but order returned from API ' +
                                'did not match POST.\nPOST data: %s\nReturned order: %s' % (
                                    json.dumps(orders[i]), json.dumps(order)))
                    # All good
                    return orderResults

                elif 'insufficient available balance' in message:
                    self.logger.error('Account out of funds. The message: %s' % error['message'])
                    exit_or_throw(Exception('Insufficient Funds'))

            # If we haven't returned or re-raised yet, we get here.
            self.client.logger.error("Unhandled Error: %s: %s" % (e, response.text))
            self.client.logger.error("Endpoint was: %s %s: %s" % (verb, path, json.dumps(postdict)))
            exit_or_throw(e)

        except requests.exceptions.Timeout as e:
            # Timeout, re-run this request
            self.client.logger.warning("Timed out on request: %s (%s), retrying..." % (path, json.dumps(postdict or '')))
            return retry()

        except requests.exceptions.ConnectionError as e:
            self.client.logger.warning("Unable to contact the BitMEX API (%s). Please check the URL. Retrying. " +
                                "Request: %s %s \n %s" % (e, url, json.dumps(postdict)))
            time.sleep(1)
            return retry()

        # Reset retry counter on success
        self.retries = 0

        return response.json()

    def _curl_bitmex(self, path, query=None, postdict=None, timeout=7, verb=None, rethrow_errors=False,
                     max_retries=None):
        """Send a request to BitMEX Servers."""
        # Handle URL
        url = self.client.base_url + path

        # Default to POST if data is attached, GET otherwise
        if not verb:
            verb = 'POST' if postdict else 'GET'

        # By default don't retry POST or PUT. Retrying GET/DELETE is okay because they are idempotent.
        # In the future we could allow retrying PUT, so long as 'leavesQty' is not used (not idempotent),
        # or you could change the clOrdID (set {"clOrdID": "new", "origClOrdID": "old"}) so that an amend
        # can't erroneously be applied twice.
        if max_retries is None:
            max_retries = 0 if verb in ['POST', 'PUT'] else 3

        # Auth: API Key/Secret

        def exit_or_throw(e):
            if rethrow_errors:
                raise e
            else:
                exit(1)

        def retry():
            self.retries += 1
            if self.retries > max_retries:
                raise Exception("Max retries on %s (%s) hit, raising." % (path, json.dumps(postdict or '')))
            return self._curl_bitmex(path, query, postdict, timeout, verb, rethrow_errors, max_retries)

        # Make the request
        response = None
        try:
            self.client.logger.info("sending req to %s: %s" % (url, json.dumps(postdict or query or '')))
            req = requests.Request(verb, url, json=postdict, params=query)
            prepped = self.client.session.prepare_request(req)
            response = self.client.session.send(prepped, timeout=timeout)
            # Make non-200s throw
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            if response is None:
                raise e


            # 429, ratelimit; cancel orders & wait until X-Ratelimit-Reset
            if response.status_code == 429:
                self.client.logger.error("Ratelimited on current request. Sleeping, then trying again. Try fewer " +
                                         "order pairs or contact support@bitmex.com to raise your limits. " +
                                         "Request: %s \n %s" % (url, json.dumps(postdict)))

                # Figure out how long we need to wait.
                ratelimit_reset = response.headers['X-Ratelimit-Reset']
                to_sleep = int(ratelimit_reset) - int(time.time())
                reset_str = datetime.datetime.fromtimestamp(int(ratelimit_reset)).strftime('%X')

                self.client.logger.error("Your ratelimit will reset at %s. Sleeping for %d seconds." % (reset_str, to_sleep))
                time.sleep(to_sleep)

                # Retry the request.
                return retry()

            # 503 - BitMEX temporary downtime, likely due to a deploy. Try again
            elif response.status_code == 503:
                self.client.logger.warning("Unable to contact the BitMEX API (503), retrying. " +
                                           "Request: %s \n %s" % (url, json.dumps(postdict)))
                time.sleep(3)
                return retry()

            # If we haven't returned or re-raised yet, we get here.
            self.client.logger.error("Unhandled Error: %s: %s" % (e, response.text))
            self.client.logger.error("Endpoint was: %s %s: %s" % (verb, path, json.dumps(postdict)))
            exit_or_throw(e)

        except requests.exceptions.Timeout as e:
            # Timeout, re-run this request
            self.client.logger.warning("Timed out on request: %s (%s), retrying..." % (path, json.dumps(postdict or '')))
            return retry()

        except requests.exceptions.ConnectionError as e:
            self.client.logger.warning("Unable to contact the BitMEX API (%s). Please check the URL. Retrying. " +
                                       "Request: %s %s \n %s" % (e, url, json.dumps(postdict)))
            time.sleep(1)
            return retry()

        # Reset retry counter on success
        self.retries = 0

        return response.json()
