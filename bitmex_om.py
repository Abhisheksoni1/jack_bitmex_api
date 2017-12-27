import sys

import datetime

from future.types.newstr import unicode

import settings
from bitmex import bitmex
from time import sleep
from bitmex.utils import log, errors

logger = log.setup_custom_logger('root')

'''
Error_code:
0: 'None'
1: 'Nonce is too small.'
999: Others
'''


def nowStr(isDate=False):
    if isDate:
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]


def todayStr():
    return datetime.datetime.today().strftime("%Y-%m-%d")


class Order(object):
    def __init__(self, exchCode, sym_, _sym, orderType, side, qty, price='', stopPrice=''):
        """in case of Market orders prices not given
           other order type also works perfectly fine
        """
        self.odid = None
        self.status = None
        self.tempOdid = None
        self.sym_ = sym_
        self._sym = _sym
        self.symbol = sym_ + _sym
        self.exchCode = exchCode.upper()
        self.orderType = orderType
        self.price = price
        self.fair = -1.0
        self.side = side
        self.quantity = qty
        self.stopPx = stopPrice

        self.activeTs = -1.0


class ExchangeInterface:
    def __init__(self, dry_run=False):
        self.exchCode = 'Bitmex'
        self.dry_run = dry_run
        if len(sys.argv) > 1:
            self.symbol = sys.argv[1]
        else:
            self.symbol = settings.SYMBOL
        self.bitmex = bitmex.BitMEX(base_url=settings.BASE_URL, symbol=self.symbol,
                                    apiKey=settings.API_KEY, apiSecret=settings.API_SECRET,
                                    orderIDPrefix=settings.ORDERID_PREFIX, postOnly=settings.POST_ONLY)
        self.cxlNb = 0
        self.retryNum = 5

    def create(self, o):
        ackMsg = self.place_order(**o.__dict__)
        return ackMsg

    def isActive(self, ackMsg):
        odid, timestamp = None, None
        if isinstance(ackMsg, dict) and ackMsg['ordStatus'] != 'Filled':
            odid = str(ackMsg['orderID'])
            timestamp = ackMsg['timestamp']
            return odid, timestamp, ackMsg
        else:
            intAckMsg = self.handleUnknownMsg(ackMsg)
            return odid, timestamp, intAckMsg

    def cxl(self, odid):
        ackMsg = self.cancel_order(order_id=odid)[0]
        try:
            if ackMsg['ordStatus'] == 'Canceled':
                return ackMsg
        except Exception:
            intAckMsg = self.handleUnknownMsg(ackMsg)
            return intAckMsg

    def isCxlSuccess(self, ackMsg):
        return self.readOrderStatus(ackMsg)

    def isCxlAllSuccess(self):
        ackMsg = self.active_orders()
        if isinstance(ackMsg, list):
            if not len(ackMsg):
                return True
            else:
                return False
        else:
            return False

    def cancelAllOrders(self):
        logger.info('Cancelling all active orders')
        self.cancel_all_orders()
        ackMsg = self.active_orders()
        if len(ackMsg) == 0:
            ackMsg = True
            return ackMsg

    def checkOrderStatus(self, o):
        a_orders = self.active_orders()
        if not len(a_orders):
            return []
        for i in range(a_orders):
            if a_orders[i]['orderID'] == o.odid:
                return self.readOrderStatus(a_orders[i])
        # return self.readOrderStatus(ackMsg)


    def readOrderStatus(self, ackMsg):
        orderStatus = None
        tradedPrice, tradedQty, remainQty = None, None, None
        if type(ackMsg) is dict:
            # it means order place
            isTraded = True if ackMsg['ordStatus'] == 'Filled' else False and not ackMsg['workingIndicator']
            isCancelled = True if ackMsg['ordStatus'] == 'Canceled' else False
            isActive = True if ackMsg['ordStatus'] == 'New' else False and ackMsg['workingIndicator']
            unknown = True if not all([isTraded,isCancelled,isActive]) else False
            if isTraded:
                tradedPrice = float(ackMsg['price'])
                tradedQty   = float(ackMsg['cumQty'])
                remainQty   = float(ackMsg['leavesQty'])
                orderStatus = 'FILLED'
            elif isCancelled:
                orderStatus = 'CXLED'
            elif isActive:
                orderStatus = 'ACTIVE'
            else:
                orderStatus = 'UNKNOWN'
            return (orderStatus, tradedPrice, tradedQty, remainQty)
        else:
            return self.handleUnknownMsg(ackMsg)

    def getActiveOrders(self):
        ackMsg = self.active_orders()
        if type(ackMsg) == list:
            if not len(ackMsg):
                return ackMsg
            odids = []
            for obj in ackMsg:
                odids.append(str(obj['orderID']))
            return odids
        else:
            intAckMsg = self.handleUnknownMsg(ackMsg)
            logger.error('Unexpected ackMsg::getActiveOrders %s %s' % ("Bitmex", intAckMsg))
            return None

    def getInitActiveOrders(self):
        ackMsg = self.active_orders()
        if type(ackMsg) == list:
            if not len(ackMsg):
                return ackMsg
            else:
                tmpActiveOrderList = []
                for i in range(len(ackMsg)):
                    obj = ackMsg[i]
                    odid = str(obj['orderID'])
                    symbol = str(obj['symbol'])
                    sym_ = symbol[:3]
                    _sym = symbol[3:]
                    orderType = obj['ordType']
                    price = float(obj['price'])
                    side = obj['side'].upper()
                    print(obj)
                    qty = float(obj['leavesQty'])
                    o = Order(self.exchCode, sym_, _sym, orderType, side, qty, price)
                    o.odid = odid
                    o.status = 'ACTIVE'
                    o.activeTs = nowStr()
                    tmpActiveOrderList.append(o)
                return tmpActiveOrderList
        else:
            intAckMsg = self.handleUnknownMsg(ackMsg)
            logger.error('Unexpected ackMsg::getInitActiveOrders %s %s' % (self.exchCode, intAckMsg))
            return None

    def getBalances(self):
        ackMsg = 'INT_MAX_SENT'
        self.balances, self.available = {}, {}
        n = 0
        n += 1
        ackMsg = self.get_margin()
        if isinstance(ackMsg, dict):
            self.balances[str(ackMsg['currency'].upper())] = float(ackMsg['marginBalance']/(10**8))
            self.available[str(ackMsg['currency'].upper())] = float(ackMsg['availableMargin']/(10**8))
        else:
            intAckMsg = self.handleUnknownMsg(ackMsg)
            if intAckMsg in ['INT_ERR_0','INT_ERR_1']:
                logger.error('getBalances %s %s' % (self.exchCode, intAckMsg))

            else:
                logger.error('Unexpected ackMsg::getBalances %s %s' % (self.exchCode, intAckMsg))
        return self.balances, self.available

    def handleUnknownMsg(self, ackMsg):
        if type(ackMsg) is dict and 'message' in ackMsg:
            ackMsg = ackMsg['message']
        elif type(ackMsg) is dict and 'error' in ackMsg:
            ackMsg = ackMsg['error']

        if ackMsg == 'INT_MAX_SENT':
            return ackMsg
        elif ackMsg is None or ackMsg == 'None':
            return 'INT_ERR_0'
        elif type(ackMsg) is str or type(ackMsg) is unicode:
            ackMsg = ackMsg.lower()
            if 'nonce' in ackMsg:
                return 'INT_ERR_1'
            elif 'err_rate_limit' in ackMsg:
                return 'INT_ERR_3: ERR_RATE_LIMIT' # Sending too many messages
            else:
                return 'INT_ERR_999: ' + ackMsg
        else:
            return 'Unexpected ackMsg=%s type=%s waiting to handle it in handleUnknownMsg()' % (ackMsg, type(ackMsg))

    def cancel_order(self, order_id):
        logger.info("Cancelling orders for id {} ".format(order_id))
        try:
            return self.bitmex.cancel(order_id)
            # sleep(settings.API_REST_INTERVAL)
        except ValueError as e:
            logger.info(e)
            return e
            # sleep(settings.API_ERROR_INTERVAL)

    def active_orders(self):
        return self.bitmex.http_open_orders()
    #

    def cancel_all_orders(self):
        if self.dry_run:
            return
        # In certain cases, a WS update might not make it through before we call this.
        # For that reason, we grab via HTTP to ensure we grab them all.
        orders = self.bitmex.http_open_orders()

        if len(orders):
            self.bitmex.cancel([order['orderID'] for order in orders])

        sleep(settings.API_REST_INTERVAL)

    def get_margin(self):
        if self.dry_run:
            return {'marginBalance': float(settings.DRY_BTC), 'availableFunds': float(settings.DRY_BTC)}
        return self.bitmex.funds()

    #
    # def get_highest_buy(self):
    #     buys = [o for o in self.get_orders() if o['side'] == 'Buy']
    #     if not len(buys):
    #         return {'price': -2**32}
    #     highest_buy = max(buys or [], key=lambda o: o['price'])
    #     return highest_buy if highest_buy else {'price': -2**32}
    #
    # def get_lowest_sell(self):
    #     sells = [o for o in self.get_orders() if o['side'] == 'Sell']
    #     if not len(sells):
    #         return {'price': 2**32}
    #     lowest_sell = min(sells or [], key=lambda o: o['price'])
    #     return lowest_sell if lowest_sell else {'price': 2**32}  # ought to be enough for anyone
    #
    # def get_position(self, symbol=None):
    #     if symbol is None:
    #         symbol = self.symbol
    #     return self.bitmex.position(symbol)
    #
    # def get_ticker(self, symbol=None):
    #     if symbol is None:
    #         symbol = self.symbol
    #     return self.bitmex.ticker_data(symbol)

    def place_order(self, **kwargs):
        if kwargs['side'] == 'sell':
            return self.bitmex.sell(**kwargs)
        elif kwargs['side'] == 'buy':
            return self.bitmex.buy(**kwargs)


o = Order('bitmex', 'XBT', 'USD', 'Limit', 'buy', 100, 15200)
ex = ExchangeInterface()
# acknw = (ex.create(o))
# sleep(1)
print(ex.getBalances())
# print(ex.isActive(acknw))
# print(ex.get_instrument())
# print(ex.is_open())
#{'orderID': 'e554bd97-3d2d-441a-61e8-7d5bbc3016e5', 'clOrdID': 'mm_bitmex_g6KFGQxPRwOvlttZweRqcg', 'clOrdLinkID': '',
#  'account': 6537, 'symbol': 'XBTUSD', 'side': 'Buy', 'simpleOrderQty': None, 'orderQty': 100, 'price': 15373,
# 'displayQty': None, 'stopPx': None, 'pegOffsetValue': None, 'pegPriceType': '', 'currency': 'USD', 'settlCurrency': 'XBt',
# 'ordType': 'Market', 'timeInForce': 'ImmediateOrCancel', 'execInst': '', 'contingencyType': '', 'exDestination': 'XBME',
# 'ordStatus': 'Filled', 'triggered': '', 'workingIndicator': False, 'ordRejReason': '', 'simpleLeavesQty': 0, 'leavesQty': 0,
# 'simpleCumQty': 0.006505, 'cumQty': 100, 'avgPx': 15373, 'multiLegReportingType': 'SingleSecurity', 'text': 'Submitted via API.',
# 'transactTime': '2017-12-27T10:31:22.922Z', 'timestamp': '2017-12-27T10:31:22.922Z'}
