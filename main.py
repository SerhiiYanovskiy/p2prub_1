from itertools import chain

import config
from google_ import GoogleSheet
from p2p import P2P, get_spot


def main():
    while True:

        gs = GoogleSheet()
        ad = P2P()
        range = "RUB!A1:ZZ200"
        res = gs.read(range)
        res = []
        spot_limit_list = ["SELL"] + list(
            chain.from_iterable([[f"{y}/{x}" for x in config.LIMITS_RUB] for y in config.SPOT]))

        res.append(spot_limit_list)
        for bank in config.BANKS_RUB:
            res.append(ad.get_corces(bank, "SELL"))
        res.append([" "])
        res.append([" "])
        spot_limit_list = ["BUY"] + list(
            chain.from_iterable([[f"{y}/{x}" for x in config.LIMITS_RUB] for y in config.SPOT]))
        res.append(spot_limit_list)
        for bank in config.BANKS_RUB:
            res.append(ad.get_corces(bank, "BUY"))
        res.append([" "])
        res.append([" "])
        res.append(["SPOT"])
        data_spots_1 = ["USDT/UAH", "BTS/USDT", get_spot("BTSUSDT"), "ETH/USDT", get_spot("ETHUSDT"), "BNB/USDT",
                        get_spot("BNBUSDT")]
        res.append(data_spots_1)
        data_spots_2 = [get_spot("USDTUAH"), "BTC/UAH", get_spot("BTCUAH"), "ETH/UAH", get_spot("ETHUAH"), "BNB/BTC",
                        get_spot("BNBBTC")]
        res.append(data_spots_2)
        data_spots_3 = ["", "BTC/BUSD", get_spot("BTCBUSD"), "ETH/BUSD", get_spot("ETHBUSD"), "BNB/ETH",
                        get_spot("BNBETH")]
        res.append(data_spots_3)
        gs.update(range, res)

        res.clear()


if __name__ == '__main__':
    main()
