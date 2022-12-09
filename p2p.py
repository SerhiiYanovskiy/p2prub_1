import requests
import config


def get_spot(cource):
    res = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={cource}").text
    res = res.split(":")[-1][1:-2]
    return res


class P2P():

    def __init__(self, **kwargs):
        data = {"asset": kwargs.get('asset'),
                "fiat": kwargs.get("fiat"),
                "merchantCheck": False,
                "page": 1,
                "payTypes": kwargs.get('payTypes'),
                "publisherType": None,
                "rows": 20,
                "tradeType": kwargs.get('tradeType'),
                'transAmount': kwargs.get('limit')}

    def get_corces(self, bank, pot):
                fiat = "RUB"
                bank_data = []
                bank_data.clear()
                bank_data.append(bank)
                for spot in config.SPOT:
                    for limit in config.LIMITS_RUB:
                        if limit == "Без указания среза":
                            limit = None
                        data = {"asset": spot,
                                "fiat": fiat,
                                "merchantCheck": False,
                                "page": 1,
                                "payTypes": [bank],
                                "publisherType": None,
                                "rows": 20,
                                'tradeType': pot,
                                "transAmount": limit}
                        r = requests.post('https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search',
                                          headers=config.headers, json=data)
                        if limit == None:
                            limit = "Без указания среза"
                        result_data = {"bank": bank, "fiat": fiat, "spot": f"{spot} {limit}"}
                        try:
                            result = (r.json()["data"][0]["adv"]['price'])
                        except:
                            result = 0
                        result_data.update({"cours": result})
                        bank_data.append(result_data['cours'])
                        print(fiat, spot, limit, bank, result_data['cours'], pot)

                return bank_data
