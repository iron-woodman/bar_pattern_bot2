from src.binance_api import load_futures_list
import multiprocessing
from binance import Client
from binance.enums import HistoricalKlinesType
import json
import datetime
import src.logger as custom_logging
from src.config_handler import TIMEFRAMES, BINANCE_API_KEY, BINANCE_Secret_KEY

THREAD_CNT = 1  # 3 потока на ядро


def check_history_bars_for_pattern_3bars(pair, bars: list) -> str:
    """
    Поиск свечного паттерна в барах истории
    :param bars:
    :return:
    """
    if len(bars) < 4:
        print(f'{pair}: Bar count = {len(bars)}.')
        return ""
    _time = []
    cl = []
    op = []

    vol = []

    for bar in bars:
        value = datetime.datetime.fromtimestamp(bar[0] / 1000)
        # print(value.strftime('%Y-%m-%d %H:%M:%S'))
        _time.append(value.strftime('%Y-%m-%d %H:%M:%S'))
        op.append(float(bar[1]))
        cl.append(float(bar[4]))
        vol.append(float(bar[5]))

    # проверяем значения на паттерны
    if cl[2] > cl[1] > cl[0] and vol[2] > vol[1] > vol[0] and cl[2] - op[2] > cl[1] - op[1] > cl[0] - op[0] \
            and cl[2] > op[2] and cl[1] > op[1] and cl[0] > op[0]:
        # цена и объемы росли росли 3 дня подряд, размер баров (close-open) увеличивался в течении этих 3-х дней
        custom_logging.info(
            f'{pair}: CLOSE: {cl[2]}- > {cl[1]} > {cl[0]} VOL: {vol[2]} > {vol[1]} > {vol[0]} => SHORT')
        return "SHORT"
    elif cl[2] < cl[1] < cl[0] and vol[2] > vol[1] > vol[0] and op[2] - cl[2] > op[1] - cl[1] > op[0] - cl[0] \
            and op[2] > cl[2] and op[1] > cl[1] and op[0] > cl[0]:
        # цена падала 3 дня подряд, а объемы росли росли 3 дня подряд, размер баров (open-close) увеличивался в течении этих 3-х дней
        custom_logging.info(
            f'{pair}: CLOSE: {cl[2]} < {cl[1]} < {cl[0]} VOL: {vol[2]} > {vol[1]} > {vol[0]} => LONG')
        return "LONG"
    return ""


def check_history_bars_for_pattern_2bars_v2(pair, bars: list) -> str:
    """
    Поиск свечного паттерна в барах истории
    :param bars:
    :return:
    """
    if len(bars) < 3:
        print(f'{pair}: Bar count = {len(bars)}.')
        return ""
    _time = []
    cl = []
    op = []
    vol = []

    for bar in bars:
        value = datetime.datetime.fromtimestamp(bar[0] / 1000)
        # print(value.strftime('%Y-%m-%d %H:%M:%S'))
        _time.append(value.strftime('%Y-%m-%d %H:%M:%S'))
        op.append(float(bar[1]))
        cl.append(float(bar[4]))
        vol.append(float(bar[5]))

    # проверяем значения на паттерны
    if cl[2] > cl[1] and vol[2] > vol[1] and cl[2] - op[2] > cl[1] - op[1] \
            and cl[2] > op[2] and cl[1] > op[1]:
        # цена и объемы росли росли 2 дня подряд, размер баров (close-open) увеличивался в течении этих 2-х дней
        custom_logging.info(
            f'{pair}: CLOSE: {cl[2]}- > {cl[1]}  VOL: {vol[2]} > {vol[1]}  => SHORT')
        return "SHORT"
    elif cl[2] < cl[1] and vol[2] > vol[1] and op[2] - cl[2] > op[1] - cl[1] \
            and op[2] > cl[2] and op[1] > cl[1]:
        # цена падала 2 дня подряд, а объемы росли росли 2 дня подряд, размер баров (open-close) увеличивался в течении этих 2-х дней
        custom_logging.info(
            f'{pair}: CLOSE: {cl[2]} < {cl[1]}  VOL: {vol[2]} > {vol[1]}   => LONG')
        return "LONG"
    return ""


def check_history_bars_for_pattern_2bars_v1(pair, bars: list) -> str:
    """
    Поиск свечного паттерна в барах истории
    :param bars:
    :return:
    """
    if len(bars) < 3:
        print(f'{pair}: Bar count = {len(bars)}.')
        return ""
    _time = []
    close = []
    volume = []

    for bar in bars:
        value = datetime.datetime.fromtimestamp(bar[0] / 1000)
        # print(value.strftime('%Y-%m-%d %H:%M:%S'))
        _time.append(value.strftime('%Y-%m-%d %H:%M:%S'))
        close.append(float(bar[4]))
        volume.append(float(bar[5]))

    # проверяем значения на паттерны
    if close[2] > close[1] > close[0] and volume[2] < volume[1]:
        # цена 2 дня росла, а объемы падали
        custom_logging.info(
            f'{pair}: CLOSE: {close[2]} > {close[1]} > {close[0]} VOL: {volume[2]} < {volume[1]} => SHORT')
        return "SHORT"
    elif close[2] < close[1] < close[0] and volume[2] < volume[1]:
        # цена 2 дня падала и объемы тоже падали
        custom_logging.info(
            f'{pair}: CLOSE: {close[2]} < {close[1]} < {close[0]} VOL: {volume[2]} < {volume[1]} => LONG')
        return "LONG"
    return ""


def load_history_bars(task):
    """
    Load historical bars
    :return:
    """
    result = dict()
    pair = task[0]
    api_key = task[1]
    secret_key = task[2]
    all_timeframes = task[3]
    is_spot = task[4]
    client = Client(api_key, secret_key)

    try:
        result['id'] = pair
        for timeframe in all_timeframes:
            if timeframe == '1d':
                st_time = "4 day ago UTC"
            else:
                print('Unknown timeframe:', timeframe)
                custom_logging.error(f'Load history bars error: unknown timeframe "{timeframe}"')
                continue

            bars = []
            try:
                if is_spot:
                    bars = client.get_historical_klines(pair, timeframe, st_time, HistoricalKlinesType.SPOT)
                else:
                    bars = client.get_historical_klines(pair, timeframe, st_time,
                                                        klines_type=HistoricalKlinesType.FUTURES)

            except Exception as e:
                print(pair, ':', e)

            if len(bars) == 0:
                print(f" 0 bars has been gathered from server. client.get_historical_klines({pair}, {timeframe}, "
                      f"{st_time})")
                result[timeframe] = 0
                continue
            # ------------ check for 3 different bar patterns ----------------------------------------------------------
            result["signal_3bars"] = check_history_bars_for_pattern_3bars(pair, bars)
            result["signal_2bars_v2"] = check_history_bars_for_pattern_2bars_v2(pair, bars)
            result["signal_2bars_v1"] = check_history_bars_for_pattern_2bars_v1(pair, bars)
            # ----------------------------------------------------------------------------------------------------------
        return result
    except Exception as e:
        print("Exception when calling load_history_bars: ", e)
        return None


def store_signals_to_file(signals_data: dict, pattern_name: str):
    with open(f"signals_{pattern_name}/{datetime.date.today().isoformat()}.txt", 'w', encoding='utf-8') as f:
        json.dump(signals_data, f, ensure_ascii=False, indent=4, separators=(',', ': '))
        print('Signals (pattern 3bars) data  stored to file.')
        custom_logging.info(
            f'New signals data stored to file "signals_{pattern_name}/{datetime.date.today().isoformat()}.txt".')
        custom_logging.info(
            f'**************************************************************************************')


def load_futures_history_bars_end(responce_list):
    signals_2bars_v1 = dict()
    signals_2bars_v2 = dict()
    signals_3bars = dict()

    for responce in responce_list:
        id = responce['id']
        del responce['id']
        if responce['signal_2bars_v1'] != '':
            signals_2bars_v1[id] = responce['signal_2bars_v1']

        if responce['signal_2bars_v2'] != '':
            signals_2bars_v2[id] = responce['signal_2bars_v2']

        if responce['signal_3bars'] != '':
            signals_3bars[id] = responce['signal_3bars']

    try:
        store_signals_to_file(signals_3bars, "3bars")
        store_signals_to_file(signals_2bars_v2, "2bars_v2")
        store_signals_to_file(signals_2bars_v1, "2bars_v1")
    except Exception as e:
        print("load_futures_history_bars_end exception:", e)
        custom_logging.error(f'load_futures_history_bars_end exception: {e}')


if __name__ == '__main__':
    futures_list = load_futures_list()
    print('Futures count:', len(futures_list))
    tasks = []
    try:
        custom_logging.info('Gathering history candles data...')
        for symbol in futures_list:
            tasks.append((symbol, BINANCE_API_KEY, BINANCE_Secret_KEY, TIMEFRAMES, False))
        with multiprocessing.Pool(multiprocessing.cpu_count() * THREAD_CNT) as pool:
            pool.map_async(load_history_bars, tasks, callback=load_futures_history_bars_end)
            pool.close()
            pool.join()
    except Exception as ex:
        print("Load history bars exception:", ex)
        custom_logging.error(f"Load history bars exception: {ex}")
