import bigqmt_signal_trader.xtquant_compat as _compat


def _norm_time_tag(value):
    """统一时间为纯数字字符串：'20260807 14:56:00' -> '20260807145600'，'20260806' 原样。"""
    return "".join(ch for ch in str(value) if ch.isdigit())


_BARS_PER_DAY = {"1m": 240, "5m": 48, "15m": 16, "30m": 8, "1h": 4, "60m": 4, "1d": 1}
_DAYS_PER_BAR = {"1w": 7, "1M": 31, "1q": 92, "1y": 365}


def _is_index_code(code):
    """按代码段粗判指数：SH 000xxx / SZ 399xxx / BJ 899xxx / SH 88xxxx(TDX行业指数)。"""
    c = str(code)
    return ((c.startswith("000") and c.endswith(".SH"))
            or (c.startswith("399") and c.endswith(".SZ"))
            or (c.startswith("899") and c.endswith(".BJ"))
            or (c.startswith("88") and c.endswith(".SH")))


def _guard_index_dividend(codes, dividend_type):
    """大QMT ContextInfo 对指数做复权可能返回退化序列
    （实测 000001.SH 前复权恒等于最新价，疑似本地复权因子表损坏）。
    指数无分红送配，复权 ≡ 原始价，强制降级为 'none'。"""
    if dividend_type in ("", "none", None):
        return dividend_type
    if any(_is_index_code(c) for c in codes):
        return "none"
    return dividend_type


def _derive_start_time(period, count):
    """大QMT 数据服务把空 start_time 当"从头开始"，会超出账号最大起始时间
    （如 1m/3001 限最近一年，报"超出最大起始时间"）。
    按 count 与周期推算一个足以覆盖所需 bar 数、且不超过 360 天的起始时间。
    已实测 count 语义为"区间内最新 N 根"，与 start_time='' 的默认行为等价。
    无法识别的周期返回 ''（保持原样）。"""
    import math
    from datetime import datetime, timedelta

    if period in _BARS_PER_DAY:
        bpd = _BARS_PER_DAY[period]
        bars = count if isinstance(count, int) and count > 0 else bpd * 30
        days = math.ceil(bars / bpd * 2.2) + 7
    elif period in _DAYS_PER_BAR:
        bars = count if isinstance(count, int) and count > 0 else 30
        days = math.ceil(bars * _DAYS_PER_BAR[period] * 1.3) + 7
    else:
        return ""
    days = min(days, 360)
    start = datetime.now() - timedelta(days=days)
    return start.strftime("%Y%m%d") if period in ("1d", "1w", "1M", "1q", "1y") else start.strftime("%Y%m%d%H%M%S")


def _rows_to_field_dict(raw, codes, fields):
    """把桥接返回的 list[{'index'/'stime', field...}] 或 {code: rows}
    转成 miniQMT 原生结构 {field: DataFrame(index=code, columns=time)}。
    无法识别的结构返回 None。"""
    import pandas as pd

    series = {}  # {field: {code: {time: value}}}
    row_time_keys = ("index", "stime", "time")

    def eat_rows(code, rows):
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            t = None
            for k in row_time_keys:
                if row.get(k) is not None:
                    t = row[k]
                    break
            if t is None:
                continue
            t = _norm_time_tag(t)
            row_fields = fields or [k for k in row if k not in row_time_keys]
            for f in row_fields:
                if f in row:
                    series.setdefault(f, {}).setdefault(code, {})[t] = row[f]

    if hasattr(raw, "to_dict") and hasattr(raw, "columns"):
        # 长表 DataFrame（columns 含 index/stime/time + 各 field），按单股处理
        if not codes:
            return None
        try:
            raw = raw.to_dict("records")
        except Exception:
            return None
    if isinstance(raw, list):
        if not codes:
            return None
        eat_rows(codes[0], raw)
    elif isinstance(raw, dict):
        code_keys = [c for c in codes if c in raw]
        if not code_keys:
            return None
        for code in code_keys:
            eat_rows(code, raw.get(code))
    else:
        return None

    if not series:
        return None
    out = {}
    for f, by_code in series.items():
        df = pd.DataFrame.from_dict(by_code, orient="index")
        df = df.reindex(sorted(df.columns), axis=1)
        out[f] = df
    return out


def __getattr__(name):
    return getattr(_compat.xtdata, name)


def get_full_tick(code_list):
    return _compat.xtdata.get_full_tick(code_list)


def get_market_data(field_list=[], stock_list=[], period="1d", start_time="", end_time="", count=-1, dividend_type="none", fill_data=True):
    codes = [str(c) for c in (stock_list or [])]
    fields = [str(f) for f in (field_list or [])]
    dividend_type = _guard_index_dividend(codes, dividend_type)
    want_time = "time" in fields
    # 大QMT ContextInfo.get_market_data 不支持 'time' 字段（终端日志报"不支持'time'数据字段"），
    # RPC 前剥离，返回时按 miniQMT 惯例（epoch 毫秒）合成。
    rpc_fields = [f for f in fields if f != "time"]
    if not start_time:
        start_time = _derive_start_time(period, count)
    if len(codes) > 1:
        # 大QMT ContextInfo 多股批量调用返回结构异常（{'is_copy': None}），
        # 逐股查询后按 field 纵向拼接成 {field: DataFrame(index=code, columns=time)}。
        import pandas as pd

        per_field = {}
        for code in codes:
            part = get_market_data(fields, [code], period, start_time, end_time, count, dividend_type, fill_data)
            if not isinstance(part, dict):
                continue
            for f, df in part.items():
                if hasattr(df, "columns"):
                    per_field.setdefault(f, []).append(df)
        return {f: pd.concat(dfs) for f, dfs in per_field.items() if dfs}

    raw = _compat.xtdata.get_market_data(rpc_fields, codes, period, start_time, end_time, count, dividend_type, fill_data)
    if isinstance(raw, dict) and raw and all(hasattr(v, "columns") for v in raw.values()):
        return raw  # 已是 miniQMT 原生 {field: DataFrame} 结构
    normalized = _rows_to_field_dict(raw, codes, rpc_fields)
    if normalized is None:
        return raw
    if want_time and "time" not in normalized and normalized:
        normalized["time"] = _make_time_field(next(iter(normalized.values())))
    return normalized


def _make_time_field(ref_df):
    """按 miniQMT 惯例合成 time 字段（epoch 毫秒），结构与 ref_df 相同（index=code, columns=time）。"""
    import pandas as pd

    labels = list(ref_df.columns)
    epochs = []
    for t in labels:
        try:
            epochs.append(int(pd.Timestamp(str(t)).timestamp() * 1000))
        except Exception:
            try:
                epochs.append(int(str(t)))
            except Exception:
                epochs.append(0)
    data = {code: dict(zip(labels, epochs)) for code in ref_df.index}
    return pd.DataFrame.from_dict(data, orient="index")


def get_market_data_ex(field_list=[], stock_list=[], period="1d", start_time="", end_time="", count=-1, dividend_type="none", fill_data=True):
    dividend_type = _guard_index_dividend([str(c) for c in (stock_list or [])], dividend_type)
    return _compat.xtdata.get_market_data_ex(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)


def get_local_data(field_list=[], stock_list=[], period="1d", start_time="", end_time="", count=-1, dividend_type="none", fill_data=True, data_dir=None):
    dividend_type = _guard_index_dividend([str(c) for c in (stock_list or [])], dividend_type)
    return _compat.xtdata.get_local_data(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, data_dir)


def get_instrument_detail(stock_code):
    return _compat.xtdata.get_instrument_detail(stock_code)


def get_instrumentdetail(stock_code):
    return _compat.xtdata.get_instrumentdetail(stock_code)


def get_instrument_type(stock_code, variety_list=None):
    return _compat.xtdata.get_instrument_type(stock_code, variety_list)


def get_stock_list_in_sector(sector_name, real_timetag=-1):
    return _compat.xtdata.get_stock_list_in_sector(sector_name, real_timetag=real_timetag)


def get_sector_list():
    return _compat.xtdata.get_sector_list()


def get_sector_info(sector_name=""):
    return _compat.xtdata.get_sector_info(sector_name)


def subscribe_quote(stock_code, period="1d", start_time="", end_time="", count=0, callback=None):
    return _compat.xtdata.subscribe_quote(stock_code, period, start_time, end_time, count, callback)


def subscribe_quote2(stock_code, period="1d", start_time="", end_time="", count=0, dividend_type=None, callback=None):
    return _compat.xtdata.subscribe_quote2(stock_code, period, start_time, end_time, count, dividend_type, callback)


def subscribe_whole_quote(code_list, callback=None):
    return _compat.xtdata.subscribe_whole_quote(code_list, callback=callback)


def unsubscribe_quote(seq):
    return _compat.xtdata.unsubscribe_quote(seq)


def run():
    return _compat.xtdata.run()


def get_divid_factors(stock_code, start_time="", end_time=""):
    return _compat.xtdata.get_divid_factors(stock_code, start_time, end_time)


def getDividFactors(*args, **kwargs):
    return _compat.xtdata.get_divid_factors(*args, **kwargs)


def download_history_data(stock_code, period, start_time="", end_time="", incrementally=None):
    return _compat.xtdata.download_history_data(stock_code, period, start_time, end_time, incrementally)


def download_history_data2(stock_list, period, start_time="", end_time="", callback=None, incrementally=None):
    return _compat.xtdata.download_history_data2(stock_list, period, start_time, end_time, callback, incrementally)


def get_trading_dates(market, start_time="", end_time="", count=-1):
    return _compat.xtdata.get_trading_dates(market, start_time, end_time, count)


def get_holidays():
    return _compat.xtdata.get_holidays()


def download_holiday_data(incrementally=True):
    return _compat.xtdata.download_holiday_data(incrementally)


def get_ipo_info(start_time="", end_time=""):
    return _compat.xtdata.get_ipo_info(start_time, end_time)


def get_etf_info():
    return _compat.xtdata.get_etf_info()


def download_etf_info():
    return _compat.xtdata.download_etf_info()


def get_option_list(undl_code, dedate, opttype="", isavailavle=False):
    return _compat.xtdata.get_option_list(undl_code, dedate, opttype, isavailavle)


def get_his_option_list(undl_code, dedate):
    return _compat.xtdata.get_his_option_list(undl_code, dedate)


def get_his_option_list_batch(undl_code, start_time="", end_time=""):
    return _compat.xtdata.get_his_option_list_batch(undl_code, start_time, end_time)


def get_financial_data(stock_list, table_list=[], start_time="", end_time="", report_type="report_time"):
    return _compat.xtdata.get_financial_data(stock_list, table_list, start_time, end_time, report_type)


def download_financial_data(stock_list, table_list=[], start_time="", end_time="", incrementally=None):
    return _compat.xtdata.download_financial_data(stock_list, table_list, start_time, end_time, incrementally)


def download_financial_data2(stock_list, table_list=[], start_time="", end_time="", callback=None):
    return _compat.xtdata.download_financial_data2(stock_list, table_list, start_time, end_time, callback)


def call_formula(formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param={}):
    return _compat.xtdata.call_formula(formula_name, stock_code, period, start_time, end_time, count, dividend_type, extend_param)


def subscribe_formula(formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param={}, callback=None):
    return _compat.xtdata.subscribe_formula(formula_name, stock_code, period, start_time, end_time, count, dividend_type, extend_param, callback)


def unsubscribe_formula(request_id):
    return _compat.xtdata.unsubscribe_formula(request_id)


def get_formula_result(request_id, start_time="", end_time="", count=-1, timeout_second=-1):
    return _compat.xtdata.get_formula_result(request_id, start_time, end_time, count, timeout_second)


def gen_factor_index(data_name, formula_name, vars, sector_list, start_time="", end_time="", period="1d", dividend_type="none"):
    return _compat.xtdata.gen_factor_index(data_name, formula_name, vars, sector_list, start_time, end_time, period, dividend_type)
