"""
bigqmt 桥端到端交易链路验证
==========================
对指定账户依次执行：
  Leg A 成交腿：买 100 股 ETF（对手方价），事件确认成交，核对持仓/资金变化
  Leg B 撤单腿：买 100 股（远低于市价的限价单，不会成交），事件确认已报，
                然后撤单，事件确认已撤

用法：
  python bigqmt_e2e_test.py --account <模拟账号>            # 模拟盘
  python bigqmt_e2e_test.py --account <资金账号>          # 实盘（真买100股！）

注意：
- 必须在交易时段运行（9:30-11:30 / 13:00-15:00）
- 实盘腿要求 QMT 策略运行模式为「实盘」；若为「模拟」则只产生信号，
  脚本会通过持仓核对发现未真实成交并报告
"""
import argparse
import os
import sys
import time

SHIM_PATH = r"<项目根目录>\xtquant\bigqmt_converter-main\src"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--account', required=True, help='资金账号')
    p.add_argument('--code', default='515050.SH', help='测试标的（默认515050 ETF）')
    p.add_argument('--volume', type=int, default=100)
    p.add_argument('--skip-cancel-leg', action='store_true')
    return p.parse_args()


def main():
    args = parse_args()
    # 必须在导入 shim 客户端配置前设定账号
    os.environ['BIGQMT_ACCOUNT_ID'] = args.account
    sys.path.insert(0, SHIM_PATH)

    from xtquant import xttrader, xtdata, xtconstant
    from xtquant.xttype import StockAccount

    code = args.code
    vol = args.volume
    results = []

    print(f"=== bigqmt E2E 测试 account={args.account} code={code} vol={vol} ===")

    xt_trader = xttrader.XtQuantTrader('', 0)

    class CB(xttrader.XtQuantTraderCallback):
        def on_stock_order(self, order):
            print(f"  [事件] 订单回报 status={order.order_status} "
                  f"traded={order.traded_volume} oid={order.order_id} "
                  f"remark={getattr(order, 'order_remark', '')}")

        def on_stock_trade(self, trade):
            print(f"  [事件] 成交回报 {trade.traded_volume}@{trade.traded_price} "
                  f"oid={trade.order_id} remark={getattr(trade, 'order_remark', '')}")

    xt_trader.register_callback(CB())
    xt_trader.start()
    r = xt_trader.connect()
    if r != 0:
        print(f"FAIL: connect 返回 {r}")
        sys.exit(1)
    acc = StockAccount(args.account)
    xt_trader.subscribe(acc)
    print("connect OK")

    # 基线
    asset0 = xt_trader.query_stock_asset(acc)
    pos0 = 0
    for p in (xt_trader.query_stock_positions(acc) or []):
        if str(p.stock_code) == code:
            pos0 = p.volume
    print(f"基线: cash={asset0.cash:.2f} 持仓{code}={pos0}")

    tick = xtdata.get_full_tick([code])
    if not tick or code not in tick:
        print("FAIL: 无行情")
        sys.exit(1)
    t0 = tick[code]
    ask = t0.get('askPrice') or []
    last = t0.get('lastPrice', 0)
    buy_px = round(ask[0] if ask and ask[0] > 0 else last, 3)
    print(f"行情: last={last} ask1={ask[0] if ask else None} -> 买价 {buy_px}")

    def wait_status(oid, targets, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            snap = xt_trader.order_exec_snapshot(oid)
            if snap and snap.get("status") in targets:
                return snap
            time.sleep(0.3)
        return xt_trader.order_exec_snapshot(oid)

    # ---------- Leg A: 成交腿 ----------
    print("\n--- Leg A: 买入成交腿")
    oid_a = xt_trader.order_stock(
        account=acc, stock_code=code, order_type=xtconstant.STOCK_BUY,
        order_volume=vol, price_type=xtconstant.FIX_PRICE, price=buy_px,
        strategy_name='e2e_test', order_remark='e2e_buy')
    print(f"下单返回 order_id={oid_a!r}")
    if not oid_a or (isinstance(oid_a, int) and oid_a <= 0):
        results.append(("A 买入委托", False, f"委托被拒: {oid_a!r}"))
    else:
        snap = wait_status(oid_a, (56,), 20)
        if snap and snap.get("status") == 56 and snap.get("filled_volume", 0) >= vol:
            print(f"  已成交: {snap['filled_volume']}股 @ {snap['avg_price']:.3f}")
            time.sleep(1)
            pos1 = pos0
            for p in (xt_trader.query_stock_positions(acc) or []):
                if str(p.stock_code) == code:
                    pos1 = p.volume
            delta = pos1 - pos0
            if delta >= vol:
                results.append(("A 买入成交+持仓核对", True,
                                f"成交{snap['filled_volume']}@{snap['avg_price']:.3f} 持仓{pos0}->{pos1} 真实"))
            else:
                results.append(("A 买入成交+持仓核对", False,
                                f"事件成交但持仓未变({pos0}->{pos1}) → 策略在模拟运行模式！"))
        else:
            results.append(("A 买入成交", False,
                            f"20s未成交 status={snap.get('status') if snap else '无事件'}"))

    # ---------- Leg B: 撤单腿 ----------
    if not args.skip_cancel_leg:
        print("\n--- Leg B: 挂单撤单腿")
        low_px = round(last * 0.93, 3)  # 低于市价7%，高于跌停价(10%)，不会成交
        print(f"挂单价 {low_px}（市价 {last}）")
        oid_b = xt_trader.order_stock(
            account=acc, stock_code=code, order_type=xtconstant.STOCK_BUY,
            order_volume=vol, price_type=xtconstant.FIX_PRICE, price=low_px,
            strategy_name='e2e_test', order_remark='e2e_cancel')
        print(f"下单返回 order_id={oid_b!r}")
        if not oid_b or (isinstance(oid_b, int) and oid_b <= 0):
            results.append(("B 挂单委托", False, f"委托被拒: {oid_b!r}"))
        else:
            snap = wait_status(oid_b, (50, 55, 56), 8)
            if not snap or snap.get("status") not in (50, 55):
                results.append(("B 挂单确认", False,
                                f"8s未见已报 status={snap.get('status') if snap else '无事件'}"))
            else:
                print(f"  已报(status=50)，sysid={snap.get('order_sys_id')}，发起撤单")
                ok = xt_trader.cancel_order_stock(acc, oid_b)
                print(f"  撤单返回 {ok}")
                snap = wait_status(oid_b, (54,), 10)
                if snap and snap.get("status") == 54:
                    results.append(("B 撤单确认", True, "已撤(status=54)"))
                else:
                    results.append(("B 撤单确认", False,
                                    f"10s未见已撤 status={snap.get('status') if snap else '无事件'}"))

    # ---------- 汇总 ----------
    print("\n=== 结果汇总 ===")
    all_pass = True
    for name, ok, detail in results:
        all_pass = all_pass and ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    print("=== 总体:", "全部通过" if all_pass else "存在失败项", "===")


if __name__ == '__main__':
    main()
