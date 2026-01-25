"""
导出处理后的核心基本面数据样例（LLM 友好）。
输出文件：
  - full_fundamentals_sample.json        # 原始全量（便于调试）
  - full_fundamentals_sample_core.json   # 精简核心（推荐给 LLM）
"""
import json
import os
import sys
from typing import Tuple, Optional

# 确保项目根目录在路径中
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tradingagents.data_sources.tushare_provider import TushareProvider
from tradingagents.data_sources.akshare_provider import AkshareProvider
from tradingagents.data_sources.utils import normalize_stock_code


def _df_to_preview(df, limit: int = 5) -> Tuple[Optional[list], dict]:
    if df is None:
        return None, {"total_rows": 0, "columns": []}
    if hasattr(df, "empty") and df.empty:
        return None, {"total_rows": 0, "columns": list(df.columns)}
    preview_df = df.head(limit) if limit and hasattr(df, "head") else df
    try:
        records = preview_df.to_dict("records")
    except Exception:
        records = []
    meta = {
        "total_rows": len(df) if hasattr(df, "__len__") else 0,
        "preview_rows": len(preview_df) if hasattr(preview_df, "__len__") else 0,
        "columns": list(df.columns) if hasattr(df, "columns") else [],
    }
    return records, meta


def _get_latest_row(df):
    if df is None or getattr(df, "empty", True):
        return None
    try:
        return df.iloc[0].to_dict()
    except Exception:
        return None


def _pick_fields(row: dict, fields: list):
    if row is None:
        return None
    data = {}
    for k in fields:
        data[k] = row.get(k)
    return data


def main(ts_code: str = "000001"):
    ts_code_norm = normalize_stock_code(ts_code)

    ak = AkshareProvider()
    ts = TushareProvider()

    out_full = {}
    out_core = {}

    # AkShare（仅保留可用接口）
    ak_company = ak.get_company_info(ts_code)
    ak_earnings = ak.get_earnings_data(ts_code, limit=10)
    out_full["akshare"] = {
        "company_info": ak_company,
        "earnings_data": ak_earnings,
    }
    # 核心提要
    ak_forecast = ak_earnings.get("forecast") if isinstance(ak_earnings, dict) else None
    ak_express = ak_earnings.get("express") if isinstance(ak_earnings, dict) else None
    ak_core = {
        "company": {
            "symbol": ak_company.get("symbol"),
            "name": ak_company.get("name"),
            "industry": ak_company.get("industry"),
            "list_date": ak_company.get("list_date"),
        },
        "earnings": {
            "forecast": ak_forecast[:5] if isinstance(ak_forecast, list) else None,
            "express": ak_express[:5] if isinstance(ak_express, list) else None,
        },
    }
    out_core["akshare"] = ak_core

    # Tushare（主数据源）
    income_df = ts.get_income(ts_code_norm)
    balance_df = ts.get_balancesheet(ts_code_norm)
    cashflow_df = ts.get_cashflow(ts_code_norm)
    fina_df = ts.get_fina_indicator(ts_code_norm)
    daily_basic_df = ts.get_daily_basic(ts_code_norm)
    forecast_df = ts.get_forecast(ts_code_norm, limit=20)
    express_df = ts.get_express(ts_code_norm, limit=20)

    out_full["tushare"] = {
        "income": income_df.to_dict(orient="records") if income_df is not None else None,
        "balance": balance_df.to_dict(orient="records") if balance_df is not None else None,
        "cashflow": cashflow_df.to_dict(orient="records") if cashflow_df is not None else None,
        "fina_indicator": fina_df.to_dict(orient="records") if fina_df is not None else None,
        "daily_basic": daily_basic_df.to_dict(orient="records") if daily_basic_df is not None else None,
        "forecast": forecast_df.to_dict(orient="records") if forecast_df is not None else None,
        "express": express_df.to_dict(orient="records") if express_df is not None else None,
    }

    # 核心提要
    income_core = _pick_fields(
        _get_latest_row(income_df),
        ["ann_date", "end_date", "revenue", "total_revenue", "operate_profit", "n_income", "basic_eps"],
    )
    balance_core = _pick_fields(
        _get_latest_row(balance_df),
        ["ann_date", "end_date", "total_assets", "total_liab", "total_hldr_eqy_exc_min_int", "depos", "loanto_oth_bank_fi"],
    )
    cashflow_core = _pick_fields(
        _get_latest_row(cashflow_df),
        ["ann_date", "end_date", "n_cashflow_act", "n_cashflow_inv_act", "n_cash_flows_fnc_act", "n_incr_cash_cash_equ"],
    )
    fina_core = _pick_fields(
        _get_latest_row(fina_df),
        ["ann_date", "end_date", "eps", "dt_eps", "roe", "roe_dt", "netprofit_margin", "debt_to_assets", "assets_to_eqt", "bps", "ocfps", "q_sales_yoy", "q_op_qoq", "equity_yoy"],
    )
    daily_basic_core = _pick_fields(
        _get_latest_row(daily_basic_df),
        ["trade_date", "close", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "total_mv", "circ_mv"],
    )

    forecast_preview, _ = _df_to_preview(forecast_df, limit=5)
    express_preview, _ = _df_to_preview(express_df, limit=5)
    forecast_core = None
    express_core = None
    if forecast_preview:
        forecast_core = [
            {
                "ann_date": it.get("ann_date"),
                "end_date": it.get("end_date"),
                "type": it.get("type"),
                "p_change_min": it.get("p_change_min"),
                "p_change_max": it.get("p_change_max"),
                "summary": it.get("summary"),
            }
            for it in forecast_preview
        ]
    if express_preview:
        express_core = [
            {
                "ann_date": it.get("ann_date"),
                "end_date": it.get("end_date"),
                "revenue": it.get("revenue"),
                "n_income": it.get("n_income"),
                "eps": it.get("diluted_eps") or it.get("eps"),
                "total_assets": it.get("total_assets"),
                "equity": it.get("total_hldr_eqy_exc_min_int"),
            }
            for it in express_preview
        ]

    out_core["tushare"] = {
        "income": income_core,
        "balance": balance_core,
        "cashflow": cashflow_core,
        "fina_indicator": fina_core,
        "valuation": daily_basic_core,
        "forecast": forecast_core,
        "express": express_core,
    }

    # 写出文件
    with open("full_fundamentals_sample.json", "w", encoding="utf-8") as f:
        json.dump(out_full, f, ensure_ascii=False, indent=2, default=str)
    with open("full_fundamentals_sample_core.json", "w", encoding="utf-8") as f:
        json.dump(out_core, f, ensure_ascii=False, indent=2, default=str)

    print("已写入 full_fundamentals_sample.json（全量） 与 full_fundamentals_sample_core.json（精简核心）")


if __name__ == "__main__":
    main()