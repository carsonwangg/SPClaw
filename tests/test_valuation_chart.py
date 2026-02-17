import pandas as pd

from coatue_claw.valuation_chart import _compute_ntm_from_yahoo_estimates


def test_compute_ntm_from_yahoo_estimates_imputed():
    df = pd.DataFrame(
        {"avg": [100.0, 120.0, 500.0, 600.0]},
        index=["0q", "+1q", "0y", "+1y"],
    )
    ntm, method, flags = _compute_ntm_from_yahoo_estimates(df)
    assert method == "imputed"
    assert ntm is not None
    # 100 + 120 + 2/3*(600-120) = 540
    assert round(ntm, 4) == 540.0
    assert "ntm_imputed_from_0q_1q_1y" in flags


def test_compute_ntm_from_yahoo_estimates_missing():
    df = pd.DataFrame({"avg": [100.0]}, index=["0q"])
    ntm, method, flags = _compute_ntm_from_yahoo_estimates(df)
    assert ntm is None
    assert method == "missing"
    assert "missing_revenue_estimates" in flags
