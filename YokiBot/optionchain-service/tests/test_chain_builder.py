from app.option_chain_service import build_option_chain

def test_build_option_chain_basic():
    instruments = [
        {"instrument_key": "1", "strike": 100, "opt_type": "CE"},
        {"instrument_key": "2", "strike": 100, "opt_type": "PE"},
    ]

    snapshot = {
        "data": {
            "1": {"market_data": {"last_traded_price": 10, "oi": 100}},
            "2": {"market_data": {"last_traded_price": 12, "oi": 120}},
        }
    }

    chain = build_option_chain(
        underlying="NIFTY",
        expiry="2025-01-30",
        instruments=instruments,
        snapshot=snapshot,
        spot=100
    )

    assert chain["underlying"] == "NIFTY"
    assert chain["expiry"] == "2025-01-30"
    assert chain["spot"] == 100
    assert "pcr" in chain
    assert len(chain["instruments"]) == 2
