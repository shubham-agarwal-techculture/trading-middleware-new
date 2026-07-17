import nifty_signal_bridge


def test_filled_positions_show_pnl_values():
    position = {
        "status": "FILLED",
        "qty": 2,
        "side": "BUY",
        "entry_price": 100.0,
        "current_ltp": 110.0,
    }

    display = nifty_signal_bridge.get_position_display_values(position)

    assert display["kind"] == "pnl"
    assert display["value"] == 20.0


def test_pending_positions_show_ltp_and_underlying():
    position = {
        "status": "PENDING",
        "qty": 1,
        "current_ltp": 105.5,
        "underlying_price": 22100.0,
    }

    display = nifty_signal_bridge.get_position_display_values(position)

    assert display["kind"] == "market"
    assert display["ltp"] == 105.5
    assert display["underlying"] == 22100.0
