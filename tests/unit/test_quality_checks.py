from gamepulse.quality.checks import CheckResult, run_all_checks


def test_check_result_to_dict():
    r = CheckResult("C01", "critical", True, 1, ">0", "ok")
    d = r.to_dict()
    assert d["name"] == "C01"
    assert d["passed"] is True


def test_run_all_checks_returns_18():
    from unittest.mock import MagicMock

    con = MagicMock()
    con.execute.return_value.fetchone.return_value = (0,)
    con.execute.return_value.fetchall.return_value = []
    results = run_all_checks(con, "2026-01-05", 0.05)
    assert len(results) == 18
    assert {r.severity for r in results} <= {"critical", "warn"}
