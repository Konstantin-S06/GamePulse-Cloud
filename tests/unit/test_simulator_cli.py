from gamepulse.simulator.cli import main


def test_cli_roundtrip(tmp_path):
    out = tmp_path / "events.jsonl"
    args = [
        "--run-date",
        "2026-01-05",
        "--num-matches",
        "2",
        "--num-players",
        "300",
        "--seed",
        "42",
        "--out",
        str(out),
    ]
    assert main(args) == 0
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) > 0
    assert main(args) == 0
    assert out.read_text(encoding="utf-8").splitlines() == lines
