from gamepulse.common.logging_setup import get_logger


def test_no_duplicate_handlers():
    name = "gamepulse.test.logging.unique"
    first = get_logger(name)
    second = get_logger(name)
    assert first is second
    assert len(first.handlers) == 1
