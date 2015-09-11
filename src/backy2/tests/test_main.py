import pytest
import sys
import backy2.main


@pytest.yield_fixture
def argv():
    original = sys.argv
    new = original[:1]
    sys.argv = new
    yield new
    sys.argv = original



def test_display_usage(capsys, argv):
    with pytest.raises(SystemExit) as exit:
        backy2.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert """\
usage: py.test [-h] [-v] [-b BACKUPDIR] {backup,restore,scrub} ...
""" == out
    assert err == ""
