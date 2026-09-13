import pytest

from collector.cli.app import build_parser, main


def test_parser_builds():
    assert build_parser().prog == "collector"


def test_main_without_command_exits_2():
    # 서브커맨드가 required=True라 없으면 argparse 자체가 종료한다.
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 2
