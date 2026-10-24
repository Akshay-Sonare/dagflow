from my_project.cli import main
from my_project.core import greet


def test_greet_default():
    assert greet() == "Hello, World!"


def test_greet_name():
    assert greet("Akshay") == "Hello, Akshay!"


def test_cli(capsys):
    assert main(["GitHub"]) == 0
    assert capsys.readouterr().out.strip() == "Hello, GitHub!"
