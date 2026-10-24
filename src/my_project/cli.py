import argparse

from my_project import __version__
from my_project.core import greet


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="my-project", description="Say hello.")
    parser.add_argument("name", nargs="?", default="World", help="who to greet")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    args = parser.parse_args(argv)
    print(greet(args.name))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
