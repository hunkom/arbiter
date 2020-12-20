#!/usr/bin/python
# coding=utf-8
""" Worker """

import os

from argparse import ArgumentParser


def arg_parse():
    parser = ArgumentParser(description='Arbiter worker processot')
    parser.add_argument('-a', '--app', type=str, default=os.environ("ARBITER_TASKS"),
                        help="Name of installed package to get tasks from")
    parser.add_argument('-w', '--workers', type=int, default=os.environ("ARBITER_WORKER_COUNT", 1),
                        help="Quantity of workers to be spawned. Default: 1")
    parser.add_argument('-t', '--worker_type', type=str, default=os.environ("ARBITER_WORKER_TYPE"),
                        help="Type of a worker for arbiter [heavy, light], can be set in ARBITER_WORKER_TYPE env var")
    args, _ = parser.parse_known_args()
    return args


def main():
    """ Main thread: start all and listen for broadcasts """
    # Parse settings
    settings = arg_parse()


if __name__ == "__main__":
    main()
