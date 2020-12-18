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
    parser.add_argument('-mq', '--host', type=str, default=os.environ("RABBITMQ_HOST"),
                        help="host to rabbitMQ, can be set in RABBITMQ_HOST env var")
    parser.add_argument('-pq', '--port', type=str, default=os.environ("RABBITMQ_PORT"),
                        help="port to rabbitMQ, can be set in RABBITMQ_PORT env var")
    parser.add_argument('-u', '--user', type=str, default=os.environ("RABBITMQ_USER"),
                        help="Login to rabbitMQ host, can be set in RABBITMQ_USER env var")
    parser.add_argument('-p', '--password', type=str, default=os.environ("RABBITMQ_PWD"),
                        help="Password to rabbitMQ host, can be set in RABBITMQ_PWD env var")
    parser.add_argument('-mq', '--manager_queue', type=str, default=os.environ("RABBITMQ_MQ"),
                        help="Rabbit manager queue name, can be set in RABBITMQ_MQ env var")
    parser.add_argument('-lq', '--light_queue', type=str, default=os.environ("RABBITMQ_LQ"),
                        help="Rabbit light workers queue name, can be set in RABBITMQ_LQ env var")
    parser.add_argument('-hq', '--heavy_queue', type=str, default=os.environ("RABBITMQ_HQ"),
                        help="Rabbit heavy worker queue name, can be set in RABBITMQ_HQ env var")
    parser.add_argument('-aq', '--all_queue', type=str, default=os.environ("RABBITMQ_AQ"),
                        help="Rabbit all workers queue name, can be set in RABBITMQ_AQ env var")
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
