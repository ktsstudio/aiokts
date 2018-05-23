import argparse
import collections
import copy
import os

import sys

import logging
import logging.config
from logging.handlers import RotatingFileHandler

import yaml

__all__ = (
    'Settings',
)


def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


class Settings:
    def __init__(self, default_config_dir=None, local_config_dir=None, **kwargs):
        self.default_config_dir = default_config_dir
        self.local_config_dir = local_config_dir

        self.config_dir = kwargs.get('config_dir')

        self.config_path = kwargs.get('config_path')
        self.local_config_path = kwargs.get('local_config_path')
        self.logging_path = kwargs.get('logging_path')
        self.log_path = kwargs.get('log_path')

        self.root_config = kwargs.get('root_config')
        self.local_config = kwargs.get('local_config')
        self.config = None

    def parse(self):
        args = self.parse_arguments()

        is_config_by_args = args.config_dir is not None
        self.config_dir = args.config_dir if is_config_by_args else self.default_config_dir

        if self.config_path is None or is_config_by_args:
            self.config_path = os.path.join(self.config_dir, 'main', 'main.yaml')

        self.logging_path = args.logging_config if args.logging_config else self.logging_path
        if self.logging_path is None and self.config_dir:
            self.logging_path = os.path.join(self.config_dir, 'api_hour', 'logging.ini')

        self.root_config = self.parse_config(self.config_path)

        if not self.local_config_path and self.local_config_dir:
            self.local_config_path = os.path.join(self.local_config_dir, 'main', 'main.yaml')
        if self.local_config_path and os.path.exists(self.local_config_path):
            self.local_config = self.parse_config(self.local_config_path)

        self.config = copy.deepcopy(self.root_config)
        if self.local_config is not None:
            self.config = update(self.config, self.local_config)

        self.log_path = args.log

    def parse_arguments(self):
        parser = argparse.ArgumentParser()

        parser.add_argument(
            '--config_dir',
            type=str,
            help='config dir path',
            default=None
        )
        parser.add_argument(
            '--logging_config',
            type=str,
            help='logging config path',
            default=None
        )
        parser.add_argument(
            '--log',
            type=str,
            help='log file path',
            default=None
        )

        args, left = parser.parse_known_args()
        sys.argv = sys.argv[:1] + left

        if args.config_dir is not None:
            assert os.path.isdir(args.config_dir), \
                'config_dir `{}` does not exist'.format(args.config_dir)

        if args.logging_config is not None:
            assert os.path.exists(args.logging_config), \
                'logging_config `{}` does not exist'.format(
                    args.logging_config)

        return args

    def parse_config(self, path=None):
        if path is None:
            path = self.config_path
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.load(f)

    def setup_logging(self):
        if self.logging_path:
            logging.config.fileConfig(
                self.logging_path,
                disable_existing_loggers=False
            )

        if self.log_path is not None:
            old_file_handler = None
            for handler in logging.root.handlers[:]:
                if isinstance(handler, logging.FileHandler):
                    old_file_handler = handler
                    logging.root.removeHandler(handler)
            if old_file_handler is not None:
                file_handler = RotatingFileHandler(self.log_path, 'w+', 104857600, 100)
                file_handler.setFormatter(old_file_handler.formatter)
                logging.root.addHandler(file_handler)

    def __getitem__(self, name):
        return self.config[name]

    def get(self, *args, **kwargs):
        try:
            result = self
            for key in args:
                result = result[key]
        except:
            for key in ['default', 'fallback']:
                if key in kwargs:
                    return kwargs[key]
            return None

        return result

    def getint(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def getfloat(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def getboolean(self, *args, **kwargs):
        return self.get(*args, **kwargs)

