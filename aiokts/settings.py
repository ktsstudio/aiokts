import argparse
import copy
import os

import sys

import logging
import logging.config
import yaml


__all__ = (
    'Settings',
)


class Settings:
    def __init__(self, default_config_dir=None, local_config_dir=None):
        self.default_config_dir = default_config_dir
        self.local_config_dir = local_config_dir

        self.config_dir = None
        self.config_path = None
        self.logging_path = None
        self.root_config = None
        self.local_config = None
        self.config = None

    def parse(self):
        args = self.parse_arguments()
        self.config_dir = args.config_dir
        if self.config_dir is None:
            self.config_dir = self.default_config_dir
        is_config_by_args = args.config_dir is not None

        self.config_path = os.path.join(self.config_dir, 'main', 'main.yaml')
        self.logging_path = args.logging_config
        if self.logging_path is None:
            self.logging_path = \
                os.path.join(self.config_dir, 'api_hour', 'logging.ini')

        self.root_config = self.parse_config(self.config_path)

        local_conf_path = os.path.join(
            self.local_config_dir, 'main', 'main.yaml')
        if not is_config_by_args and os.path.exists(local_conf_path):
            self.local_config = self.parse_config(local_conf_path)

        self.config = copy.deepcopy(self.root_config)
        if self.local_config is not None:
            self.config.update(self.local_config)

    def parse_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--config_dir',
                            type=str,
                            help='config dir path',
                            default=None)
        parser.add_argument('--logging_config',
                            type=str,
                            help='logging config path',
                            default=None)
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
        with open(path, 'r') as f:
            return yaml.load(f)

    def setup_logging(self):
        logging.config.fileConfig(
            self.logging_path,
            disable_existing_loggers=False
        )
