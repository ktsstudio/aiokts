import argparse
import inspect
import logging
import logging.config
import os
import pkgutil
import sys

from aiokts.managecommands import Command
from aiokts.store import Store

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURRENT_DIR)


class BaseManage(object):
    commands_package_path = None
    store_cls = Store
    _modules = {}
    _commands = None

    def __init__(self):
        self._config_dir = None
        self._config_path = None
        self._config = None
        self._logger = None

        assert self.commands_package_path is not None, \
            'Must specify path to where commands are'
        self.commands_package_path = os.path.abspath(
            os.path.join(
                os.path.dirname(inspect.getfile(self.__class__)),
                self.commands_package_path))
        self.logger.debug('Commands path: %s', self.commands_package_path)

    @property
    def commands(self):
        if self._commands is None:
            self._commands = ['help']
            for loader, name, ispkg in \
                    pkgutil.iter_modules([self.commands_package_path]):
                if not ispkg:
                    self._commands.append(name)
                    self._modules[name] = loader.find_module(name)
        return self._commands

    @property
    def config(self):
        return {}

    def help(self):
        print('Available commands:\n - %s' % ('\n - '.join(self.commands)))

    def run(self):
        args = self._parse_manage_arguments()

        command = None
        try:
            command = args.command
            if command not in self.commands:
                logging.error('Command %s not found' % command)
                self.help()
                return 1

            if command == 'help':
                self.help()
                return 0

            self._run_command(command, *args.opts)
        except Exception:
            self.logger.exception('Exception while running command %s',
                                  command)
            return 2
        except BaseException:
            self.logger.exception('Exception while running command %s',
                                  command)
            return 3

    def _run_command(self, command, *args):
        module = self._modules[command].load_module(command)
        if hasattr(module, 'main'):
            module.main(*args)

        cmd_cls = None
        for name, cls in module.__dict__.items():
            if isinstance(cls, type) and issubclass(cls, Command)\
                    and cls.__module__ == module.__name__:
                cmd_cls = cls
                break

        assert cmd_cls is not None, \
            "Couldn't find Command in command {}".format(command)
        cmd = cmd_cls(self)
        cmd.run(*args)

    def _parse_manage_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('command', help='command to execute')
        parser.add_argument('opts', nargs=argparse.REMAINDER, default=None)
        args = parser.parse_args()

        return args

    @property
    def logger(self):
        if self._logger is None:
            self._logger = logging.getLogger('Manager')
        return self._logger


def main(manager_cls):
    manage = manager_cls()
    exit(manage.run())


if __name__ == '__main__':
    main(Manage)
