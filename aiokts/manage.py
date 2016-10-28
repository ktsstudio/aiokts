#! /usr/bin/env python
import argparse
import logging
import os
import pkgutil
import sys
import traceback

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURRENT_DIR)


class Manage(object):
    __commands_package_path__ = os.path.join(CURRENT_DIR, 'commands')
    _modules = {}
    _commands = None

    @property
    def commands(self):
        if self._commands is None:
            self._commands = ['help']
            for loader, name, ispkg in pkgutil.iter_modules([self.__commands_package_path__]):
                if not ispkg:
                    self._commands.append(name)
                    self._modules[name] = loader.find_module(name)  # .load_module(name)
        return self._commands

    def help(self):
        print('Available commands:\n - %s' % ('\n - '.join(self.commands)))

    def run(self):
        args = self._parse_manage_arguments()
        # if args.config is not None:
        #     settings.parse_config(args.config)
        command = args.command
        if command not in iter(self.commands):
            logging.error('Command %s not found' % command)
            self.help()
            return 1
        try:
            if command == 'help':
                self.help()
                return 0
            self._run_command(command, *args.opts)
        except Exception:
            traceback.print_exc()
            return 2
        except BaseException:
            traceback.print_exc()
            return 3

    def _run_command(self, command, *args):
        module = self._modules[command].load_module(command)
        if hasattr(module, 'main'):
            module.main(*args)

    def _parse_manage_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('command', help='command to execute')
        parser.add_argument('--config', type=str, help='config path', default=None)
        parser.add_argument('opts', nargs=argparse.REMAINDER, default=None)
        args = parser.parse_args()
        return args


if __name__ == '__main__':
    manage = Manage()
    exit(manage.run())
