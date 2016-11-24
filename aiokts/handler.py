from aiohttp import web
from aiohttp.web_exceptions import HTTPNotAcceptable, HTTPNotFound
import logging

logger = logging.getLogger('aiohttp.access')


class BaseView(web.View):
    def __init__(self, request):
        super().__init__(request)
        self.logger = logger

    @property
    def stores(self):
        return self.request.app.stores

    @property
    def default_get_method(self):
        return None

    @property
    def default_post_method(self):
        return None

    @property
    def get_methods(self):
        return {}

    @property
    def post_methods(self):
        return {}

    async def before_action(self):
        pass

    async def after_action(self):
        pass

    async def get(self):
        action_title = self.request.match_info['method']

        if not (action_title in self.get_methods):
            action_title = self.default_get_method

        if action_title in self.get_methods:
            await self.before_action()
            result = await self.get_methods.get(action_title)()
            await self.after_action()
        elif action_title in self.post_methods:
            raise HTTPNotAcceptable()
        else:
            raise HTTPNotFound()

        return result

    async def post(self):
        action_title = self.request.match_info['method']

        if not (action_title in self.post_methods):
            action_title = self.default_post_method

        if action_title in self.post_methods:
            await self.before_action()
            result = await self.post_methods.get(action_title)()
            await self.after_action()
        elif action_title in self.get_methods:
            raise HTTPNotAcceptable()
        else:
            raise HTTPNotFound()

        return result
