from aiohttp.web_reqrep import Request


class KtsRequest(Request):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._context = None
        
    def set_context(self, ctx):
        self._context = ctx
        
    @property
    def ctx(self):
        return self._context
