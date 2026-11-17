"""FPS module providing Jupyverse's existing terminal API."""

from anyio import create_task_group
from fps import Module
from jupyverse_api import App
from jupyverse_auth import Auth
from jupyverse_terminals import Terminals

from .config import ShellConfig
from .routes import ShellTerminals


class ShellModule(Module):
    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.config = ShellConfig(**kwargs)

    async def prepare(self) -> None:
        app = await self.get(App)
        auth = await self.get(Auth)
        terminals = ShellTerminals(app, auth, self.config)
        async with create_task_group() as task_group:
            await task_group.start(terminals.start)
            self.put(terminals, Terminals, teardown_callback=terminals.stop)
            self.done()
