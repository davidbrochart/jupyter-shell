"""Implement Jupyverse's authenticated terminal routes."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import anyio
from fastapi import HTTPException, Response
from jupyverse_terminals import Terminal, Terminals

from .config import ShellConfig
from .server import ShellServer

log = logging.getLogger(__name__)


class ShellTerminals(Terminals):
    def __init__(self, app, auth, config: ShellConfig):
        super().__init__(app, auth)
        self.config = config
        self.terminals: dict[str, tuple[Terminal, ShellServer]] = {}
        self.stop_event = anyio.Event()
        self.counter = 0

    async def start(self, *, task_status=anyio.TASK_STATUS_IGNORED):
        async with anyio.create_task_group() as self.task_group:
            task_status.started()
            await self.stop_event.wait()

    async def stop(self):
        servers = [server for _, server in self.terminals.values()]
        for server in servers:
            server.quit()
        for server in servers:
            await server.finished.wait()
        self.terminals.clear()
        self.stop_event.set()

    async def get_terminals(self, user):
        return [info for info, _ in self.terminals.values()]

    async def create_terminal(self, user):
        self.counter += 1
        name = str(self.counter)
        info = Terminal(name=name, last_activity=datetime.now(UTC).isoformat())
        server = ShellServer(self.config)
        self.terminals[name] = (info, server)
        try:
            with anyio.fail_after(self.config.startup_timeout):
                await self.task_group.start(self._run, name, server)
        except BaseException as error:
            server.quit()
            self.terminals.pop(name, None)
            if isinstance(error, Exception):
                log.exception("Unable to start Anyshell terminal")
                raise HTTPException(503, "Unable to start Anyshell terminal") from error
            raise
        return info

    async def _run(self, name, server, *, task_status=anyio.TASK_STATUS_IGNORED):
        started = False

        class Ready:
            def started(self, value=None):
                nonlocal started
                started = True
                task_status.started(value)

        try:
            await server.run(task_status=Ready())
        except Exception:
            if not started:
                raise
            log.exception("Anyshell terminal failed", extra={"terminal": name})
        finally:
            self.terminals.pop(name, None)

    async def delete_terminal(self, name, user):
        entry = self.terminals.pop(name, None)
        if entry:
            entry[1].quit()
            await entry[1].finished.wait()
        return Response(status_code=204)

    async def terminal_websocket(self, name, websocket_permissions):
        if websocket_permissions is None:
            return
        websocket, permissions = websocket_permissions
        if name not in self.terminals:
            await websocket.close(code=1008)
            return
        info, server = self.terminals[name]
        info.last_activity = datetime.now(UTC).isoformat()
        await websocket.accept()
        await server.serve(websocket, permissions)
