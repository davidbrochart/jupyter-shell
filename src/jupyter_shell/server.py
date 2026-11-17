"""Persistent Anyshell sessions with bounded per-client WebSocket output."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from dataclasses import dataclass
from typing import Any

import anyio
from anyio.abc import TaskStatus
from anyio.streams.text import TextReceiveStream
from fastapi import WebSocket, WebSocketDisconnect
from ghostty import Terminal as Emulator
from jupyverse_terminals import TerminalServer

from .config import ShellConfig

log = logging.getLogger(__name__)


def valid_size(columns: Any, rows: Any) -> bool:
    return (
        type(columns) is int
        and type(rows) is int
        and 0 < columns <= 500
        and 0 < rows <= 500
        and columns * rows <= 100_000
    )


@dataclass(eq=False)
class Client:
    websocket: WebSocket
    send: Any
    receive: Any
    size: tuple[int, int] | None = None
    scope: anyio.CancelScope | None = None


class ShellServer(TerminalServer):
    def __init__(self, config: ShellConfig):
        self.config = config
        self.clients: dict[WebSocket, Client] = {}
        self.closed = False
        self.process = None
        self.app = None
        self.driver = None
        self.scope = None
        self.driver_size = (80, 24)
        self.emulator = Emulator(cols=80, rows=24)
        self.input_lock = anyio.Lock()
        self.finished = anyio.Event()
        self.stderr = ""

    async def _command(self, message: list) -> None:
        async with self.input_lock:
            if self.closed:
                return
            if self.driver is not None:
                if message[0] == "stdin":
                    self.driver.feed_input(message[1])
                elif message[0] == "set_size":
                    self.driver.resize(message[1], message[2])
                return
            if self.process and self.process.stdin:
                await self.process.stdin.send((json.dumps(message) + "\n").encode())

    async def _errors(self) -> None:
        async for text in TextReceiveStream(self.process.stderr):
            self.stderr = (self.stderr + text)[-8192:]

    async def run(self, *, task_status: TaskStatus = anyio.TASK_STATUS_IGNORED) -> None:
        try:
            if self.config.mode == "inprocess":
                await self._run_inprocess(task_status)
            else:
                await self._run_subprocess(task_status)
        finally:
            self.closed = True
            for client in list(self.clients.values()):
                client.send.close()
            self.emulator.close()
            if self.process:
                with anyio.CancelScope(shield=True):
                    await self.process.aclose()
            self.finished.set()

    async def _run_inprocess(self, task_status: TaskStatus) -> None:
        # Import here: subprocess mode keeps Anytext entirely out of the host.
        from anyshell import LocalFileSystem, Session
        from anytext.drivers.callback_driver import CallbackDriver

        from .app import TerminalShellApp

        def make_driver(app, **options):
            self.driver = CallbackDriver(app, output=self._output, **options)
            return self.driver

        class EmbeddedApp(TerminalShellApp):
            def on_mount(self):
                self.call_after_refresh(task_status.started)

        self.app = EmbeddedApp(
            Session(LocalFileSystem(self.config.root_dir)),
            prompt_position=self.config.prompt_position,
            driver_class=make_driver,
            capture_output=False,
        )
        with anyio.CancelScope() as self.scope:
            if self.closed:
                self.scope.cancel()
            await self.app.run_async(size=self.driver_size)

    async def _run_subprocess(self, task_status: TaskStatus) -> None:
        ready = False
        self.process = await anyio.open_process(
            [
                sys.executable,
                "-m",
                "jupyter_shell.worker",
                str(self.config.root_dir.resolve(strict=True)),
                self.config.prompt_position,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(self._errors)
            try:
                with anyio.CancelScope() as self.scope:
                    if self.closed:
                        self.scope.cancel()
                    buffer = ""
                    async for text in TextReceiveStream(self.process.stdout):
                        buffer += text
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            message = json.loads(line)
                            if message[0] == "stdout":
                                self._output(message[1])
                            elif message[0] == "ready" and not ready:
                                ready = True
                                task_status.started()
                            elif message[0] == "exit":
                                return
            finally:
                # Release the child's blocking input read before reaping it.
                with anyio.CancelScope(shield=True):
                    if self.process.stdin:
                        await self.process.stdin.aclose()
                    with anyio.move_on_after(3):
                        await self.process.wait()
                    if self.process.returncode is None:
                        self.process.kill()
                        await self.process.wait()
                    task_group.cancel_scope.cancel()
        if not ready:
            raise RuntimeError(f"Anyshell failed to start: {self.stderr}")

    def _output(self, text: str) -> None:
        self.emulator.feed(text)
        for client in list(self.clients.values()):
            try:
                client.send.send_nowait(["stdout", text])
            except (
                anyio.WouldBlock,
                anyio.ClosedResourceError,
                anyio.BrokenResourceError,
            ):
                # One slow client must not block the app or other clients.
                self.quit(client.websocket)

    async def _resize(self) -> None:
        sizes = [client.size for client in self.clients.values() if client.size]
        if sizes:
            size = (min(s[0] for s in sizes), min(s[1] for s in sizes))
            if size != self.driver_size:
                self.driver_size = size
                self.emulator.resize(*size)
                await self._command(["set_size", *size])

    async def _sender(self, client: Client, scope: anyio.CancelScope) -> None:
        try:
            async with client.receive:
                async for message in client.receive:
                    if message[0] == "stdout":
                        text = message[1]
                        for start in range(0, len(text), 8192):
                            with anyio.fail_after(5):
                                await client.websocket.send_json(
                                    ["stdout", text[start : start + 8192]]
                                )
                    else:
                        with anyio.fail_after(5):
                            await client.websocket.send_json(message)
        except Exception:
            log.debug("Terminal client disconnected", exc_info=True)
        finally:
            with anyio.CancelScope(shield=True):
                with anyio.move_on_after(1):
                    try:
                        await client.websocket.send_json(["disconnect", 1])
                        await client.websocket.close(code=1000 if self.closed else 1013)
                    except Exception:
                        pass
            scope.cancel()

    async def serve(self, websocket: WebSocket, permissions) -> None:
        if self.closed:
            await websocket.close(code=1000)
            return
        can_execute = permissions is None or "execute" in permissions.get(
            "terminals", []
        )
        send, receive = anyio.create_memory_object_stream(256)
        client = Client(websocket, send, receive)
        # No await between snapshotting and registration: output cannot be missed.
        snapshot = self.emulator.serialize().decode("utf-8")
        self.clients[websocket] = client
        send.send_nowait(["setup", {}])
        send.send_nowait(["stdout", snapshot])
        async with anyio.create_task_group() as task_group:
            client.scope = task_group.cancel_scope
            task_group.start_soon(self._sender, client, task_group.cancel_scope)
            try:
                while True:
                    message = await websocket.receive_json()
                    if (
                        not isinstance(message, list)
                        or not message
                        or not isinstance(message[0], str)
                    ):
                        continue
                    if message[0] == "stdin" and can_execute:
                        if (
                            len(message) == 2
                            and isinstance(message[1], str)
                            and len(message[1]) <= 65536
                        ):
                            await self._command(message)
                    elif message[0] == "set_size" and can_execute:
                        if len(message) >= 3 and valid_size(message[2], message[1]):
                            client.size = (message[2], message[1])
                            await self._resize()
            except (
                WebSocketDisconnect,
                anyio.BrokenResourceError,
                anyio.ClosedResourceError,
            ):
                pass
            finally:
                self.quit(websocket)
                with anyio.CancelScope(shield=True):
                    with anyio.move_on_after(1):
                        try:
                            await self._resize()
                        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
                            pass
                task_group.cancel_scope.cancel()

    def quit(self, websocket: WebSocket | None = None) -> None:
        if websocket is not None:
            client = self.clients.pop(websocket, None)
            if client:
                client.send.close()
                if client.scope:
                    client.scope.cancel()
            return
        self.closed = True
        if self.scope:
            self.scope.cancel()
