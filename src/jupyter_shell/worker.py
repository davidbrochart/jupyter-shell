"""Isolated Anyshell UI, connected to the server through JSON lines."""

from __future__ import annotations

import json
import os
import sys

# Configure only this worker, before Anytext is imported.
os.environ["ANYTEXT_COLOR_SYSTEM"] = "truecolor"
os.environ["ANYTEXT_THREADS"] = "0"

import anyio  # noqa: E402
from anyshell import LocalFileSystem, Session  # noqa: E402
from anytext.drivers.callback_driver import CallbackDriver  # noqa: E402

from .app import TerminalShellApp  # noqa: E402


def send(message: list) -> None:
    # Anytext captures sys.stdout while running; the protocol owns original stdout.
    sys.__stdout__.write(json.dumps(message, ensure_ascii=True) + "\n")
    sys.__stdout__.flush()


async def main() -> None:
    driver = None

    def make_driver(app, **options):
        nonlocal driver
        driver = CallbackDriver(
            app, output=lambda text: send(["stdout", text]), **options
        )
        return driver

    class TerminalApp(TerminalShellApp):
        def on_mount(self):
            self.call_after_refresh(lambda: send(["ready"]))

    app = TerminalApp(
        Session(LocalFileSystem(sys.argv[1])),
        prompt_position=sys.argv[2],
        driver_class=make_driver,
    )

    async def read_input() -> None:
        async with anyio.wrap_file(sys.__stdin__) as stream:
            while line := await stream.readline():
                message = json.loads(line)
                if message[0] == "shutdown":
                    break
                if driver is not None:
                    if message[0] == "stdin":
                        driver.feed_input(message[1])
                    elif message[0] == "set_size":
                        driver.resize(message[1], message[2])
        app.exit()

    async with anyio.create_task_group() as task_group:
        # EOF and a shutdown request also let a blocked input worker finish.
        task_group.start_soon(read_input)
        await app.run_async(size=(80, 24))
        send(["exit"])
        task_group.cancel_scope.cancel()


if __name__ == "__main__":
    anyio.run(main)
