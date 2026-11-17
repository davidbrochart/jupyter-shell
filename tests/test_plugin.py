"""Exercise the public Jupyverse API with real Anyshell worker processes."""

import sys
from contextlib import asynccontextmanager
from importlib.metadata import entry_points

import anyio
import pytest
from fastapi import FastAPI, WebSocket
from fps import Module, get_root_module
from ghostty import Terminal
from jupyverse_api import App
from jupyverse_auth import Auth, User
from jupyverse_terminals import Terminals
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from jupyter_shell.config import ShellConfig
from jupyter_shell.routes import ShellTerminals
from jupyter_shell.server import valid_size


class TestAuth(Auth):
    __test__ = False

    def __init__(self, execute=True):
        self.execute = execute

    def current_user(self, permissions=None):
        async def user():
            return User(username="test")

        return user

    def websocket_auth(self, permissions=None):
        async def websocket(websocket: WebSocket):
            allowed = ["read", "execute"] if self.execute else ["read"]
            return websocket, {"terminals": allowed}

        return websocket

    async def update_user(self):
        raise NotImplementedError


@pytest.fixture(params=["subprocess", "inprocess"])
def mode(request):
    return request.param


def assert_alive(server):
    assert not server.finished.is_set()
    if server.process is not None:
        assert server.process.returncode is None


def assert_closed(server):
    assert server.finished.is_set()
    if server.process is not None:
        assert server.process.returncode is not None


def make_app(root, execute=True, prompt="inline", mode="subprocess"):
    @asynccontextmanager
    async def lifespan(app):
        async with anyio.create_task_group() as task_group:
            await task_group.start(terminals.start)
            try:
                yield
            finally:
                await terminals.stop()

    app = FastAPI(lifespan=lifespan)
    terminals = ShellTerminals(
        App(app),
        TestAuth(execute),
        ShellConfig(root_dir=root, prompt_position=prompt, mode=mode),
    )
    app.state.terminals = terminals
    return app


def until(websocket, emulator, expected):
    for _ in range(2000):
        message = websocket.receive_json()
        if message[0] == "stdout":
            emulator.feed(message[1])
            if expected in emulator.text():
                return emulator.text()
        elif message[0] == "disconnect":
            pytest.fail("Terminal disconnected before rendering " + expected)
    pytest.fail("Terminal did not render " + expected)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@pytest.mark.parametrize("prompt", ["inline", "bottom"])
def test_commands_resize_reconnect_and_delete(tmp_path, backend, prompt, mode):
    (tmp_path / "hello.txt").write_text("content-from-worker\n")
    app = make_app(tmp_path, prompt=prompt, mode=mode)
    with (
        TestClient(app, backend=backend) as client,
        Terminal(cols=100, rows=30) as screen,
    ):
        assert client.get("/api/terminals").json() == []
        response = client.post("/api/terminals")
        assert response.status_code == 200, response.text
        name = response.json()["name"]
        server = app.state.terminals.terminals[name][1]
        with client.websocket_connect(f"/terminals/websocket/{name}") as websocket:
            assert websocket.receive_json() == ["setup", {}]
            websocket.send_json(["set_size", 30, 100])
            websocket.send_json(
                ["stdin", "write created.txt saved-through-anyshell; cat hello.txt\r"]
            )
            until(websocket, screen, "content-from-worker")
        assert_alive(server)
        assert (
            tmp_path / "created.txt"
        ).read_text().strip() == "saved-through-anyshell"
        assert client.get("/api/terminals").json()[0]["name"] == name
        with client.websocket_connect(f"/terminals/websocket/{name}") as websocket:
            assert websocket.receive_json() == ["setup", {}]
            # The initial snapshot, rather than a reissued command, restores output.
            with Terminal(cols=100, rows=30) as restored:
                until(websocket, restored, "content-from-worker")
        assert client.delete(f"/api/terminals/{name}").status_code == 204
        assert_closed(server)
        assert client.get("/api/terminals").json() == []
    assert_closed(server)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_exit_and_server_shutdown(tmp_path, backend, mode):
    app = make_app(tmp_path, mode=mode)
    with TestClient(app, backend=backend) as client:
        first = client.post("/api/terminals").json()["name"]
        second = client.post("/api/terminals").json()["name"]
        first_server = app.state.terminals.terminals[first][1]
        second_server = app.state.terminals.terminals[second][1]
        with client.websocket_connect(f"/terminals/websocket/{first}") as websocket:
            websocket.send_json(["stdin", "exit\r"])
            while websocket.receive_json()[0] != "disconnect":
                pass
        assert_closed(first_server)
        if first_server.process is not None:
            assert first_server.process.returncode == 0
        assert [t["name"] for t in client.get("/api/terminals").json()] == [second]
    assert_closed(second_server)


def test_read_only_and_invalid_size(tmp_path, mode):
    app = make_app(tmp_path, execute=False, mode=mode)
    with TestClient(app) as client, Terminal(cols=80, rows=24) as screen:
        name = client.post("/api/terminals").json()["name"]
        server = app.state.terminals.terminals[name][1]
        with client.websocket_connect(f"/terminals/websocket/{name}") as websocket:
            websocket.send_json(["stdin", "write forbidden.txt no\r"])
            websocket.send_json(["set_size", 40, 120])
            until(websocket, screen, "/")
            assert server.driver_size == (80, 24)
        assert not (tmp_path / "forbidden.txt").exists()
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/terminals/websocket/missing"):
                pytest.fail("Missing terminal was accepted")


def test_discovery():
    for group in ("fps.modules", "jupyverse.modules"):
        entries = [ep for ep in entry_points(group=group) if ep.name == "shell"]
        assert len(entries) == 1
        assert entries[0].load().__name__ == "ShellModule"


@pytest.mark.parametrize("size", [(0, 20), (True, 20), (501, 2), (500, 500), (1.5, 20)])
def test_size_limits(size):
    assert not valid_size(*size)
    assert valid_size(80, 24)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_shared_dimensions_and_resize_on_detach(tmp_path, backend, mode):
    app = make_app(tmp_path, mode=mode)
    with TestClient(app, backend=backend) as client:
        name = client.post("/api/terminals").json()["name"]
        server = app.state.terminals.terminals[name][1]

        async def resized(expected):
            with anyio.fail_after(5):
                while server.driver_size != expected:
                    await anyio.sleep(0.01)

        with client.websocket_connect(f"/terminals/websocket/{name}") as first:
            first.send_json(["set_size", 30, 100])
            client.portal.call(resized, (100, 30))
            with client.websocket_connect(f"/terminals/websocket/{name}") as second:
                second.send_json(["set_size", 40, 90])
                client.portal.call(resized, (90, 30))
                second.send_json(["set_size", 0, 0])
                assert server.driver_size == (90, 30)
            client.portal.call(resized, (100, 30))


@pytest.mark.parametrize("invalid_root", ["missing", "file"])
def test_startup_failure_isolated(tmp_path, invalid_root, mode):
    root = tmp_path / invalid_root
    if invalid_root == "file":
        root.write_text("not a directory")
    app = make_app(root, mode=mode)
    with TestClient(app) as client:
        assert client.post("/api/terminals").status_code == 503
        assert client.get("/api/terminals").json() == []
        app.state.terminals.config.root_dir = tmp_path
        name = client.post("/api/terminals").json()["name"]
        assert client.delete(f"/api/terminals/{name}").status_code == 204


class ProviderModule(Module):
    async def prepare(self):
        self.put(App(FastAPI()), App)
        self.put(TestAuth(), Auth)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_fps_lifecycle(tmp_path, backend, mode):
    async def run():
        root = get_root_module(
            {
                "root": {
                    "type": "fps:Module",
                    "modules": {
                        "provider": {"type": "test_plugin:ProviderModule"},
                        "shell": {
                            "type": "shell",
                            "config": {"root_dir": str(tmp_path), "mode": mode},
                        },
                    },
                }
            }
        )
        root._global_start_timeout = 10
        root._global_stop_timeout = 10
        async with root:
            terminals = await root.modules["shell"].get(Terminals)
            info = await terminals.create_terminal(User())
            server = terminals.terminals[info.name][1]
            assert_alive(server)
        assert server.finished.is_set()
        assert_closed(server)

    anyio.run(run, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@pytest.mark.parametrize("close_order", [(0, 1), (1, 0)])
def test_inprocess_independent_sessions_and_host_streams(
    tmp_path, backend, close_order, monkeypatch
):
    output, errors = sys.stdout, sys.stderr

    def no_process(*args, **kwargs):
        raise AssertionError("In-process mode spawned a process")

    monkeypatch.setattr(anyio, "open_process", no_process)
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "first.txt").write_text("first-session-only")
    (tmp_path / "second.txt").write_text("second-session-only")
    app = make_app(tmp_path, mode="inprocess")
    with TestClient(app, backend=backend) as client:
        names = [client.post("/api/terminals").json()["name"] for _ in range(2)]
        servers = [app.state.terminals.terminals[name][1] for name in names]
        assert sys.stdout is output
        assert sys.stderr is errors
        for index, command, expected in (
            (0, "cd sub; cat first.txt", "first-session-only"),
            (1, "cat second.txt", "second-session-only"),
        ):
            with client.websocket_connect(
                f"/terminals/websocket/{names[index]}"
            ) as websocket:
                websocket.send_json(["stdin", command + "\r"])
                with Terminal(cols=80, rows=24) as screen:
                    until(websocket, screen, expected)
        assert servers[0].app.session.cwd == "/sub"
        assert servers[1].app.session.cwd == "/"
        for index in close_order:
            assert client.delete(f"/api/terminals/{names[index]}").status_code == 204
            assert_closed(servers[index])
            assert sys.stdout is output
            assert sys.stderr is errors
        assert all(server.process is None for server in servers)
    assert sys.stdout is output
    assert sys.stderr is errors


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_inprocess_delete_cancels_jobs(tmp_path, backend):
    app = make_app(tmp_path, mode="inprocess")
    with TestClient(app, backend=backend) as client:
        name = client.post("/api/terminals").json()["name"]
        server = app.state.terminals.terminals[name][1]
        with client.websocket_connect(f"/terminals/websocket/{name}") as websocket:
            websocket.send_json(["stdin", "sleep 30 &\r"])

            async def started():
                with anyio.fail_after(5):
                    while not server.app.session.jobs:
                        await anyio.sleep(0.01)
                return next(iter(server.app.session.jobs.values()))

            job = client.portal.call(started)
            assert not job.done.is_set()
            assert client.delete(f"/api/terminals/{name}").status_code == 204
            assert job.done.is_set()
            assert_closed(server)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_jupyterlab_pane_shortcuts(tmp_path, backend):
    app = make_app(tmp_path, mode="inprocess")
    with TestClient(app, backend=backend) as client:
        name = client.post("/api/terminals").json()["name"]
        server = app.state.terminals.terminals[name][1]
        from anyshell.panes import ShellWorkspace

        workspace = server.app.query_one(ShellWorkspace)
        first = workspace.active

        async def pane_count(count):
            with anyio.fail_after(5):
                while len(workspace.panes) != count:
                    await anyio.sleep(0.01)
                while workspace.active is not workspace.panes[-1]:
                    await anyio.sleep(0.01)
                while not workspace.active.query_one("CommandInput").has_focus:
                    await anyio.sleep(0.01)

        async def focused(pane):
            with anyio.fail_after(5):
                while workspace.active is not pane:
                    await anyio.sleep(0.01)

        with client.websocket_connect(f"/terminals/websocket/{name}") as websocket:
            websocket.send_json(["stdin", "\x1bv"])
            client.portal.call(pane_count, 2)
            right = workspace.active
            assert right is not first
            websocket.send_json(["stdin", "\x1b[1;7D"])
            client.portal.call(focused, first)
            websocket.send_json(["stdin", "\x1b[1;7C"])
            client.portal.call(focused, right)
            websocket.send_json(["stdin", "\x1bh"])
            client.portal.call(pane_count, 3)
            below = workspace.active
            assert below is not right
            websocket.send_json(["stdin", "\x1b[1;7A"])
            client.portal.call(focused, right)
            websocket.send_json(["stdin", "\x1b[1;7B"])
            client.portal.call(focused, below)
