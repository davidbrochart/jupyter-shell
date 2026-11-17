# jupyter-shell

A [Jupyverse](https://github.com/jupyter-server/jupyverse) terminal plugin powered by [Anyshell](https://github.com/davidbrochart/anyshell).

## Install and run

With `pip`:

```sh
pip install 'jupyverse[jupyterlab,auth]'
pip install jupyter-shell
# Jupyverse comes with its terminal plugin, disable it to use ours:
jupyverse --disable terminals
```

Open a terminal from JupyterLab's launcher.

For a development setup, you can use `uv`:

```sh
uv run --with 'jupyverse[jupyterlab,auth]' jupyverse --disable terminals
```

## Execution mode

The default `shell.mode=subprocess` runs each terminal in an isolated worker.
An optional `inprocess` mode runs each Anyshell UI directly in an AnyIO task owned
by the plugin, without starting a process or using pipes:

```sh
jupyverse --disable terminals --set shell.mode=inprocess
```
