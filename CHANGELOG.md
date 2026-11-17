# Changelog

## 0.1.0

- Require published Anytext 9.3 for in-process output capture control.
- Add Ctrl+Alt+Arrow pane navigation for JupyterLab terminal keyboard encoding.
- Add optional `shell.mode=inprocess` for hosts using Anytext with output capture opt-out.
- Provide Anyshell terminals through Jupyverse's existing REST and WebSocket API.
- Register a standalone `shell` FPS plugin with configurable workspace and prompt.
- Keep sessions alive across browser reconnects and restore terminal screen state.
- Support multiple clients, resize negotiation, terminal permissions, and worker cleanup.
