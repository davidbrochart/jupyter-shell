"""Anyshell UI adapted to JupyterLab's terminal keyboard encoding."""

from anyshell.frontend import ShellApp
from anytext.binding import Binding


class TerminalShellApp(ShellApp):
    BINDINGS = [
        Binding("alt+ctrl+right", "focus_pane('right')", "Focus pane", priority=True),
        Binding("alt+ctrl+left", "focus_pane('left')", show=False, priority=True),
        Binding("alt+ctrl+up", "focus_pane('up')", show=False, priority=True),
        Binding("alt+ctrl+down", "focus_pane('down')", show=False, priority=True),
        # xterm.js maps Alt+Arrow to Ctrl+Arrow. Keep the original binding, but
        # advertise the shortcut that survives JupyterLab's encoding instead.
        Binding("alt+right", "focus_pane('right')", show=False, priority=True),
    ]
