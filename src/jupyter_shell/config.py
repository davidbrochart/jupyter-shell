"""Plugin configuration."""

from pathlib import Path
from typing import Literal

from jupyverse_api import Config
from pydantic import Field


class ShellConfig(Config):
    mode: Literal["subprocess", "inprocess"] = "subprocess"
    root_dir: Path = Path(".")
    prompt_position: Literal["inline", "bottom"] = "inline"
    startup_timeout: float = Field(default=30, gt=0)
