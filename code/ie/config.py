"""
Configuration management for the IE pipeline.

Handles data paths, LLM settings, and default configuration.
Reads from environment variables and provides sensible defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Project root: code/ is where this file lives
_CODE_DIR = Path(__file__).parent
_PROJECT_ROOT = _CODE_DIR.parent


@dataclass
class MIMICPaths:
    """Paths to MIMIC-IV data files."""
    mimic_iv_root: Path = _PROJECT_ROOT / "data" / "Mimic" / "mimic-iv-2.2" / "mimic-iv-2.2"
    mimic_note_root: Path = (
        _PROJECT_ROOT / "data" / "Mimic"
        / "mimic-iv-note-deidentified-free-text-clinical-notes-2.2"
        / "mimic-iv-note-deidentified-free-text-clinical-notes-2.2"
    )

    @property
    def hosp_dir(self) -> Path:
        return self.mimic_iv_root / "hosp"

    @property
    def icu_dir(self) -> Path:
        return self.mimic_iv_root / "icu"

    @property
    def note_dir(self) -> Path:
        return self.mimic_note_root / "note"

    def table_path(self, module: str, table_name: str) -> Path:
        """
        Get path to a MIMIC table file.

        Args:
            module: "hosp", "icu", or "note"
            table_name: Table name without extension (e.g., "admissions")

        Returns:
            Path to the .csv.gz file.
        """
        if module == "hosp":
            return self.hosp_dir / f"{table_name}.csv.gz"
        elif module == "icu":
            return self.icu_dir / f"{table_name}.csv.gz"
        elif module == "note":
            return self.note_dir / f"{table_name}.csv.gz"
        else:
            raise ValueError(f"Unknown module: {module}. Use 'hosp', 'icu', or 'note'.")


@dataclass
class LLMConfig:
    """LLM API configuration."""
    model: str = "google/gemini-2.5-flash-preview"
    api_key: str = ""
    base_url: str = "https://openrouter.ai/api/v1"
    temperature: float = 0.0
    max_tokens: int = 4096

    def __post_init__(self):
        # Read from environment if not explicitly set
        if not self.api_key:
            self.api_key = os.environ.get("AUTORAI_LLM_API_KEY", "")
        if not self.base_url:
            self.base_url = os.environ.get(
                "AUTORAI_LLM_BASE_URL", "https://openrouter.ai/api/v1"
            )


@dataclass
class IEConfig:
    """Top-level configuration for the IE pipeline."""
    mimic_paths: MIMICPaths = field(default_factory=MIMICPaths)
    llm: LLMConfig = field(default_factory=LLMConfig)
    output_dir: Path = _PROJECT_ROOT / "output" / "ie"
    log_level: str = "INFO"

    def __post_init__(self):
        # Create output directory if needed
        self.output_dir.mkdir(parents=True, exist_ok=True)


# Singleton default config
_default_config: IEConfig | None = None


def get_config() -> IEConfig:
    """Get or create the default configuration."""
    global _default_config
    if _default_config is None:
        _default_config = IEConfig()
    return _default_config


def set_config(config: IEConfig) -> None:
    """Override the default configuration."""
    global _default_config
    _default_config = config
