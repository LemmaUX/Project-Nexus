from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from nexus.dlq_monitor import DLQMonitor, DLQMonitorConfig, main  # noqa: E402


__all__ = ["DLQMonitor", "DLQMonitorConfig", "main"]


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())