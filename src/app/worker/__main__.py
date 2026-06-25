"""``python -m app.worker`` entrypoint.

Deliberately minimal: drive the runner's async ``main()`` coroutine under
``asyncio.run``. Everything testable lives in ``runner.py``. ``main`` here is a
*sync* function so it doubles as the ``aie-worker`` console-script target
(``[project.scripts]`` in pyproject), while ``python -m app.worker`` resolves to
this module's ``__main__`` guard.
"""

from __future__ import annotations

import asyncio

from app.worker.runner import main as _async_main


def main() -> None:
    """Synchronous launcher: run the async entrypoint to completion.

    ``asyncio.run`` creates a fresh event loop, runs ``runner.main()`` (which
    configures logging, builds the shared container, runs the consume loop, and
    closes the container), and tears the loop down on exit.
    """
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
