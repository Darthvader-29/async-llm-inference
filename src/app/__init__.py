"""Asynchronous AI Serving Engine — application package root.

Hexagonal architecture: this package's ``domain`` and ``core`` layers must not
import any web framework, ORM, broker, or cloud SDK. Adapters live under
``app.adapters`` (added in later phases) and depend inward on ``app.ports``.
"""

__all__: list[str] = []
