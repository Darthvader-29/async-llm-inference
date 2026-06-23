"""Adapter implementations of the ports (driven/secondary adapters).

Each subpackage is a concrete implementation of a ``typing.Protocol`` port from
``app.ports`` — persistence (SQLAlchemy, Phase 3), object store + providers
(Phase 4), broker (Phase 5). Adapters are the ONLY layer that imports a
third-party SDK; the domain/services never do.
"""
