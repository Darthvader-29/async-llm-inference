<!--
============================================================================
AUTHORING TEMPLATE — DO NOT COPY THIS COMMENT BLOCK INTO THE FINAL DOCUMENT.

This file defines the canonical section skeleton every phase document MUST
follow so all 9 docs are structurally consistent and cross-linkable.

Rules for the authoring agent:
- Replace every <PLACEHOLDER> and <!-- guidance --> with real content.
- Keep the section order and heading levels exactly as below.
- Target 1000+ lines of genuinely useful, grounded content (no filler).
- Use GitHub-Flavored Markdown alerts for callouts:
    > [!NOTE]      neutral context
    > [!TIP]       a better way / shortcut
    > [!IMPORTANT] must-know to succeed
    > [!WARNING]   easy to get wrong / breaks things
    > [!CAUTION]   data-loss / security / irreversible
- Every diagram is a ```mermaid fenced block (flowchart / sequenceDiagram /
  classDiagram / stateDiagram-v2 / erDiagram as appropriate).
- Ground ALL code in real, current library APIs. If unsure of a signature,
  verify against official docs (WebFetch) — never invent APIs.
- Prefer complete, runnable code over fragments. Annotate non-obvious lines.
- Use tables for decisions, file manifests, env vars, and exit-criteria maps.
- Add a real References section with hyperlinks to authoritative docs.
- Cross-link the previous/next phase docs by relative path.
============================================================================
-->

# Phase <N> — <Phase Title>

> **Part of:** [Asynchronous AI Serving Engine](../implementation-plan.md) · [Problem Statement](../problem-statement.md)
> **Status:** Planned (greenfield) · **Depends on:** <prior phase(s) or "none"> · **Unlocks:** <later phase(s)>
> **Delivers:** <one-sentence summary of what exists after this phase that did not before>
> **Primary skills applied:** <comma-separated skills used to author this doc>

---

## Table of Contents

1. [Overview & Objectives](#1-overview--objectives)
2. [Where This Fits](#2-where-this-fits)
3. [Prerequisites & Inputs](#3-prerequisites--inputs)
4. [Deliverables](#4-deliverables)
5. [Design Decisions & Rationale](#5-design-decisions--rationale)
6. [Detailed Implementation](#6-detailed-implementation)
7. [Flow & Sequence Diagrams](#7-flow--sequence-diagrams)
8. [Configuration & Environment](#8-configuration--environment)
9. [Testing Strategy](#9-testing-strategy)
10. [Verification & Exit-Criteria Mapping](#10-verification--exit-criteria-mapping)
11. [Windows & Cross-Platform Notes](#11-windows--cross-platform-notes)
12. [Common Pitfalls & Troubleshooting](#12-common-pitfalls--troubleshooting)
13. [Definition of Done](#13-definition-of-done)
14. [References & Further Reading](#14-references--further-reading)
15. [Navigation](#15-navigation)

---

## 1. Overview & Objectives

<!-- What this phase is, why it exists, and the 3-6 concrete objectives it must hit.
     State what the system can do AFTER this phase that it could not before. -->

## 2. Where This Fits

<!-- A mermaid diagram of the full 5-layer system with THIS phase's components highlighted.
     Then 1-2 paragraphs on how this phase connects to the ones before/after it. -->

```mermaid
flowchart TD
    <!-- highlight this phase's surface in the overall architecture -->
```

## 3. Prerequisites & Inputs

<!-- What must already exist (files, abstractions, infra) for this phase to start.
     Reference the specific prior-phase docs that produced them. -->

## 4. Deliverables

<!-- A manifest table of every file created/changed in this phase. -->

| File | Type | Purpose |
|------|------|---------|
| `<path>` | new/changed | <purpose> |

## 5. Design Decisions & Rationale

<!-- The locked decisions relevant to this phase, WHY each was chosen, and the
     alternatives rejected. Use a decision table + prose + callouts. -->

| Decision | Choice | Why | Rejected alternative |
|----------|--------|-----|----------------------|

## 6. Detailed Implementation

<!-- The core of the document. For EACH file in the deliverables manifest:
       ### 6.x `path/to/file.py`
       - Purpose & responsibilities
       - Full code block (grounded, runnable)
       - Walkthrough of the non-obvious parts
       - Design rationale & how it honors the locked architecture
       - Inline callouts for gotchas
     Keep the Ports/Adapters discipline visible throughout. -->

## 7. Flow & Sequence Diagrams

<!-- mermaid sequenceDiagram / stateDiagram for the runtime behavior introduced
     by this phase (e.g., request lifecycle, retry/offload path, consume loop). -->

## 8. Configuration & Environment

<!-- New/changed settings, env vars (AIE_ prefix), defaults, and how they flow
     through Settings into the components. Table of env vars. -->

| Env var | Default | Used by | Notes |
|---------|---------|---------|-------|

## 9. Testing Strategy

<!-- Unit + integration tests this phase adds. Emphasize DETERMINISTIC, clock-free
     testing (RecordingOffloader spy, attempt-counting, Event-gated drains, DI overrides).
     Include real test code. State which tests need infra (integration marker) vs none. -->

## 10. Verification & Exit-Criteria Mapping

<!-- Map this phase's work to the spec's exit criteria. The exact verify command(s). -->

| Spec exit criterion | How this phase proves it | Command / test file |
|---------------------|--------------------------|---------------------|

## 11. Windows & Cross-Platform Notes

<!-- Concrete Windows gotchas relevant to this phase (Proactor loop, signals, CRLF,
     path-with-space, named volumes, uvloop). Use callouts. -->

## 12. Common Pitfalls & Troubleshooting

<!-- A table or list of likely failure modes, the symptom, and the fix. -->

| Symptom | Likely cause | Fix |
|---------|--------------|-----|

## 13. Definition of Done

<!-- A checklist the implementer ticks before the phase is considered complete. -->

- [ ] <criterion>

## 14. References & Further Reading

<!-- Hyperlinks to authoritative external docs/articles used or worth reading. -->

## 15. Navigation

<!-- Prev / Next phase links + back to the index. -->

| ← Previous | Index | Next → |
|-----------|-------|--------|
| <prev or —> | [All phases](README.md) | <next or —> |
