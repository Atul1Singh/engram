"""PostgreSQL-canonical consolidation handlers used by Temporal V2 workers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any

from engram import prompts
from engram.config import ConsolidationConfig
from engram.models.core import CoreModelProvider
from engram.models.request_context import model_request_session
from engram.storage.memory_repository import MemoryRepository

_OVERVIEW_OUTPUT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "overview": {"type": "string"},
        "claim_ids": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["overview", "claim_ids"],
}


class OverviewInputBudgetError(ValueError):
    """Scope must be partitioned before a complete summary can be generated."""


def consolidate_overview(
    *,
    node_id: str,
    repository: MemoryRepository,
    core: CoreModelProvider,
    cfg: ConsolidationConfig,
    tenant_id: str,
    overview_cache: Any | None = None,
) -> None:
    """Generate a revision-bound overview solely from canonical snapshots."""

    node = repository.resolve_memory_ref(node_id, tenant_id=tenant_id, include_historical=False)
    if node is None:
        raise ValueError(f"canonical overview scope not found: {node_id}")
    existing = repository.get_overview(node.id, tenant_id=tenant_id, require_current=True)
    if existing is not None:
        return
    # Capture canonical descendants, not generated child summaries. This
    # includes source facts underneath nested collections without compounding
    # lossy summaries or silently omitting overflowing branches.
    children = []
    pending = list(repository.get_children(node.id, tenant_id=tenant_id))
    seen = {node.id}
    while pending:
        child = pending.pop(0)
        if child.id in seen:
            continue
        seen.add(child.id)
        children.append(child)
        if len(children) > cfg.overview_max_children:
            raise OverviewInputBudgetError(
                "overview scope exceeds configured source budget; partition the scope"
            )
        pending.extend(repository.get_children(child.id, tenant_id=tenant_id))
    dependencies: list[dict[str, Any]] = []
    child_abstracts: list[dict[str, str]] = []
    child_relations: list[dict[str, Any]] = []
    sources = [node, *children] if cfg.quality_mode == "enforce" else children
    for child in sources:
        snapshot = repository.get_current_state(child.id, tenant_id=tenant_id,
            include_historical_claims=cfg.quality_mode == "enforce")
        if snapshot is None:
            if cfg.quality_mode == "enforce":
                raise ValueError("overview source disappeared during input capture")
            continue
        version = snapshot.current_version
        dependencies.append(
            {
                "memory_id": str(child.id),
                "revision": snapshot.node.revision,
                "version_id": str(version.id) if version else None,
            }
        )
        child_abstracts.append(
            {
                "source_uri": child.canonical_uri,
                "abstract": (
                    version.abstract
                    if version and version.abstract
                    else (version.body[:500] if version else child.canonical_name or "")
                ),
            }
        )
        child_relations.extend(
            {
                "claim_id": str(claim.id),
                "subject_id": str(claim.subject_id),
                "subject_name": child.canonical_name,
                "predicate": claim.predicate,
                "object_entity_id": (
                    str(claim.object_entity_id) if claim.object_entity_id else None
                ),
                "object_value": claim.object_value,
                "status": claim.status,
                "valid_from": str(claim.valid_from) if claim.valid_from else None,
                "valid_until": str(claim.valid_until) if claim.valid_until else None,
                "asserted_at": str(claim.asserted_at),
                "source_event_id": claim.source_event_id,
                "source_version_id": str(claim.source_version_id) if claim.source_version_id else None,
                "source_metadata": claim.metadata,
                "evidence": [asdict(e) for e in snapshot.evidence if e.claim_id == claim.id],
            }
            for claim in snapshot.claims
        )
    prompt = prompts.render(
        "canonical_overview",
        directory_uri=node.canonical_uri,
        children_abstracts=child_abstracts,
        children_relations=child_relations,
        overview_max_tokens=cfg.overview_max_tokens,
        quality_mode=cfg.quality_mode,
    )
    from engram import tokens

    if tokens.count_tokens(prompt) > cfg.overview_input_tokens:
        raise OverviewInputBudgetError(
            "overview input exceeds configured token budget; partition the scope"
        )
    with model_request_session(str(node.id)):
        result = core.complete(
            system_prompt=prompt,
            user_prompt=("Select canonical claim IDs; leave overview empty. The server renders the claims."
                         if cfg.quality_mode == "enforce" else "Return the canonical overview text."),
            output_schema=_OVERVIEW_OUTPUT_SCHEMA,
            **({"max_tokens": cfg.overview_max_tokens} if cfg.quality_mode == "enforce" else {}),
        )
    if isinstance(result.output, dict):
        text = str(result.output.get("overview") or result.output.get("summary") or "")
    elif isinstance(result.output, str):
        text = result.output
    else:
        text = result.raw_text
    if not text.strip() and cfg.quality_mode != "enforce":
        raise ValueError("overview model returned empty content")
    supported = {claim["claim_id"] for claim in child_relations}
    cited = result.output.get("claim_ids", []) if isinstance(result.output, dict) else []
    if not isinstance(cited, list) or not all(isinstance(value, str) for value in cited):
        raise ValueError("overview claim citations must be a list of IDs")
    if not set(cited) <= supported or (supported and not cited):
        raise ValueError("overview must cite only supplied canonical claims")
    cited = list(dict.fromkeys(cited))
    omitted = sorted(supported - set(cited))
    if cfg.quality_mode == "enforce":
        # The model selects salient claims; the server renders their exact
        # values and qualifiers. Generated prose cannot add uncited facts or
        # turn an historical claim into a current assertion.
        selected, rendered = [], []
        by_id = {claim["claim_id"]: claim for claim in child_relations}
        for identity in cited:
            claim = by_id[identity]
            value = claim["object_entity_id"] or json.dumps(claim["object_value"], ensure_ascii=False)
            line = (f"- [{identity}] {claim['subject_name'] or claim['subject_id']} "
                    f"{claim['predicate']}: {value}; status={claim['status']}; "
                    f"valid={claim['valid_from']}..{claim['valid_until']}; "
                    f"asserted_at={claim['asserted_at']}; source_event={claim['source_event_id']}; "
                    f"source_metadata={json.dumps(claim['source_metadata'], ensure_ascii=False, default=str)}")
            if tokens.count_tokens("\n".join([*rendered, line])) <= cfg.overview_max_tokens - 80:
                rendered.append(line)
                selected.append(identity)
        cited = selected
        omitted = sorted(supported - set(cited))
        text = "Derived overview; selected canonical claims, not an exhaustive record.\n" + (
            "\n".join(rendered) or "No structured claims fit this overview; consult the original records.")
    # Re-read after the model call. A concurrent child mutation changes the
    # scope revision and makes this generated result unsafe to publish.
    current = repository.get_memory(node.id, tenant_id=tenant_id)
    if current is None or current.revision != node.revision:
        raise RuntimeError("canonical overview scope changed during generation")
    repository.save_overview(
        scope_id=node.id,
        content=text.strip(),
        input_revision=current.revision,
        expected_revision=node.revision,
        dependencies=dependencies,
        model_metadata={
            "schema": "canonical-overview-v3",
            "input_fingerprint": hashlib.sha256(
                json.dumps(
                    {"revision": node.revision, "dependencies": dependencies,
                     "claims": child_relations, "abstracts": child_abstracts}, sort_keys=True, default=str
                ).encode()
            ).hexdigest(),
            "claim_ids": cited,
            "quality_policy": "extractive-claims-v1" if cfg.quality_mode == "enforce" else "legacy",
            "input_claim_ids": sorted(supported),
            "omitted_claim_ids": omitted,
            "coverage": "PARTIAL" if omitted else "ALL_INPUT_CLAIMS",
            "source_event_ids": sorted({claim["source_event_id"] for claim in child_relations
                                        if claim["claim_id"] in cited and claim["source_event_id"]}),
        },
        tenant_id=tenant_id,
    )
    if overview_cache is not None:
        overview_cache.invalidate(tenant_id, node.canonical_uri)


def unmerge_entity(
    *,
    node_id: str,
    task_id: str,
    repository: MemoryRepository,
    core: CoreModelProvider,
    tenant_id: str,
) -> None:
    """Plan an entity split with Muse Spark, then commit it canonically."""

    node = repository.resolve_memory_ref(node_id, tenant_id=tenant_id, include_historical=False)
    if node is None:
        raise ValueError(f"canonical entity not found: {node_id}")
    if node.memory_type != "ENTITY":
        raise ValueError("only ENTITY memories can be unmerged")
    snapshot = repository.get_current_state(
        node.id,
        tenant_id=tenant_id,
        include_historical_claims=True,
        require_current_overview=False,
    )
    if snapshot is None:
        raise ValueError(f"canonical entity not found: {node_id}")
    source_extractions = [asdict(claim) for claim in snapshot.claims]
    prompt = prompts.render(
        "unmerge",
        merged_node={
            "memory_id": str(node.id),
            "canonical_uri": node.canonical_uri,
            "canonical_name": node.canonical_name,
            "l0_abstract": (
                snapshot.current_version.abstract
                if snapshot.current_version
                else node.canonical_name or ""
            ),
        },
        source_extractions=source_extractions,
    )
    with model_request_session(str(node.id)):
        result = core.complete(
            system_prompt=prompt,
            user_prompt="Return the split JSON.",
        )
    output = result.output if isinstance(result.output, dict) else {}
    raw_splits = output.get("splits") or []
    splits = [dict(item) for item in raw_splits if isinstance(item, dict)]
    if not splits:
        raise ValueError("unmerge produced no validated entity splits")
    repository.unmerge_entity(
        node.id,
        splits,
        tenant_id=tenant_id,
        mutation_key=f"consolidation:{task_id}:unmerge",
    )


def enqueue_stale_overviews(
    *,
    repository: MemoryRepository,
    control_plane: Any,
    tenant_id: str,
    limit: int = 200,
) -> int:
    """Enqueue scopes whose latest overview does not match current revision."""

    with repository.transaction(tenant_id=tenant_id) as conn:
        rows = conn.execute(
            "SELECT n.id FROM memory_nodes n WHERE n.tenant_id = ? "
            "AND n.status = 'ACTIVE' AND n.memory_type IN ('COLLECTION', 'PROJECT') "
            "AND NOT EXISTS (SELECT 1 FROM memory_overviews o "
            "WHERE o.tenant_id = n.tenant_id AND o.scope_id = n.id "
            "AND o.input_revision = n.revision AND o.model_metadata->>'schema' = 'canonical-overview-v3') "
            "ORDER BY n.updated_at LIMIT ?",
            (tenant_id, limit),
        ).fetchall()
    return sum(
        1
        for row in rows
        if control_plane.enqueue_task(
            node_id=str(row["id"]),
            task_type="CONSOLIDATE_OVERVIEW",
            priority=6,
            tenant_id=tenant_id,
        )
    )


__all__ = ["consolidate_overview", "enqueue_stale_overviews", "unmerge_entity"]
