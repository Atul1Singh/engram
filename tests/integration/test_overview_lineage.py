"""Real PostgreSQL checks for source invalidation and atomic derived publication."""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from engram.config import ConsolidationConfig
from engram.consolidation.canonical import consolidate_overview
from engram.storage.memory_repository import MemoryRepository, StaleOverviewError
from tests.postgres_support import PostgresTestStore

TENANT = "overview-test"


def test_strict_overview_renders_claims_and_records_omissions(tree):
    repo, _, _, folder, person = tree
    old = repo.add_claim(subject_id=person.id, predicate="HAS_ROLE", object_value="Engineer",
        object_type="ROLE", valid_from=datetime(2023, 1, 1, tzinfo=timezone.utc),
        metadata={"source_metadata": {"occurrence_year": 2022, "temporal_precision": "year"}}, tenant_id=TENANT)
    other = repo.add_claim(subject_id=person.id, predicate="LOCATED_IN", object_value="Pune",
        object_type="LOCATION", tenant_id=TENANT)
    class Model:
        def complete(self, **kwargs):
            assert "2022" in kwargs["system_prompt"]
            assert "2023-01-01" in kwargs["system_prompt"]
            return SimpleNamespace(output={"overview": "Alice is secretly a CEO", "claim_ids": [str(old.id)]}, raw_text="")
    consolidate_overview(node_id=str(folder.id), repository=repo, core=Model(),
                        cfg=ConsolidationConfig(quality_mode="enforce"), tenant_id=TENANT)
    overview = repo.get_overview(folder.id, tenant_id=TENANT, require_current=True)
    assert "Engineer" in overview.content and "CEO" not in overview.content
    assert "2022" in overview.content and "not an exhaustive" in overview.content
    assert overview.model_metadata["omitted_claim_ids"] == [str(other.id)]
    assert overview.model_metadata["coverage"] == "PARTIAL"


def test_unscheduled_projection_repair_is_bounded_and_idempotent(tree):
    repo, store, project, folder, person = tree
    repo.append_version(person.id, body="A changed source", tenant_id=TENANT)
    assert repo.repair_projection_dispatches(tenant_id=TENANT, limit=1) == 1
    while repo.repair_projection_dispatches(tenant_id=TENANT, limit=200):
        pass
    assert repo.repair_projection_dispatches(tenant_id=TENANT) == 0
    for memory in (project, folder, person):
        current = repo.get_memory(memory.id, tenant_id=TENANT)
        row = store.get_conn().execute(
            "SELECT max(aggregate_revision) AS revision FROM workflow_dispatches "
            "WHERE tenant_id=? AND payload->>'memory_id'=?", (TENANT, str(memory.id))).fetchone()
        assert row["revision"] == current.revision


def test_strict_overview_includes_claims_on_the_scope_itself(tree):
    repo, _, _, folder, _ = tree
    claim = repo.add_claim(subject_id=folder.id, predicate="HAS_ATTRIBUTE", object_value="Project members",
        object_type="TEXT", tenant_id=TENANT)
    class Model:
        def complete(self, **kwargs):
            assert str(claim.id) in kwargs["system_prompt"]
            return SimpleNamespace(output={"overview": "", "claim_ids": [str(claim.id)]}, raw_text="")
    consolidate_overview(node_id=str(folder.id), repository=repo, core=Model(),
        cfg=ConsolidationConfig(quality_mode="enforce"), tenant_id=TENANT)
    assert "Project members" in repo.get_overview(folder.id, tenant_id=TENANT, require_current=True).content


def test_overview_cannot_publish_with_foreign_or_stale_dependency(tree):
    repo, _, _, folder, person = tree
    previous = repo.get_memory(person.id, tenant_id=TENANT)
    repo.append_version(person.id, body="new", tenant_id=TENANT)
    current = repo.get_memory(folder.id, tenant_id=TENANT)
    with pytest.raises(StaleOverviewError):
        repo.save_overview(scope_id=folder.id, content="stale", input_revision=current.revision,
            expected_revision=current.revision,
            dependencies=[{"memory_id": str(person.id), "revision": previous.revision}], tenant_id=TENANT)


@pytest.fixture
def tree():
    store = PostgresTestStore()
    repo = MemoryRepository(store)
    repo.ensure_schema()
    project = repo.create_memory(memory_type="PROJECT", canonical_name="Orion", tenant_id=TENANT)
    folder = repo.create_memory(memory_type="COLLECTION", canonical_name="People", tenant_id=TENANT)
    person = repo.create_memory(memory_type="ENTITY", canonical_name="Alice", tenant_id=TENANT)
    repo.add_hierarchy(parent_id=project.id, child_id=folder.id, tenant_id=TENANT)
    repo.add_hierarchy(parent_id=folder.id, child_id=person.id, tenant_id=TENANT)
    yield repo, store, project, folder, person
    store.close()


def publish(repo, scope, dependencies=()):
    current = repo.get_memory(scope.id, tenant_id=TENANT)
    return repo.save_overview(
        scope_id=scope.id,
        content="Supported summary",
        input_revision=current.revision,
        expected_revision=current.revision,
        model_metadata={"schema": "canonical-overview-v3"},
        dependencies=dependencies,
        tenant_id=TENANT,
    )


def test_child_change_invalidates_all_ancestors_and_coalesces_outbox(tree):
    repo, store, project, folder, person = tree
    child = repo.get_memory(person.id, tenant_id=TENANT)
    overview = publish(repo, folder, [{"memory_id": str(person.id), "revision": child.revision}])
    publish(repo, project)
    assert repo.get_overview(folder.id, tenant_id=TENANT, require_current=True).id == overview.id
    repo.append_version(person.id, body="Alice moved to Bengaluru", tenant_id=TENANT)
    assert repo.get_overview(folder.id, tenant_id=TENANT, require_current=True) is None
    assert repo.get_overview(project.id, tenant_id=TENANT, require_current=True) is None
    assert repo.get_current_state(folder.id, tenant_id=TENANT).overview is None
    assert repo.get_overview(folder.id, tenant_id=TENANT).id == overview.id
    rows = (
        store.get_conn()
        .execute(
            "SELECT t.node_id, t.requested_generation, d.dispatch_id FROM consolidation_tasks t "
            "JOIN workflow_dispatches d ON d.aggregate_id = t.task_id "
            "WHERE t.tenant_id = ? AND t.status = 'PENDING'",
            (TENANT,),
        )
        .fetchall()
    )
    assert {r["node_id"] for r in rows} == {str(project.id), str(folder.id)}
    assert len(rows) == 2
    assert all(r["requested_generation"] > 1 for r in rows)


def test_publication_compare_and_swap_and_immutable_retries(tree):
    repo, _, _, folder, person = tree
    revision = repo.get_memory(folder.id, tenant_id=TENANT).revision
    repo.append_version(person.id, body="New source", tenant_id=TENANT)
    with pytest.raises(StaleOverviewError):
        repo.save_overview(
            scope_id=folder.id,
            content="Stale",
            input_revision=revision,
            expected_revision=revision,
            tenant_id=TENANT,
        )
    first = publish(repo, folder)
    retry = repo.save_overview(
        scope_id=folder.id,
        content="Different retry output",
        input_revision=first.input_revision,
        tenant_id=TENANT,
    )
    assert retry.id == first.id
    assert retry.content == first.content


def test_mutation_during_model_generation_cannot_publish(tree):
    repo, _, _, folder, person = tree

    class ChangingModel:
        def complete(self, **kwargs):
            repo.append_version(person.id, body="Changed while the model ran", tenant_id=TENANT)
            return SimpleNamespace(output={"overview": "Old answer"}, raw_text="")

    with pytest.raises(RuntimeError, match="changed"):
        consolidate_overview(
            node_id=str(folder.id),
            repository=repo,
            core=ChangingModel(),
            cfg=ConsolidationConfig(),
            tenant_id=TENANT,
        )
    assert repo.get_overview(folder.id, tenant_id=TENANT) is None


def test_new_generation_during_processing_gets_exactly_one_followup(tree):
    repo, store, _, folder, person = tree
    task = (
        store.get_conn()
        .execute(
            "UPDATE consolidation_tasks SET status = 'PROCESSING', processing_generation = requested_generation "
            "WHERE tenant_id = ? AND node_id = ? RETURNING *",
            (TENANT, str(folder.id)),
        )
        .fetchone()
    )
    repo.append_version(person.id, body="Changed after input capture", tenant_id=TENANT)
    followup = store.finish_consolidation_task(task["task_id"], task["processing_generation"])
    assert followup and followup != task["task_id"]
    assert store.get_dispatch_for_aggregate("CONSOLIDATION", followup)["status"] == "PENDING"
    assert store.finish_consolidation_task(task["task_id"], task["processing_generation"]) is None
    rows = (
        store.get_conn()
        .execute(
            "SELECT task_id FROM consolidation_tasks WHERE tenant_id = ? AND node_id = ? "
            "AND status IN ('PENDING', 'PROCESSING')",
            (TENANT, str(folder.id)),
        )
        .fetchall()
    )
    assert [r["task_id"] for r in rows] == [followup]


def test_direct_metadata_mutation_and_deletion_fence_publication(tree):
    repo, _store, _, folder, _ = tree
    overview = publish(repo, folder)
    with repo.transaction(tenant_id=TENANT) as conn:
        conn.execute(
            "UPDATE memory_nodes SET metadata = '{\"updated\":true}' WHERE tenant_id = ? AND id = ?",
            (TENANT, folder.id),
        )
    assert repo.get_overview(folder.id, tenant_id=TENANT, require_current=True) is None
    with repo.transaction(tenant_id=TENANT) as conn:
        conn.execute(
            "UPDATE memory_nodes SET status = 'DELETED' WHERE tenant_id = ? AND id = ?",
            (TENANT, folder.id),
        )
    with pytest.raises(StaleOverviewError):
        repo.save_overview(
            scope_id=folder.id,
            content="Stale",
            input_revision=overview.input_revision,
            expected_revision=overview.input_revision,
            tenant_id=TENANT,
        )
