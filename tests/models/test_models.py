from __future__ import annotations

from datetime import UTC, datetime

import pytest
from bson import ObjectId

from llming_plumber.models import (
    Attachment,
    BlockDefinition,
    BlockPosition,
    BlockState,
    Parcel,
    PipeDefinition,
    PipelineAccess,
    PipelineDefinition,
    Role,
    Run,
    RunLog,
    RunStatus,
    Schedule,
    Team,
    TeamMember,
    doc_to_model,
    model_to_doc,
)

# ---------------------------------------------------------------------------
# Pipeline models
# ---------------------------------------------------------------------------


class TestBlockPosition:
    def test_defaults(self) -> None:
        pos = BlockPosition()
        assert pos.x == 0.0
        assert pos.y == 0.0

    def test_custom_values(self) -> None:
        pos = BlockPosition(x=100.5, y=200.3)
        assert pos.x == 100.5
        assert pos.y == 200.3


class TestBlockDefinition:
    def test_minimal(self) -> None:
        b = BlockDefinition(uid="b1", block_type="http_request", label="Fetch")
        assert b.uid == "b1"
        assert b.block_type == "http_request"
        assert b.label == "Fetch"
        assert b.config == {}
        assert b.notes == ""

    def test_full(self) -> None:
        b = BlockDefinition(
            uid="b2",
            block_type="rss_reader",
            label="RSS",
            config={"url": "https://example.com/feed"},
            position=BlockPosition(x=10, y=20),
            notes="Main feed",
        )
        assert b.config["url"] == "https://example.com/feed"
        assert b.position.x == 10
        assert b.notes == "Main feed"

    def test_serialization_roundtrip(self) -> None:
        b = BlockDefinition(uid="b1", block_type="http_request", label="Fetch")
        data = b.model_dump()
        b2 = BlockDefinition.model_validate(data)
        assert b == b2


class TestPipeDefinition:
    def test_minimal(self) -> None:
        p = PipeDefinition(
            uid="p1",
            source_block_uid="b1",
            source_fitting_uid="out",
            target_block_uid="b2",
            target_fitting_uid="in",
        )
        assert p.field_mapping is None
        assert p.attachment_filter is None
        assert p.transform is None

    def test_with_mapping(self) -> None:
        p = PipeDefinition(
            uid="p2",
            source_block_uid="b1",
            source_fitting_uid="out",
            target_block_uid="b2",
            target_fitting_uid="in",
            field_mapping={"temp": "temperature"},
            attachment_filter=["application/pdf"],
            transform="value * 2",
        )
        assert p.field_mapping == {"temp": "temperature"}
        assert p.attachment_filter == ["application/pdf"]
        assert p.transform == "value * 2"


class TestPipelineDefinition:
    def test_minimal(self) -> None:
        pipeline = PipelineDefinition(name="Test Pipeline")
        assert pipeline.name == "Test Pipeline"
        assert pipeline.id == ""
        assert pipeline.version == 1
        assert pipeline.blocks == []
        assert pipeline.pipes == []
        assert pipeline.owner_type == "user"
        assert pipeline.tags == []

    def test_full(self) -> None:
        block = BlockDefinition(uid="b1", block_type="http_request", label="Fetch")
        pipe = PipeDefinition(
            uid="p1",
            source_block_uid="b1",
            source_fitting_uid="out",
            target_block_uid="b2",
            target_fitting_uid="in",
        )
        pipeline = PipelineDefinition(
            id="abc123",
            name="Full Pipeline",
            description="A full pipeline",
            blocks=[block],
            pipes=[pipe],
            version=3,
            owner_id="user_1",
            owner_type="team",
            tags=["nightly"],
        )
        assert len(pipeline.blocks) == 1
        assert len(pipeline.pipes) == 1
        assert pipeline.owner_type == "team"

    def test_serialization_roundtrip(self) -> None:
        pipeline = PipelineDefinition(name="Roundtrip", tags=["a", "b"])
        data = pipeline.model_dump(mode="json")
        p2 = PipelineDefinition.model_validate(data)
        assert p2.name == "Roundtrip"
        assert p2.tags == ["a", "b"]

    def test_invalid_owner_type(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PipelineDefinition(name="Bad", owner_type="robot")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Parcel models
# ---------------------------------------------------------------------------


class TestAttachment:
    def test_creation(self) -> None:
        att = Attachment(
            uid="att1",
            filename="report.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            data_b64="SGVsbG8=",
        )
        assert att.uid == "att1"
        assert att.data_b64 == "SGVsbG8="
        assert att.storage_ref is None
        assert att.metadata == {}

    def test_with_storage_ref(self) -> None:
        att = Attachment(
            uid="att2",
            filename="big.tiff",
            mime_type="image/tiff",
            size_bytes=50_000_000,
            storage_ref="gridfs://plumber/att2",
        )
        assert att.data_b64 is None
        assert att.storage_ref == "gridfs://plumber/att2"


class TestParcel:
    def test_minimal(self) -> None:
        p = Parcel(uid="parcel1")
        assert p.fields == {}
        assert p.attachments == []

    def test_with_data(self) -> None:
        att = Attachment(
            uid="a1",
            filename="f.txt",
            mime_type="text/plain",
            size_bytes=5,
            data_b64="dGVzdA==",
        )
        p = Parcel(
            uid="parcel2",
            fields={"title": "Test", "count": 42},
            attachments=[att],
        )
        assert p.fields["title"] == "Test"
        assert len(p.attachments) == 1
        assert p.attachments[0].filename == "f.txt"

    def test_serialization_roundtrip(self) -> None:
        att = Attachment(
            uid="a1",
            filename="f.txt",
            mime_type="text/plain",
            size_bytes=5,
            data_b64="dGVzdA==",
        )
        p = Parcel(uid="p1", fields={"key": "value"}, attachments=[att])
        data = p.model_dump(mode="json")
        p2 = Parcel.model_validate(data)
        assert p2.uid == "p1"
        assert p2.attachments[0].uid == "a1"


# ---------------------------------------------------------------------------
# Run models
# ---------------------------------------------------------------------------


class TestRunStatus:
    def test_all_values(self) -> None:
        expected = {"queued", "running", "completed", "failed", "retrying", "cancelled"}
        assert {s.value for s in RunStatus} == expected

    def test_string_value(self) -> None:
        assert RunStatus.queued == "queued"
        assert RunStatus("completed") is RunStatus.completed


class TestBlockState:
    def test_defaults(self) -> None:
        bs = BlockState()
        assert bs.status == ""
        assert bs.output is None
        assert bs.error is None
        assert bs.duration_ms is None

    def test_full(self) -> None:
        bs = BlockState(
            status="completed",
            output={"data": [1, 2, 3]},
            error=None,
            duration_ms=123.4,
        )
        assert bs.duration_ms == 123.4


class TestRun:
    def test_defaults(self) -> None:
        run = Run()
        assert run.status == RunStatus.queued
        assert run.attempt == 0
        assert run.max_attempts == 3
        assert run.block_states == {}
        assert run.tags == []

    def test_full(self) -> None:
        now = datetime.now(UTC)
        run = Run(
            id="abc",
            pipeline_id="pipe1",
            pipeline_version=2,
            status=RunStatus.running,
            created_at=now,
            started_at=now,
            lemming_id="host:1234:abcd",
            arq_job_id="arq:job:xyz",
            current_block="block1",
            block_states={"block1": BlockState(status="running")},
            input={"key": "val"},
            attempt=1,
            tags=["nightly"],
        )
        assert run.status == RunStatus.running
        assert run.lemming_id == "host:1234:abcd"

    def test_serialization_roundtrip(self) -> None:
        run = Run(pipeline_id="p1", status=RunStatus.failed, error="timeout")
        data = run.model_dump(mode="json")
        r2 = Run.model_validate(data)
        assert r2.status == RunStatus.failed
        assert r2.error == "timeout"


# ---------------------------------------------------------------------------
# Log model
# ---------------------------------------------------------------------------


class TestRunLog:
    def test_defaults(self) -> None:
        log = RunLog()
        assert log.level == "info"
        assert log.msg == ""

    def test_full(self) -> None:
        log = RunLog(
            id="log1",
            run_id="run1",
            lemming_id="host:1:abc",
            block_id="fetch-rss",
            block_type="rss_reader",
            level="warning",
            msg="Slow response",
            duration_ms=500.0,
            output_summary={"parcel_count": 10},
        )
        assert log.block_type == "rss_reader"
        assert log.output_summary == {"parcel_count": 10}

    def test_serialization_roundtrip(self) -> None:
        log = RunLog(run_id="r1", msg="OK")
        data = log.model_dump(mode="json")
        l2 = RunLog.model_validate(data)
        assert l2.msg == "OK"


# ---------------------------------------------------------------------------
# Schedule model
# ---------------------------------------------------------------------------


class TestSchedule:
    def test_defaults(self) -> None:
        s = Schedule()
        assert s.enabled is True
        assert s.cron_expression is None
        assert s.interval_seconds is None
        assert s.tags == []

    def test_cron(self) -> None:
        s = Schedule(
            pipeline_id="p1",
            cron_expression="0 * * * *",
            tags=["hourly"],
        )
        assert s.cron_expression == "0 * * * *"
        assert s.tags == ["hourly"]

    def test_interval(self) -> None:
        s = Schedule(pipeline_id="p1", interval_seconds=300)
        assert s.interval_seconds == 300

    def test_serialization_roundtrip(self) -> None:
        now = datetime.now(UTC)
        s = Schedule(pipeline_id="p1", next_run_at=now)
        data = s.model_dump(mode="json")
        s2 = Schedule.model_validate(data)
        assert s2.pipeline_id == "p1"


# ---------------------------------------------------------------------------
# Ownership models
# ---------------------------------------------------------------------------


class TestRole:
    def test_values(self) -> None:
        assert {r.value for r in Role} == {"owner", "editor", "viewer"}


class TestTeamMember:
    def test_creation(self) -> None:
        tm = TeamMember(user_id="u1")
        assert tm.role == Role.viewer

    def test_custom_role(self) -> None:
        tm = TeamMember(user_id="u1", role=Role.editor)
        assert tm.role == Role.editor


class TestTeam:
    def test_minimal(self) -> None:
        t = Team(name="Alpha")
        assert t.members == []

    def test_with_members(self) -> None:
        t = Team(
            name="Beta",
            members=[
                TeamMember(user_id="u1", role=Role.owner),
                TeamMember(user_id="u2", role=Role.viewer),
            ],
        )
        assert len(t.members) == 2

    def test_serialization_roundtrip(self) -> None:
        t = Team(name="Gamma", members=[TeamMember(user_id="u1")])
        data = t.model_dump(mode="json")
        t2 = Team.model_validate(data)
        assert t2.members[0].user_id == "u1"


class TestPipelineAccess:
    def test_user_access(self) -> None:
        pa = PipelineAccess(user_id="u1", role=Role.editor)
        assert pa.user_id == "u1"
        assert pa.team_id is None

    def test_team_access(self) -> None:
        pa = PipelineAccess(team_id="t1", role=Role.viewer)
        assert pa.team_id == "t1"
        assert pa.user_id is None


# ---------------------------------------------------------------------------
# Mongo helpers
# ---------------------------------------------------------------------------


class TestDocToModel:
    def test_converts_objectid_to_str(self) -> None:
        oid = ObjectId()
        doc = {"_id": oid, "name": "Test Pipeline", "blocks": [], "pipes": []}
        pipeline = doc_to_model(doc, PipelineDefinition)
        assert pipeline.id == str(oid)
        assert pipeline.name == "Test Pipeline"

    def test_no_id_field(self) -> None:
        doc = {"name": "No ID", "blocks": [], "pipes": []}
        pipeline = doc_to_model(doc, PipelineDefinition)
        assert pipeline.id == ""

    def test_run_model(self) -> None:
        oid = ObjectId()
        doc = {
            "_id": oid,
            "pipeline_id": "p1",
            "status": "running",
            "attempt": 2,
        }
        run = doc_to_model(doc, Run)
        assert run.id == str(oid)
        assert run.status == RunStatus.running
        assert run.attempt == 2


class TestModelToDoc:
    def test_converts_id_to_objectid(self) -> None:
        oid = ObjectId()
        pipeline = PipelineDefinition(id=str(oid), name="Test")
        doc = model_to_doc(pipeline)
        assert "_id" in doc
        assert doc["_id"] == oid
        assert "id" not in doc

    def test_empty_id_omits_objectid(self) -> None:
        pipeline = PipelineDefinition(name="No ID")
        doc = model_to_doc(pipeline)
        assert "_id" not in doc
        assert "id" not in doc

    def test_roundtrip(self) -> None:
        oid = ObjectId()
        original = Run(id=str(oid), pipeline_id="p1", status=RunStatus.completed)
        doc = model_to_doc(original)
        restored = doc_to_model(doc, Run)
        assert restored.id == str(oid)
        assert restored.status == RunStatus.completed
        assert restored.pipeline_id == "p1"
