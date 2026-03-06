from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class Role(StrEnum):
    owner = "owner"
    editor = "editor"
    viewer = "viewer"


class TeamMember(BaseModel):
    """A member within a team, with an assigned role."""

    user_id: str
    role: Role = Role.viewer


class Team(BaseModel):
    """A named group of users."""

    id: str = ""
    name: str
    members: list[TeamMember] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PipelineAccess(BaseModel):
    """Grants a user or team access to a pipeline with a given role."""

    user_id: str | None = None
    team_id: str | None = None
    role: Role = Role.viewer
