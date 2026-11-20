from typing import ClassVar

from agentlogs.schema import (
    AgentSession,
    AgentSessionEvent,
    AgentSessionUsage,
    BranchReference,
)
from .exporter import Exporter
from .common import (
    find_duplicate_task_session,
    format_pull_request_reference,
    format_task_reference,
    format_user_reference,
    format_workflow_run_reference,
    warn_duplicate_task,
)
from ..resources import TaskRef
from ..state import ResourceRecord
from ..datetime import parse_utc_datetime
    
class SessionExporter(Exporter[AgentSession]):
    resource_type: ClassVar[str] = "session"
    expand_relations: ClassVar[list[str]] = [
        "session_user",
        "session_session_log",
        "session_pull_request",
    ]
    load_relations_for: ClassVar[list[str]] = ["pull_request"]

    def get_record_type(self) -> type[AgentSession]:
        return AgentSession

    def format_resource(
        self, 
        resource: ResourceRecord,
        expanded_resources_from: dict[str, list[ResourceRecord]],
        expanded_resources_to: dict[str, list[ResourceRecord]]
    ) -> AgentSession | None:
        if resource.metadata.get("index") is None:
            raise ValueError("Session is missing index")

        if resource.metadata.get("unattached") == "true":
            return None

        if "id" not in resource.aliases:
            raise ValueError("Session is missing id alias")

        assert isinstance(resource.payload, dict)
        payload = resource.payload

        logs = expanded_resources_from.get("session_session_log", [])
        if len(logs) == 0:
            raise ValueError("Session has no log")
        if len(logs) > 1:
            raise ValueError("Session has more than one log")
        log_found = logs[0].found is True
        
        task_refs = [
            ref
            for ref in resource.relations_to.get("task_sessions", [])
            if isinstance(ref, TaskRef)
        ]
        if len(task_refs) == 0:
            raise ValueError("Session must have task relation")
        found = find_duplicate_task_session(self.state, resource) if len(task_refs) != 1 else None
        if len(task_refs) != 1 and found is None:
            shown = ", ".join(ref.id for ref in task_refs if ref.id is not None)
            raise ValueError(f"Session must have exactly one task relation ({shown})")
        if found is None:
            task_ref = task_refs[0]
        else:
            task, duplicate_task = found
            warn_duplicate_task(str(payload.get("id")), task.id, duplicate_task.id)
            task_ref = task
        if not isinstance(task_ref, TaskRef):
            raise ValueError("Session task relation is not a task")
        task = format_task_reference(task_ref)

        if (
            "session_user" not in resource.relations_from or
            len(resource.relations_from["session_user"]) == 0
        ):
            raise ValueError("Session must have user")
        
        if len(resource.relations_from["session_user"]) != 1:
            raise ValueError("Session must have exactly one user")

        users = expanded_resources_from.get("session_user", [])
        if len(users) != 1:
            raise ValueError("Session must have exactly one user")
        user = format_user_reference(users[0])

        pull_requests = [
            related
            for related in expanded_resources_from.get("session_pull_request", [])
            if related.metadata.get("unattached") != "true"
        ]
        if len(pull_requests) > 1:
            raise ValueError("Session has more than one pull request")
        runs = resource.relations_from.get("session_workflow_run", [])
        if len(runs) > 1:
            raise ValueError("Session has more than one workflow run")
        base_ref = (
            payload["base_ref"]
            if "base_ref" in payload and payload["base_ref"] != "" else
            None
        )
        head_ref = (
            payload["head_ref"]
            if "head_ref" in payload and payload["head_ref"] != "" else
            None
        )

        return AgentSession(
            id=resource.aliases["id"],
            session_index=int(resource.metadata["index"]),

            log_found=log_found,

            task=task,
            user=user,

            name=payload["name"],
            model=payload["model"] if payload["model"] != "" else None,
            prompt=payload["prompt"] if payload["prompt"] != "" else None,
            reasoning_effort=(
                payload["reasoning_effort"]
                if "reasoning_effort" in payload and payload["reasoning_effort"] != "" else
                None
            ),
            state=payload["state"],
            usage=AgentSessionUsage(
                type=payload["usage"]["type"],
                amount=(
                    float(payload["usage"]["amount"])
                    if "amount" in payload["usage"] and payload["usage"]["amount"] is not None else 
                    None
                ),
                credits=(
                    float(payload["usage"]["credits"])
                    if "credits" in payload["usage"] and payload["usage"]["credits"] is not None else 
                    None
                ),
            ) if "usage" in payload else None,
            premium_requests=float(payload["premium_requests"]),
            error=payload["error"]["message"] if "error" in payload else None,
            remote_steerable=payload["remote_steerable"] if "remote_steerable" in payload else None,
            workflow_run=(
                format_workflow_run_reference(runs[0])
                if runs else
                None
            ),

            event=AgentSessionEvent(
                type=(
                    payload["event_type"]
                    if "event_type" in payload and payload["event_type"] != "" else
                    None
                ),
                url=(
                    payload["event_url"]
                    if "event_url" in payload and payload["event_url"] != "" else
                    None
                ),
                ids=[
                    identifier
                    for identifier in payload["event_identifiers"]
                    if identifier != ""
                 ] if "event_identifiers" in payload else None,
                content=(
                    payload["event_content"] 
                    if "event_content" in payload and payload["event_content"] != "" else
                    None
                ),
            ),
            pull_request_artifact=(
                format_pull_request_reference(pull_requests[0])
                if pull_requests else
                None
            ),
            base=BranchReference(ref=base_ref) if base_ref is not None else None,
            head=BranchReference(ref=head_ref) if head_ref is not None else None,

            created_at=parse_utc_datetime(payload["created_at"]),
            updated_at=parse_utc_datetime(payload["updated_at"]),
            completed_at=parse_utc_datetime(payload["completed_at"]) if "completed_at" in payload else None,
        )
