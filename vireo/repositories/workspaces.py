"""Persistence for workspace navigation state."""

import json


class WorkspaceRepository:
    def __init__(self, conn, workspace_id, *, allowed_nav_ids, default_tabs):
        self.conn = conn
        self.workspace_id = workspace_id
        self.allowed_nav_ids = allowed_nav_ids
        self.default_tabs = list(default_tabs)

    def get_tabs(self):
        row = self.conn.execute(
            "SELECT tabs FROM workspaces WHERE id=?", (self.workspace_id,),
        ).fetchone()
        if not row or not row["tabs"]:
            return list(self.default_tabs)
        try:
            value = json.loads(row["tabs"]) if isinstance(row["tabs"], str) else row["tabs"]
        except (json.JSONDecodeError, TypeError):
            return list(self.default_tabs)
        if not isinstance(value, list):
            return list(self.default_tabs)
        return [
            tab for tab in value
            if isinstance(tab, str) and tab in self.allowed_nav_ids
        ]

    def set_tabs(self, tabs):
        if not isinstance(tabs, list):
            raise ValueError("tabs must be a list")
        seen = set()
        for nav_id in tabs:
            if not isinstance(nav_id, str):
                raise ValueError(
                    f"tab id must be a string, got {type(nav_id).__name__}"
                )
            if nav_id not in self.allowed_nav_ids:
                raise ValueError(f"{nav_id!r} is not a known nav id")
            if nav_id in seen:
                raise ValueError(f"{nav_id!r} appears more than once")
            seen.add(nav_id)
        self._write(tabs)
        return list(tabs)

    def pin_tab(self, nav_id):
        self._validate_nav_id(nav_id)
        tabs = self.get_tabs()
        if nav_id not in tabs:
            tabs.append(nav_id)
            self._write(tabs)
        return tabs

    def unpin_tab(self, nav_id):
        self._validate_nav_id(nav_id)
        tabs = self.get_tabs()
        if nav_id in tabs:
            tabs = [tab for tab in tabs if tab != nav_id]
            self._write(tabs)
        return tabs

    def _validate_nav_id(self, nav_id):
        if nav_id not in self.allowed_nav_ids:
            raise ValueError(f"{nav_id!r} is not a known nav id")

    def _write(self, tabs):
        self.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(tabs), self.workspace_id),
        )
        self.conn.commit()
