"""SQL fragments for workspace-scoped prediction review reads.

Predictions are shared classifier output; review metadata belongs to one
workspace. Use both fragments in review-aware queries, binding the workspace
id at the join's ``?`` before subsequent query parameters. Keep detector
confidence, label-set currency and visibility predicates at the call site:
their meaning varies across reads.

For example, bind ``(workspace_id, detection_id)`` to::

    f'''SELECT pr.*, {prediction_review_status()} AS status
        FROM predictions pr {prediction_review_join('pr')}
        WHERE pr.detection_id = ?'''

The fragments preserve ordinary joins (including correlated workspace
expressions) so SQLite can use the prediction_review composite primary key
without materializing a catalog-wide intermediate result.
"""

import re


def _identifier(value: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value) is None:
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def prediction_review_join(
    prediction_alias: str,
    review_alias: str = "pr_rev",
    *,
    workspace: str = "?",
    required: bool = False,
) -> str:
    """Join one workspace's review metadata to a prediction alias.

    ``required=True`` keeps only explicitly reviewed rows. ``workspace`` is
    either a bound parameter or a qualified column in a correlated query;
    never interpolate a workspace id or request text into the SQL.
    """
    prediction_alias = _identifier(prediction_alias)
    review_alias = _identifier(review_alias)
    if workspace != "?":
        parts = workspace.split(".")
        if len(parts) != 2:
            raise ValueError(f"Expected a workspace parameter or qualified column: {workspace!r}")
        workspace = ".".join(_identifier(part) for part in parts)
    kind = "JOIN" if required else "LEFT JOIN"
    return (
        f"{kind} prediction_review {review_alias} "
        f"ON {review_alias}.prediction_id = {prediction_alias}.id "
        f"AND {review_alias}.workspace_id = {workspace}"
    )


def prediction_review_status(review_alias: str = "pr_rev") -> str:
    """Effective status: a missing review row means pending in this workspace."""
    return f"COALESCE({_identifier(review_alias)}.status, 'pending')"
