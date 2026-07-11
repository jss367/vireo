import hashlib
from pathlib import Path


def _public_route_contract(app):
    rows = []
    for rule in app.url_map.iter_rules():
        if rule.rule.startswith("/static/"):
            continue
        methods = sorted(set(rule.methods) - {"HEAD", "OPTIONS"})
        rows.append(f"{','.join(methods):12} {rule.rule}")
    return "\n".join(sorted(rows)) + "\n"


def test_route_contract_matches_snapshot(app_and_db):
    app, _ = app_and_db
    contract = _public_route_contract(app)
    snapshot_path = Path(__file__).with_name("contracts") / "routes.sha256"
    expected = snapshot_path.read_text(encoding="utf-8").strip()
    actual = f"{len(contract.splitlines())} {hashlib.sha256(contract.encode()).hexdigest()}"

    assert actual == expected, "Route contract changed:\n" + contract
