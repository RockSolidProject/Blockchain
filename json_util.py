import json

def to_deterministic_json(data) -> str:
    return json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":")
    )
def from_json(json_string: str):
    return json.loads(json_string)