"""Small, explicit search surface; all remaining feed fields stay in _source."""


def text_keyword():
    return {
        "type": "text", "norms": False, "index_options": "docs",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 8191}},
    }


def stored_long():
    return {"type": "long", "index": False, "doc_values": False}


INDEX_DEFINITION = {
    "settings": {
        "number_of_shards": 4,
        "number_of_replicas": 0,
        "refresh_interval": "-1",
        # Keep request durability: a successful bulk has reached the translog.
        "translog.durability": "request",
    },
    "mappings": {
        "dynamic": False,
        "properties": {
            "forager_id": {"type": "long", "doc_values": False},
            "linkedin_id": stored_long(),
            "first_name": text_keyword(),
            "last_name": text_keyword(),
            "headline": {"type": "text", "norms": False, "index_options": "docs"},
            "country": {"type": "keyword"},
            "city": {"type": "keyword", "ignore_above": 8191},
            "industry": {"type": "keyword", "ignore_above": 8191},
            "skills": {"type": "keyword", "ignore_above": 8191},
            "has_unresolved_organizations": {"type": "boolean"},
            "unresolved_organization_ids": {"type": "long", "doc_values": False},
            "roles": {
                "type": "object", "dynamic": False,
                "properties": {
                    "id": stored_long(),
                    "role_title": text_keyword(),
                    "organization_id": {"type": "long", "doc_values": False},
                    "organization_name": text_keyword(),
                    "organization_unresolved": {"type": "boolean"},
                },
            },
            "organizations": {
                "type": "object", "dynamic": False,
                "properties": {
                    "forager_id": {"type": "long", "doc_values": False},
                    "linkedin_id": stored_long(),
                    "name": text_keyword(),
                },
            },
        },
    },
}


def index_definition(shards=4):
    """Vary physical parallelism without mutating the search/source contract."""
    if type(shards) is not int or not 1 <= shards <= 8:
        raise ValueError("INDEX_SHARDS must be between 1 and 8")
    return {**INDEX_DEFINITION, "settings": {**INDEX_DEFINITION["settings"], "number_of_shards": shards}}
