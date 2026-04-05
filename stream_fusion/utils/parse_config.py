import json

from stream_fusion.utils.string_encoding import decrypt_config
from stream_fusion.logging_config import logger


# Default search category per indexer.
# Categories control both search phase and sort priority:
#   priority_private    → Phase 1 (always queried), sort priority 1
#   intermediary_private → Phase 2 (if private results < minCachedResults), sort priority 2
#   fallback_private    → Phase 3 (last resort), sort priority 3
#   public              → Phase 1 or last (depending on utopeerPriority), not counted in private threshold
DEFAULT_INDEXER_CATEGORIES = {
    "c411":           "priority_private",
    "torr9":          "priority_private",
    "lacale":         "intermediary_private",
    "generationfree": "intermediary_private",
    "g3mini":         "intermediary_private",
    "theoldschool":   "intermediary_private",
    "abn":            "fallback_private",
    "zilean":         "fallback_private",
    "jackett":        "fallback_private",
    "utopeer":        "public",
}


def parse_config(config_token):
    config = json.loads(decrypt_config(config_token))

    if "languages" not in config:
        config["languages"] = [config["language"]]

    if "jackett" not in config:
        config["jackett"] = False

    if isinstance(config.get("RDToken"), str):
        try:
            config["RDToken"] = json.loads(config["RDToken"])
        except json.JSONDecodeError:
            pass

    if "anonymizeMagnets" not in config:
        config["anonymizeMagnets"] = False

    if "addonHost" not in config:
        logger.warning("addonHost not found in config, using default")
        config["addonHost"] = "http://127.0.0.1:8000"

    # Indexer enable/disable (backward compat — derived from indexerCategories if present)
    if "indexerCategories" not in config:
        config["indexerCategories"] = DEFAULT_INDEXER_CATEGORIES.copy()
    else:
        for key, default_cat in DEFAULT_INDEXER_CATEGORIES.items():
            config["indexerCategories"].setdefault(key, default_cat)

    # Derive boolean enable flags from categories (disabled = category missing or "disabled")
    for key in DEFAULT_INDEXER_CATEGORIES:
        if key not in config:
            cat = config["indexerCategories"].get(key, "disabled")
            config[key] = cat != "disabled"

    # Legacy configs that have booleans but no indexerCategories: keep them as-is
    # (already handled above since indexerCategories was just set to defaults)

    if "utopeer" not in config:
        config["utopeer"] = True

    # Whether to query Utopeer (public) in Phase 1 (true) or after all private phases (false)
    if "utopeerPriority" not in config:
        config["utopeerPriority"] = True

    # Skip RealDebrid if already N cached results from prior services (RD API is slow/restrictive)
    # 0 = disabled (always check RD if maxResults not reached)
    if "rdMinCachedBeforeCheck" not in config:
        config["rdMinCachedBeforeCheck"] = 3

    # Cache-first: minimum confirmed Postgres results to skip live indexer search entirely.
    # 0 = disabled (always query live indexers)
    if "minPostgresResults" not in config:
        config["minPostgresResults"] = 5

    # Cache-first TTL: max age in days of Postgres results before forcing a live search refresh.
    # 0 = disabled (always trust Postgres cache if minPostgresResults is met)
    if "postgresMaxAgeDays" not in config:
        config["postgresMaxAgeDays"] = 7

    return config
