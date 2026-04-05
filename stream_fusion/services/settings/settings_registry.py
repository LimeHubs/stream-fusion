from dataclasses import dataclass, field
from typing import Literal

SettingType = Literal["bool", "int", "str", "float", "enum"]


@dataclass
class SettingDef:
    key: str
    type: SettingType
    label: str
    category: str
    requires_restart: bool = False
    enum_choices: list[str] = field(default_factory=list)
    description: str = ""


# ---------------------------------------------------------------------------
# Registry — single source of truth for all admin-configurable settings.
# The `default` value is NOT stored here; it is read at runtime from the
# live `settings` singleton so it always reflects the current env var.
# ---------------------------------------------------------------------------

SETTINGS_REGISTRY: list[SettingDef] = [

    # ── Général ────────────────────────────────────────────────────────────

    SettingDef(
        "allow_debrid_download", "bool",
        "Autoriser les téléchargements debrid", "general",
        description=(
            "Lorsqu'activé, les liens de téléchargement debrid (torrents non encore en cache) "
            "apparaissent dans les résultats Stremio de l'utilisateur. "
            "Sur une instance publique, désactivez cette option pour éviter que des utilisateurs "
            "anonymes ne déclenchent des téléchargements sur votre compte debrid. "
            "Les liens de stream depuis le cache debrid restent disponibles quelle que soit cette valeur."
        ),
    ),
    SettingDef(
        "allow_public_key_registration", "bool",
        "Inscription publique aux clés API", "general",
        description=(
            "Permet à n'importe qui d'accéder à la page /register pour générer sa propre clé API "
            "et configurer son addon Stremio de façon autonome. "
            "Désactivez cette option sur les instances privées ou à accès restreint — "
            "les clés devront alors être créées manuellement dans l'onglet 'Clés API'."
        ),
    ),
    SettingDef(
        "use_https", "bool",
        "Utiliser HTTPS pour les URLs générées", "general",
        description=(
            "Force le schéma https:// dans toutes les URLs construites par l'application "
            "(URLs d'addon Stremio, liens de stream proxifiés, webhooks, etc.). "
            "Activez cette option si stream-fusion est derrière un reverse proxy TLS (Nginx, Caddy, Traefik). "
            "Si vous exposez l'application directement en HTTP, laissez cette option désactivée "
            "pour éviter des URLs cassées."
        ),
    ),
    SettingDef(
        "proxied_link", "bool",
        "Proxifier les liens de stream", "proxy",
        description=(
            "Lorsqu'activé, les liens de stream renvoyés à Stremio passent par stream-fusion "
            "au lieu de pointer directement vers le CDN debrid. "
            "Utile pour masquer l'IP de l'utilisateur, contourner les restrictions géographiques "
            "des CDN debrid, ou centraliser la bande passante. "
            "Activé automatiquement si RD_TOKEN ou AD_TOKEN est défini dans l'environnement. "
            "Désactivez si vous préférez que les clients se connectent directement au CDN "
            "(meilleure performance, moins de charge serveur)."
        ),
    ),
    SettingDef(
        "download_service", "enum",
        "Service debrid par défaut du serveur", "proxy",
        enum_choices=["", "Real-Debrid", "AllDebrid", "TorBox", "Premiumize",
                      "Debrid-Link", "EasyDebrid", "Offcloud", "PikPak"],
        description=(
            "Service debrid utilisé par défaut pour les requêtes serveur (vérification de cache, "
            "ajout de magnets, génération de liens) quand aucun service n'est spécifié dans "
            "la configuration utilisateur. "
            "Laissez vide pour n'avoir aucun service par défaut — les utilisateurs devront "
            "configurer leur propre service dans leur addon."
        ),
    ),
    SettingDef(
        "log_level", "enum",
        "Niveau de verbosité des logs", "general",
        enum_choices=["NOTSET", "TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "FATAL"],
        description=(
            "Contrôle la quantité de messages écrits dans les logs. "
            "TRACE : tous les détails internes, utile pour le débogage (très verbeux). "
            "DEBUG : détails internes, utile pour le débogage (verbeux). "
            "INFO : messages d'état normaux, valeur recommandée en production. "
            "WARNING : uniquement les situations anormales non bloquantes. "
            "ERROR : uniquement les erreurs qui empêchent une opération de se terminer. "
            "FATAL : uniquement les erreurs critiques qui font crasher l'application. "
            "NOTSET : hérite du logger parent (comportement système)."
        ),
    ),
    SettingDef(
        "no_cache_video_language", "enum",
        "Langue de la vidéo 'non disponible en cache'", "general",
        enum_choices=["FR", "EN"],
        description=(
            "Lorsqu'un utilisateur clique sur un résultat debrid dont le torrent n'est pas encore "
            "en cache (téléchargement en cours), stream-fusion renvoie une courte vidéo d'attente "
            "à la place du flux réel. "
            "Sélectionnez la langue de cette vidéo d'information selon votre audience principale. "
            "FR : vidéo en français, EN : vidéo en anglais."
        ),
    ),
    SettingDef(
        "aiohttp_timeout", "int",
        "Timeout des requêtes HTTP sortantes (secondes)", "system",
        description=(
            "Durée maximale d'attente pour toutes les requêtes HTTP effectuées par stream-fusion "
            "vers des services externes : API debrid, indexeurs torrent, TMDB, etc. "
            "Une valeur trop faible peut provoquer des timeouts sur des connexions lentes. "
            "Une valeur trop élevée peut bloquer des workers Gunicorn sur des services non répondants. "
            "Valeur par défaut : 7200 secondes (2h) — élevée pour couvrir les transferts debrid longs."
        ),
    ),
    SettingDef(
        "gunicorn_timeout", "int",
        "Timeout des workers Gunicorn (secondes)", "system",
        requires_restart=True,
        description=(
            "Durée maximale qu'un worker Gunicorn peut passer sur une seule requête avant d'être "
            "tué et redémarré par le master process. "
            "Si une requête dépasse ce délai, Gunicorn envoie SIGKILL au worker — la requête "
            "client reçoit une erreur 502. "
            "Doit être supérieur à aiohttp_timeout pour éviter des kills prématurés. "
            "Nécessite un redémarrage de l'application pour prendre effet."
        ),
    ),

    # ── Proxy & Rate limits ────────────────────────────────────────────────

    SettingDef(
        "playback_proxy", "bool",
        "Activer le proxy de lecture des flux", "proxy",
        description=(
            "Lorsqu'activé, les flux vidéo sont relayés par stream-fusion via l'URL proxy "
            "configurée (variable d'environnement PROXY_URL). "
            "Utile pour contourner des restrictions réseau côté utilisateur ou pour anonymiser "
            "les requêtes vers les CDN debrid. "
            "Augmente significativement la bande passante consommée par le serveur. "
            "Sans effet si PROXY_URL n'est pas défini dans l'environnement."
        ),
    ),
    SettingDef(
        "proxy_buffer_size", "int",
        "Taille du buffer de proxy (octets)", "proxy",
        description=(
            "Quantité de données (en octets) lues en une seule opération lors du relayage "
            "des flux vidéo. "
            "Un buffer plus grand améliore le débit sur les connexions rapides et réduit "
            "le nombre d'appels système, mais consomme plus de mémoire par connexion active. "
            "Valeur par défaut : 1 048 576 octets (1 Mo). "
            "Réduisez à 131 072 (128 Ko) si le serveur manque de RAM avec beaucoup de streams simultanés."
        ),
    ),
    SettingDef(
        "playback_limit_requests", "int",
        "Nombre max de requêtes de lecture par fenêtre", "proxy",
        description=(
            "Nombre maximum de requêtes de stream qu'un même utilisateur (identifié par sa clé API) "
            "peut effectuer dans la fenêtre de temps définie par 'Fenêtre de limite'. "
            "Au-delà, les requêtes supplémentaires reçoivent une erreur 429 (Too Many Requests). "
            "Sert à prévenir l'abus de la fonctionnalité de proxy par des clients bogués "
            "ou des utilisateurs qui spamment les lectures. "
            "Valeur par défaut : 60 requêtes."
        ),
    ),
    SettingDef(
        "playback_limit_seconds", "int",
        "Fenêtre de rate limiting (secondes)", "proxy",
        description=(
            "Durée de la fenêtre glissante utilisée pour le rate limiting des requêtes de lecture. "
            "Combiné avec 'Nombre max de requêtes', définit la règle : "
            "N requêtes maximum sur les X dernières secondes. "
            "Exemple avec les valeurs par défaut : 60 requêtes max sur 60 secondes = 1 req/s en moyenne. "
            "Un utilisateur peut faire un burst jusqu'à 60 req en une seconde, puis doit attendre."
        ),
    ),

    # ── Cache ──────────────────────────────────────────────────────────────

    SettingDef(
        "redis_expiration", "int",
        "TTL par défaut du cache Redis (secondes)", "cache",
        description=(
            "Durée de vie par défaut des entrées stockées dans Redis (résultats de recherche "
            "d'indexeurs, disponibilité debrid, métadonnées TMDB mises en cache, etc.). "
            "Après ce délai, les données expirent et seront récupérées à nouveau depuis la source. "
            "Une valeur plus longue améliore les performances et réduit les appels aux API externes, "
            "mais peut servir des résultats obsolètes (nouveaux torrents non visibles). "
            "Valeur par défaut : 604 800 secondes (7 jours)."
        ),
    ),
    SettingDef(
        "bg_refresh_indexer_ttl", "int",
        "TTL du verrou de refresh background (secondes)", "cache",
        description=(
            "Durée pendant laquelle un verrou Redis est maintenu par indexeur lors d'un "
            "refresh en arrière-plan des résultats de recherche. "
            "Ce verrou empêche plusieurs workers Gunicorn de rafraîchir le même indexeur "
            "simultanément pour la même requête (évite les doublons et la surcharge des API). "
            "Si un refresh prend plus longtemps que ce TTL, le verrou expire et un autre "
            "worker peut tenter un nouveau refresh. "
            "Valeur par défaut : 21 600 secondes (6 heures)."
        ),
    ),

    # ── Scheduler ─────────────────────────────────────────────────────────

    SettingDef(
        "scheduler_enabled", "bool",
        "Activer le planificateur de nettoyage", "scheduler",
        description=(
            "Active les tâches de maintenance automatique qui s'exécutent en arrière-plan : "
            "suppression des torrents anciens, purge du cache debrid expiré, désactivation "
            "des clés API/peer expirées. "
            "En environnement multi-worker (Gunicorn), une élection de leader via Redis garantit "
            "qu'un seul worker exécute le scheduler à la fois. "
            "Désactivez uniquement si vous gérez la maintenance manuellement via l'onglet Maintenance."
        ),
    ),
    SettingDef(
        "scheduler_torrent_orphan_max_age_days", "int",
        "Âge max des torrents sans TMDB (jours)", "scheduler",
        description=(
            "Les torrents sans identifiant TMDB (non identifiés) créés il y a plus de N jours "
            "sont supprimés automatiquement. "
            "Après ce délai, le job de matching automatique et les recherches utilisateurs ont eu "
            "suffisamment d'opportunités de les résoudre — s'ils restent orphelins, ils ne seront "
            "probablement jamais matchés et sont inutilisables pour Stremio. "
            "Valeur par défaut : 7 jours."
        ),
    ),
    SettingDef(
        "scheduler_debrid_cleanup_interval_hours", "int",
        "Intervalle de purge du cache debrid (heures)", "scheduler",
        description=(
            "Fréquence à laquelle le scheduler supprime les entrées expirées de la table "
            "debrid_cache. Ces entrées correspondent à des vérifications de disponibilité "
            "debrid (est-ce que tel hash est en cache chez Real-Debrid, AllDebrid, etc.) "
            "qui ont dépassé leur TTL. "
            "Un intervalle court maintient la base propre mais génère plus de charge SQL. "
            "Valeur par défaut : toutes les 6 heures."
        ),
    ),
    SettingDef(
        "scheduler_torrent_cleanup_interval_hours", "int",
        "Intervalle de purge des torrents (heures)", "scheduler",
        description=(
            "Fréquence à laquelle le scheduler effectue la purge et la déduplication "
            "de la table torrent_items : suppression des entrées trop anciennes (selon "
            "'Âge maximum des torrents'), élimination des doublons par hash/indexeur. "
            "Cette opération peut être coûteuse sur de grandes bases — ne pas descendre "
            "en dessous de 12 heures. "
            "Valeur par défaut : toutes les 24 heures."
        ),
    ),
    SettingDef(
        "scheduler_keys_cleanup_interval_hours", "int",
        "Intervalle de désactivation des clés expirées (heures)", "scheduler",
        description=(
            "Fréquence à laquelle le scheduler passe les clés API et peer keys arrivées "
            "à leur date d'expiration à l'état 'inactive'. "
            "Les clés inactives sont refusées lors de l'authentification mais conservées "
            "en base pour l'historique. "
            "Valeur par défaut : toutes les 6 heures."
        ),
    ),

    # ── TMDB ──────────────────────────────────────────────────────────────

    SettingDef(
        "tmdb_language", "str",
        "Langue des métadonnées TMDB", "tmdb",
        description=(
            "Code de langue BCP 47 utilisé pour toutes les requêtes vers l'API TMDB : "
            "titres de films/séries, synopsis, titres d'épisodes, etc. "
            "Format : langue-RÉGION (exemples : fr-FR, en-US, de-DE, es-ES). "
            "La langue principale influence les résultats de recherche et les métadonnées "
            "retournées à Stremio. "
            "Si la langue demandée n'est pas disponible pour un titre, TMDB renvoie "
            "automatiquement l'anglais en fallback. "
            "Valeur par défaut : fr-FR."
        ),
    ),

    # ── Indexers toggles ──────────────────────────────────────────────────

    SettingDef(
        "jackett_enable", "bool",
        "Activer Jackett", "indexers",
        description=(
            "Active l'indexeur Jackett, un agrégateur multi-trackers qui expose une interface "
            "Torznab unifiée. Jackett doit être déployé séparément et sa clé API configurée "
            "via la variable d'environnement JACKETT_API_KEY. "
            "Quand désactivé, aucune recherche n'est envoyée à Jackett, même si les utilisateurs "
            "ont activé Jackett dans leur config personnelle."
        ),
    ),
    SettingDef(
        "c411_enable", "bool",
        "Activer C411", "indexers",
        description=(
            "Active l'indexeur C411 (tracker privé francophone). "
            "Expose une interface Torznab compatible. "
            "Requiert une clé API (C411_API_KEY) et optionnellement une passkey (C411_PASSKEY) "
            "pour les announce URLs. "
            "Quand désactivé au niveau serveur, l'indexeur est masqué pour tous les utilisateurs."
        ),
    ),
    SettingDef(
        "torr9_enable", "bool",
        "Activer Torr9", "indexers",
        description=(
            "Active l'indexeur Torr9, tracker francophone proposant une API Torznab. "
            "Requiert une clé API (TORR9_API_KEY). "
            "Quand désactivé au niveau serveur, l'indexeur est masqué pour tous les utilisateurs "
            "indépendamment de leur configuration personnelle."
        ),
    ),
    SettingDef(
        "lacale_enable", "bool",
        "Activer LaCale", "indexers",
        description=(
            "Active l'indexeur LaCale, tracker privé francophone avec endpoint Torznab. "
            "Requiert une clé API (LACALE_API_KEY). "
            "Quand désactivé au niveau serveur, l'indexeur est masqué pour tous les utilisateurs."
        ),
    ),
    SettingDef(
        "generationfree_enable", "bool",
        "Activer GénérationFree", "indexers",
        description=(
            "Active l'indexeur GénérationFree (tracker communautaire francophone). "
            "Requiert une clé API (GENERATIONFREE_API_KEY) et une passkey (GENERATIONFREE_PASSKEY). "
            "Quand désactivé au niveau serveur, l'indexeur est masqué pour tous les utilisateurs."
        ),
    ),
    SettingDef(
        "abn_enable", "bool",
        "Activer ABN (Abnormal)", "indexers",
        description=(
            "Active l'indexeur ABN (Abnormal), tracker privé avec API dédiée et interface Torznab. "
            "Requiert ABN_API_KEY et optionnellement ABN_PASSKEY. "
            "Quand désactivé au niveau serveur, l'indexeur est masqué pour tous les utilisateurs."
        ),
    ),
    SettingDef(
        "g3mini_enable", "bool",
        "Activer G3Mini (Gemini Tracker)", "indexers",
        description=(
            "Active l'indexeur G3Mini (Gemini Tracker), tracker privé avec interface Torznab. "
            "Requiert G3MINI_API_KEY et optionnellement G3MINI_PASSKEY. "
            "Quand désactivé au niveau serveur, l'indexeur est masqué pour tous les utilisateurs."
        ),
    ),
    SettingDef(
        "theoldschool_enable", "bool",
        "Activer TheOldSchool", "indexers",
        description=(
            "Active l'indexeur TheOldSchool, tracker privé avec interface Torznab. "
            "Requiert THEOLDSCHOOL_API_KEY et optionnellement THEOLDSCHOOL_PASSKEY. "
            "Quand désactivé au niveau serveur, l'indexeur est masqué pour tous les utilisateurs."
        ),
    ),
    SettingDef(
        "utopeer_unique_account", "bool",
        "u2p / Utopeer — Compte unique serveur", "indexers",
        description=(
            "Lorsqu'activé, stream-fusion utilise la passkey u2p configurée côté serveur "
            "(variable UTOPEER_PASSKEY) pour tous les utilisateurs, quelle que soit leur configuration "
            "personnelle. Utile si l'administrateur dispose d'un compte u2p partagé pour l'instance. "
            "Lorsque désactivé, chaque utilisateur doit fournir sa propre passkey u2p dans "
            "sa configuration d'addon."
        ),
    ),

    # ── Indexer URLs ──────────────────────────────────────────────────────

    SettingDef(
        "utopeer_relay_urls_raw", "str",
        "Relais u2p / Utopeer (WebSocket)", "indexer_urls",
        description=(
            "Adresses WSS des relais Nostr u2p interrogés en parallèle lors de chaque recherche. "
            "Une URL par ligne. Les relais hors-ligne sont détectés automatiquement et exclus "
            "temporairement (cache 5 min). Exemple : wss://u2p.anhkagi.net"
        ),
    ),
    SettingDef(
        "jackett_host", "str",
        "Hôte de l'instance Jackett", "indexer_urls",
        description=(
            "Nom d'hôte DNS ou adresse IP de l'instance Jackett. "
            "En déploiement Docker Compose, correspond typiquement au nom du service "
            "(ex : 'jackett'). "
            "Le schéma (http/https) et le port (9117 par défaut) sont gérés séparément "
            "via JACKETT_SCHEMA et JACKETT_PORT dans l'environnement."
        ),
    ),
    SettingDef(
        "c411_url", "str",
        "URL de base C411", "indexer_urls",
        description=(
            "URL racine de l'instance C411 utilisée pour construire les endpoints Torznab. "
            "Modifiez uniquement si C411 change de domaine ou si vous utilisez un proxy local."
        ),
    ),
    SettingDef(
        "torr9_url", "str",
        "URL de l'API Torr9", "indexer_urls",
        description=(
            "URL racine de l'API Torr9. "
            "Modifiez uniquement si Torr9 change de domaine ou si vous passez par un proxy."
        ),
    ),
    SettingDef(
        "lacale_url", "str",
        "URL du endpoint Torznab LaCale", "indexer_urls",
        description=(
            "URL complète du endpoint Torznab de LaCale. "
            "Contrairement aux autres indexeurs, LaCale expose directement le endpoint "
            "Torznab complet dans son URL (chemin inclus). "
            "Modifiez si le tracker change d'adresse."
        ),
    ),
    SettingDef(
        "generationfree_url", "str",
        "URL de base GénérationFree", "indexer_urls",
        description=(
            "URL racine du site GénérationFree. "
            "Utilisée pour construire les endpoints Torznab et les announce URLs. "
            "Modifiez uniquement en cas de changement de domaine."
        ),
    ),
    SettingDef(
        "abn_url", "str",
        "URL de base ABN", "indexer_urls",
        description=(
            "URL racine du site ABN (Abnormal), utilisée pour les pages web et certains endpoints. "
            "L'API ABN utilise un sous-domaine séparé (abn_api_url). "
            "Modifiez uniquement en cas de changement de domaine."
        ),
    ),
    SettingDef(
        "g3mini_url", "str",
        "URL de base G3Mini (Gemini Tracker)", "indexer_urls",
        description=(
            "URL racine du site G3Mini, utilisée pour construire les endpoints Torznab. "
            "Modifiez uniquement en cas de changement de domaine du tracker."
        ),
    ),
    SettingDef(
        "theoldschool_url", "str",
        "URL de base TheOldSchool", "indexer_urls",
        description=(
            "URL racine de TheOldSchool, utilisée pour construire les endpoints Torznab. "
            "Modifiez uniquement en cas de changement de domaine du tracker."
        ),
    ),
    SettingDef(
        "zilean_host", "str",
        "Hôte de l'API Zilean DMM", "indexer_urls",
        description=(
            "Nom d'hôte ou adresse IP de l'instance Zilean. "
            "Zilean est un agrégateur de métadonnées DMM (Debrid Media Manager) qui expose "
            "une API de recherche de hashs disponibles sur les services debrid. "
            "En déploiement Docker Compose, peut correspondre au nom du service Zilean. "
            "Le schéma (https par défaut) et le port sont gérés par ZILEAN_SCHEMA et ZILEAN_PORT."
        ),
    ),

    # ── Système (pool, workers) — nécessitent un redémarrage ─────────────

    SettingDef(
        "workers_count", "int",
        "Nombre de workers Gunicorn", "system",
        requires_restart=True,
        description=(
            "Nombre de processus workers Gunicorn lancés pour traiter les requêtes en parallèle. "
            "Chaque worker est un processus Python indépendant avec sa propre mémoire. "
            "Règle habituelle : (nb_CPU × 2) + 1. "
            "Augmenter le nombre de workers améliore la concurrence mais multiplie la consommation "
            "mémoire (chaque worker charge l'intégralité de l'application). "
            "La valeur par défaut est calculée automatiquement au démarrage selon les CPUs détectés. "
            "Nécessite un redémarrage complet de l'application."
        ),
    ),
    SettingDef(
        "pg_pool_size", "int",
        "Taille du pool de connexions PostgreSQL", "system",
        requires_restart=True,
        description=(
            "Nombre de connexions persistantes maintenues dans le pool SQLAlchemy par worker. "
            "Ces connexions sont réutilisées entre les requêtes pour éviter le coût "
            "d'ouverture/fermeture à chaque fois. "
            "Le nombre total de connexions à PostgreSQL est : pool_size × workers_count. "
            "Assurez-vous que PostgreSQL supporte suffisamment de connexions simultanées "
            "(paramètre max_connections). "
            "Valeur par défaut : 100. Nécessite un redémarrage."
        ),
    ),
    SettingDef(
        "pg_max_overflow", "int",
        "Débordement max du pool PostgreSQL", "system",
        requires_restart=True,
        description=(
            "Nombre de connexions supplémentaires autorisées au-delà de pg_pool_size "
            "en cas de pic de charge. Ces connexions sont créées à la demande et fermées "
            "dès qu'elles sont libérées (contrairement aux connexions du pool qui restent ouvertes). "
            "Total max par worker : pool_size + max_overflow. "
            "Valeur par défaut : 50. Nécessite un redémarrage."
        ),
    ),
    SettingDef(
        "zilean_pool_connections", "int",
        "Connexions de base du pool HTTP Zilean", "system",
        requires_restart=True,
        description=(
            "Nombre de connexions HTTP persistantes maintenues vers l'API Zilean. "
            "Augmentez si les recherches Zilean sont fréquentes et que vous observez "
            "des délais de connexion. "
            "Nécessite un redémarrage de l'application."
        ),
    ),
    SettingDef(
        "zilean_api_pool_maxsize", "int",
        "Taille maximale du pool HTTP Zilean", "system",
        requires_restart=True,
        description=(
            "Nombre maximum de connexions HTTP simultanées vers l'API Zilean. "
            "Plafond au-delà duquel les requêtes vers Zilean attendent qu'une connexion "
            "se libère dans le pool. "
            "À ajuster en fonction du débit de l'instance Zilean et du nombre de "
            "recherches parallèles attendues. "
            "Nécessite un redémarrage de l'application."
        ),
    ),
]

# Index rapide par clé
REGISTRY_BY_KEY: dict[str, SettingDef] = {s.key: s for s in SETTINGS_REGISTRY}

# Ensemble des clés configurables (pour validation rapide)
CONFIGURABLE_KEYS: frozenset[str] = frozenset(REGISTRY_BY_KEY)

# Groupement par catégorie pour le rendu des templates
REGISTRY_BY_CATEGORY: dict[str, list[SettingDef]] = {}
for _s in SETTINGS_REGISTRY:
    REGISTRY_BY_CATEGORY.setdefault(_s.category, []).append(_s)

# Catégories pour chaque sous-page admin
PAGE_CATEGORIES: dict[str, list[str]] = {
    "general": ["general"],
    "proxy": ["proxy"],
    "cache": ["cache", "scheduler"],
    "indexers": ["indexers", "indexer_urls", "tmdb"],
    "system": ["system"],
}
