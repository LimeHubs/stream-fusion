class Media:
    def __init__(self, id, tmdb_id, titles, languages, type):
        self.id = id
        self.tmdb_id = tmdb_id
        self.titles = titles
        self.languages = languages
        self.type = type

    def to_dict(self) -> dict:
        return {
            "__type__": self.type,
            "id": self.id,
            "tmdb_id": self.tmdb_id,
            "titles": self.titles,
            "languages": self.languages,
        }
