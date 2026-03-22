from stream_fusion.utils.models.media import Media


class Movie(Media):
    def __init__(self, id, tmdb_id, titles, year, languages):
        super().__init__(id, tmdb_id, titles, languages, "movie")
        self.year = year

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["year"] = self.year
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "Movie":
        return cls(
            id=data["id"],
            tmdb_id=data["tmdb_id"],
            titles=data["titles"],
            year=data.get("year"),
            languages=data["languages"],
        )
