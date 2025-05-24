class Version:
    def __init__(self, version="1.0.0", release_date="2025-05-15", author="Julian Wu", comment="MDD Framework"):
        self.version = version
        self.release_date = release_date
        self.author = author
        self.comment = comment

    def __repr__(self):
        return (f"Version(version={self.version!r}, release_date={self.release_date!r}, "
                f"author={self.author!r}, comment={self.comment!r})")

    def __str__(self):
        return f"Version {self.version} by {self.author} (Released: {self.release_date}) - {self.comment}"
