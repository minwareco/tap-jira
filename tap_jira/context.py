from datetime import datetime
from singer import utils, metadata
import singer


class Context():
    config = None
    state = None
    catalog = None
    client = None
    stream_map = {}

    @classmethod
    def get_projects(cls):
        projectsRaw = cls.config.get("projects", None)
        if projectsRaw:
            # convert comma-separated list into array
            projectsList = list(map(lambda p: p.strip(), projectsRaw.split(",")))
            # filter out empty strings
            projectsList = list(filter(lambda p: len(p) > 0, projectsList))
            return projectsList
        return []

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s.tap_stream_id: s for s in cls.catalog.streams}
        return cls.stream_map.get(stream_name)

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        if stream is None:
            return False
        stream_metadata = metadata.to_map(stream.metadata)
        return metadata.get(stream_metadata, (), 'selected')

    @classmethod
    def bookmarks(cls):
        if "bookmarks" not in cls.state:
            cls.state["bookmarks"] = {}
        return cls.state["bookmarks"]

    @classmethod
    def bookmark(cls, paths):
        bookmark = cls.bookmarks()
        for path in paths:
            if path not in bookmark:
                bookmark[path] = {}
            bookmark = bookmark[path]
        return bookmark

    @classmethod
    def set_bookmark(cls, path, val):
        if isinstance(val, datetime):
            val = utils.strftime(val)

        if val is None:
            cls.bookmark(path[:-1]).pop(path[-1], None)
        else:
            cls.bookmark(path[:-1])[path[-1]] = val

    @classmethod
    def update_start_date_bookmark(cls, path):
        val = cls.bookmark(path)
        if not val:
            val = cls.config["start_date"]
            val = utils.strptime_to_utc(val)
            cls.set_bookmark(path, val)
        if isinstance(val, str):
            val = utils.strptime_to_utc(val)
        return val

    @classmethod
    def retrieve_timezone(cls):
        response = cls.client.send("GET", "/rest/api/2/myself")
        response.raise_for_status()
        return response.json()["timeZone"]
