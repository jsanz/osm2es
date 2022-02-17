from collections import namedtuple
from osmium.osm import RelationMember


def members2dict( members):
    member_list = []
    for member in members:
        if isinstance(member, tuple):
            m = namedtuple("member", ("type", "ref", "role"))
        elif isinstance(member, RelationMember):
            m = member
        member_list.append({"ref": m.ref, "role": m.role, "type": m.type})
    return member_list


def tags2dict(tags, type):
    """
    Convert osmium TagList into python dict

    Arguments:
        tags {TagList} -- osmium TagList for a geo-object

    Returns:
        dict -- tags in a python dict
    """
    tag_dict = {}

    for tag in tags:
        if tag.k not in OSM_TAGS[type]:
            tag_dict[tag.k] = tag.v

    return tag_dict


OSM_TAGS = {
    "node": [
        "name",
        "man_made",
        "wikidata",
        "highway",
        "address",
        "amenity",
        "crossing",
        "entrance",
        "leisure",
        "natural",
        "office",
        "place",
        "shop",
        "wheelchair",
    ],
    "way": [
        "name",
        "man_made",
        "wikidata",
        "highway",
        "access",
        "aerialway",
        "barrier",
        "cycleway",
        "lanes",
        "layer",
        "junction",
        "maxspeed",
        "network",
        "oneway",
        "ref",
        "route",
        "surface",
        "waterway",
    ],
    "area": [
        "name",
        "natural",
        "man_made",
        "wikidata",
        "admin_level",
        "boundary",
        "landuse",
        "building",
    ],
    "relation": ["name", "man_made", "wikidata"],
}

INDEX_MAPPINGS = {
    "properties": {
        # common
        "osm_id": {"type": "keyword"},
        "osm_version": {"type": "integer"},
        "osm_type": {"type": "keyword"},
        "osm_user": {"type": "keyword"},
        "visible": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "nodes": {"type": "keyword"},
        "point": {"type": "geo_point"},
        "geometry": {"type": "geo_shape"},
        "other_tags": {"type": "flattened"},
        "num_tags": {"type": "integer"},
        "name": {"type": "text"},
        "man_made": {"type": "keyword"},
        "wikidata": {"type": "text"},
        "highway": {"type": "keyword"},
        "members": {
            "properties": {
                "ref": {"type": "keyword"},
                "role": {"type": "keyword"},
                "type": {"type": "keyword"},
            }
        },
        # node
        "address": {"type": "text"},
        "amenity": {"type": "keyword"},
        "crossing": {"type": "keyword"},
        "entrance": {"type": "keyword"},
        "leisure": {"type": "keyword"},
        "natural": {"type": "keyword"},
        "office": {"type": "keyword"},
        "place": {"type": "keyword"},
        "shop": {"type": "keyword"},
        "wheelchair": {"type": "keyword"},
        # way
        "access": {"type": "keyword"},
        "aerialway": {"type": "keyword"},
        "barrier": {"type": "keyword"},
        "cycleway": {"type": "keyword"},
        "lanes": {"type": "keyword"},
        "layer": {"type": "keyword"},
        "junction": {"type": "keyword"},
        "maxspeed": {"type": "keyword"},
        "network": {"type": "keyword"},
        "oneway": {"type": "keyword"},
        "ref": {"type": "text"},
        "route": {"type": "keyword"},
        "surface": {"type": "keyword"},
        "waterway": {"type": "keyword"},
        # area
        "admin_level": {"type": "keyword"},
        "boundary": {"type": "keyword"},
        "building": {"type": "keyword"},
        "landuse": {"type": "keyword"},
        "natural": {"type": "keyword"},
    }
}
