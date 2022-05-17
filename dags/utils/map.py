import os
import json
import random
from functools import partial

import pyproj
import xml.etree.ElementTree as ET
import networkx as nx
from shapely.geometry import shape, Point, MultiPoint, LineString
from shapely.ops import nearest_points
from shapely.ops import transform as shapely_transform


def load_bangkok_map(**kwargs):
    mapfile_path = os.path.join(os.getcwd(), "assets", "BK_NW_01.xml")
    with open(mapfile_path, "r", encoding="utf-8") as mapfile:
        network_xml = ET.parse(mapfile)
    G = nx.Graph()
    G.add_nodes_from([d.get("id") for d in network_xml.find("nodes").findall("node")])
    for start, end, length in [
        (d.get("from"), d.get("to"), d.get("length"))
        for d in network_xml.find("links").findall("link")
    ]:
        G.add_edge(start, end, length=float(length))

    network_nodes = MultiPoint(
        [
            Point(float(d.get("x")), float(d.get("y")))
            for d in network_xml.find("nodes").findall("node")
        ]
    )
    node_geom_to_id = {}
    for p in network_xml.find("nodes").findall("node"):
        node_geom_to_id[p.get("x") + "-" + p.get("y")] = p.get("id")

    node_id_to_geom = {}
    for p in network_xml.find("nodes").findall("node"):
        node_id_to_geom[p.get("id")] = Point(float(p.get("x")), float(p.get("y")))

    return {
        "G": G,
        "network_nodes": network_nodes,
        "node_geom_to_id": node_geom_to_id,
        "node_id_to_geom": node_id_to_geom,
    }


def random_route(
    network_nodes, node_geom_to_id, node_id_to_geom, amounts=10, **kwargs
):
    zonefile_path = os.path.join(os.getcwd(), "assets", "exportJSON_TAZ_UTM47N.json")
    with open(zonefile_path, "r", encoding="utf-8") as zonefile:
        TAZ = json.loads(zonefile.read())
    zone_centroid_nearest_nodes = []
    for zone in TAZ:
        centroid_nearest_node = nearest_points(
            shape(zone["geometry"]).centroid, network_nodes
        )[1]
        zone_centroid_nearest_nodes.append(
            node_geom_to_id[
                str(centroid_nearest_node.x) + "-" + str(centroid_nearest_node.y)
            ]
        )

    random_routes = []
    for i in range(amounts):
        origin_rand, destination_rand = random.sample(zone_centroid_nearest_nodes, 2)
        random_routes.append(
            {"org_node_id": origin_rand, "dest_node_id": destination_rand}
        )

    return random_routes


def map_to_normal_coor(route):
    project = partial(
        pyproj.transform,
        pyproj.Proj(init="epsg:32647"),  # source coordinate system
        pyproj.Proj(init="epsg:4326"),   # destination coordinate system
    )

    route_normal_coor = shapely_transform(project, route)  ### change coordinate system
    route_normal_coor = [[d.y, d.x] for d in list(route_normal_coor)]  ### swap x, y
    return route_normal_coor


def sample_shortest_path(
    G,
    network_nodes,
    node_geom_to_id,
    node_id_to_geom,
    route,
    amounts=30,
    **kwargs
):
    route = nx.shortest_path(G, route["org_node_id"], route["dest_node_id"], weight="length")

    path_point = [node_id_to_geom[nid] for nid in route]
    path = LineString(path_point)

    sampling_route = MultiPoint(
        [
            path.interpolate(float(n) / amounts, normalized=True)
            for n in range(amounts + 1)
        ]
    )
    
    return sampling_route
    
