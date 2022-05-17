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


# Thi function will load the bangkok map file in xml and transform to usable variable in python
def load_bangkok_map(**kwargs):
    # define map's file path
    mapfile_path = os.path.join(os.getcwd(), "assets", "BK_NW_01.xml")
    # read map's file and parse to variable
    with open(mapfile_path, "r", encoding="utf-8") as mapfile:
        network_xml = ET.parse(mapfile)
    # create network graph
    G = nx.Graph()
    # add node from map
    G.add_nodes_from([d.get("id") for d in network_xml.find("nodes").findall("node")])
    # add edge from map
    for start, end, length in [
        (d.get("from"), d.get("to"), d.get("length"))
        for d in network_xml.find("links").findall("link")
    ]:
        G.add_edge(start, end, length=float(length))

    # create geometry points of node
    network_nodes = MultiPoint(
        [
            Point(float(d.get("x")), float(d.get("y")))
            for d in network_xml.find("nodes").findall("node")
        ]
    )
    # create mapping from geomatry to node id
    node_geom_to_id = {}
    for p in network_xml.find("nodes").findall("node"):
        node_geom_to_id[p.get("x") + "-" + p.get("y")] = p.get("id")
    # create mapping from node id to geomatry
    node_id_to_geom = {}
    for p in network_xml.find("nodes").findall("node"):
        node_id_to_geom[p.get("id")] = Point(float(p.get("x")), float(p.get("y")))

    # return usefull variables
    return {
        "G": G,
        "network_nodes": network_nodes,
        "node_geom_to_id": node_geom_to_id,
        "node_id_to_geom": node_id_to_geom,
    }


# This function will random the route by random two points from available point
# We use only the centroid of zone from `UTM zone 47N` to be sure that each random pair will not close too much
def random_route(
    network_nodes, node_geom_to_id, node_id_to_geom, amounts=10, **kwargs
):
    # Read zone file from `UTM zone 47N`
    zonefile_path = os.path.join(os.getcwd(), "assets", "exportJSON_TAZ_UTM47N.json")
    with open(zonefile_path, "r", encoding="utf-8") as zonefile:
        TAZ = json.loads(zonefile.read())
    zone_centroid_nearest_nodes = []
    
    # for each zone, we will use the centroid of each zone for random points
    for zone in TAZ:
        # calculate the centroid of zone and find the nearest point in map
        centroid_nearest_node = nearest_points(
            shape(zone["geometry"]).centroid, network_nodes
        )[1]
        
        # append the nearest node id of zone's centroid to be use for random points
        zone_centroid_nearest_nodes.append(
            node_geom_to_id[
                str(centroid_nearest_node.x) + "-" + str(centroid_nearest_node.y)
            ]
        )

    # sample 2 points from zone_centroid_nearest_nodes created before
    random_routes = []
    for i in range(amounts):
        origin_rand, destination_rand = random.sample(zone_centroid_nearest_nodes, 2)
        random_routes.append(
            {"org_node_id": origin_rand, "dest_node_id": destination_rand}
        )

    # return the result
    return random_routes


# This function is to convert the coordinate system between map file and normal system(lat, long)
def map_to_normal_coor(route):
    # define source and destination coordinate system
    project = partial(
        pyproj.transform,
        pyproj.Proj(init="epsg:32647"),  # source coordinate system
        pyproj.Proj(init="epsg:4326"),   # destination coordinate system
    )

    # transform the coordinate system
    route_normal_coor = shapely_transform(project, route)
    # after tranform the coordinate system x and y will be swapped
    route_normal_coor = [[d.y, d.x] for d in list(route_normal_coor)]  ### swap x, y
    return route_normal_coor


# This function is to sampling the points along the given origin and destination route
def sample_shortest_path(
    G,
    network_nodes,
    node_geom_to_id,
    node_id_to_geom,
    route,
    amounts=30,
    **kwargs
):
    # find the shortest path from origin to destination point in network graph
    route = nx.shortest_path(G, route["org_node_id"], route["dest_node_id"], weight="length")

    # translate the node it to coordinate of points
    path_point = [node_id_to_geom[nid] for nid in route]
    # Packed it to line string for interpolate
    path = LineString(path_point)

    # sampling the points by interpolating along the line string with fraction i/total for point i-th
    sampling_route = MultiPoint(
        [
            path.interpolate(float(n) / amounts, normalized=True)
            for n in range(amounts + 1)
        ]
    )
    
    # return the sampling points for the route
    return sampling_route
    
