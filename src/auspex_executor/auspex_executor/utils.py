import json
import math
from typing import List, Dict, Tuple
from auspex_msgs.msg import PlanStatus
from msg_context.loader import ActionInstance, Plan, ActionStatus
from fractions import Fraction

from upf_msgs.msg import (
    Atom,
    Real
)

def enum_to_str(msg_cls, value, prefix=None):
    return next((name for name in dir(msg_cls)
                 if ((prefix and name.startswith(prefix)) or (not prefix and name.isupper()))
                 and getattr(msg_cls, name) == value), "UNKNOWN")

def parse_plan_from_dict(plan_dict_list):
    """
    Parses a list of dictionaries into a list of auspex_msgs.msg.loader.Plan messages.

    :param plan_dict_list: List of dictionaries representing the plan.
    :return: List of auspex_msgs.msg.Plan messages.
    """
    parsed_plans = []

    for plan_dict in plan_dict_list:
        platform_id = plan_dict["platform_id"]
        actions = []

        for action in plan_dict["actions"]:
            action_id = int(action["id"])
            action_name = action["action_name"]
            status = enum_to_str(ActionStatus, ActionStatus.INACTIVE)

            parameters = []
            for param in action["parameters"]:
                symbol_atom = param["symbol_atom"]

                # Casting because of strings in KB
                int_atom = [int(i) for i in param["int_atom"]]
                boolean_atom = [s.lower() == "true" for s in param["boolean_atom"]]

                # Convert real_atom entries to upf_msgs.msg.Real
                real_atom = [
                    Real(numerator=int(real["numerator"]), denominator=int(real["denominator"]))
                    for real in param["real_atom"]
                ]

                # Create upf.msg.Atom instance
                atom_msg = Atom(
                    symbol_atom=symbol_atom,
                    int_atom=int_atom,
                    real_atom=real_atom,
                    boolean_atom=boolean_atom
                )
                parameters.append(atom_msg)

            # Create auspex_msgs.msg.ActionInstance
            action_instance = ActionInstance(
                id=action_id,
                action_name=action_name,
                parameters=parameters,
                status=status
            )
            actions.append(action_instance)

        # Create auspex_msgs.msg.Plan
        plan_msg = Plan(
            platform_id=platform_id,
            plan_id=int(plan_dict.get("plan_id", "0")),
            team_id=plan_dict.get("team_id", ""),
            priority=int(plan_dict.get("priority", "0")),
            status=plan_dict.get("status", "FAILED"),
            actions=actions
        )

        parsed_plans.append(plan_msg)

    return parsed_plans

def point_in_poly(x: float, y: float, poly: List[Tuple[float, float]]) -> bool:
    """Ray-casting algorithm to test if point (x=lon, y=lat) is inside polygon."""
    inside = False
    n = len(poly)
    for i in range(n):
        xi, yi = poly[i]   # xi = lon, yi = lat
        xj, yj = poly[(i + 1) % n]
        intersects = ((yi > y) != (yj > y)) and \
                     (x < (xj - xi) * (y - yi) / (yj - yi + 1e-12) + xi)
        if intersects:
            inside = not inside
    return inside

def extract_gps_points(platform_history: List[Dict], downsample: int = 10) -> List[Tuple[float, float]]:
    """
    Flattens and downsamples GPS points from platform history.
    Returns list of (lat, lon) tuples.
    """
    points = []
    for entry in platform_history:
        traj = entry.get('trajectory', [])
        for i, p in enumerate(traj):
            if i % downsample == 0:
                lat = float(p['lat'])
                lon = float(p['lon'])
                points.append((lat, lon))
    return points

def mark_footprint_cells(
    lat: float,
    lon: float,
    min_lat: float,
    min_lon: float,
    dlat: float,
    dlon: float,
    grid_size: int,
    radius_m: float = 10.0
) -> List[Tuple[int, int]]:
    """
    Returns list of grid cells within UAV footprint centered at (lat, lon).
    radius_m: coverage radius in meters.
    """
    # Convert meters to degrees
    deg_lat_radius = radius_m / 111_000.0  # meters per degree latitude
    deg_lon_radius = radius_m / (111_000.0 * abs(math.cos(math.radians(lat))) + 1e-6)

    lat_start = lat - deg_lat_radius
    lat_end = lat + deg_lat_radius
    lon_start = lon - deg_lon_radius
    lon_end = lon + deg_lon_radius

    i_start = max(0, int((lat_start - min_lat) / dlat))
    i_end = min(grid_size - 1, int((lat_end - min_lat) / dlat))
    j_start = max(0, int((lon_start - min_lon) / dlon))
    j_end = min(grid_size - 1, int((lon_end - min_lon) / dlon))

    cells = []
    for i in range(i_start, i_end + 1):
        for j in range(j_start, j_end + 1):
            cells.append((i, j))
    return cells

def compute_coverage(
    search_area_points: List[Dict],
    platform_history: List[Dict],
    downsample: int = 10,
    grid_size: int = 100,
    uav_radius_m: float = 10.0
) -> float:
    """
    Estimates coverage percentage of the given search polygon by GPS histories.
    Returns coverage in [0.0, 1.0]
    """
    # 1) Prepare polygon: [(lon, lat)]
    poly = [(float(p['longitude']), float(p['latitude'])) for p in search_area_points]

    # 2) Extract and downsample GPS points
    gps_points = extract_gps_points(platform_history, downsample)

    # 3) Bounding box
    lats = [lat for _, lat in poly]
    lons = [lon for lon, _ in poly]
    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)

    # 4) Grid cell size
    dlat = (max_lat - min_lat) / grid_size
    dlon = (max_lon - min_lon) / grid_size

    # 5) Identify valid cells inside the polygon
    valid_cells = set()
    for i in range(grid_size):
        for j in range(grid_size):
            lat = min_lat + (i + 0.5) * dlat
            lon = min_lon + (j + 0.5) * dlon
            if point_in_poly(lon, lat, poly):  # switched order for consistency
                valid_cells.add((i, j))

    # 6) Mark visited cells using UAV footprint
    visited = set()
    for lat, lon in gps_points:
        if not point_in_poly(lon, lat, poly):
            continue
        cells = mark_footprint_cells(lat, lon, min_lat, min_lon, dlat, dlon, grid_size, uav_radius_m)
        visited.update(cells)

    # 7) Compute coverage
    if not valid_cells:
        return 0.0
    covered = visited & valid_cells

    # # Debug info
    # print(f"Valid grid cells inside polygon: {len(valid_cells)}")
    # print(f"Visited cells: {len(visited)}")
    # print(f"Overlap: {len(covered)}")

    return len(covered) / len(valid_cells)