import json
import math
from typing import List, Dict, Tuple, Optional, Union
from auspex_msgs.msg import PlanStatus, Task, Action, Plan, ActionStatus

from fractions import Fraction

from upf_msgs.msg import (
    Atom,
    Real,
    Plan as UPFPlan,
    ActionInstance as UPFActionInstance
)


def enum_to_str(msg_cls, value, prefix=None):
    return next((name for name in dir(msg_cls)
                 if ((prefix and name.startswith(prefix)) or (not prefix and name.isupper()))
                 and getattr(msg_cls, name) == value), 'UNKNOWN')


# =============================================================================
# UPF Message Parsing Utilities
# =============================================================================

def parse_real_from_dict(real_dict: Dict) -> Real:
    return Real(
        numerator=int(real_dict.get("numerator", 0)),
        denominator=int(real_dict.get("denominator", 1))
    )


def parse_real_from_value(value: Union[float, int, str, Dict]) -> Real:
    if isinstance(value, dict):
        return parse_real_from_dict(value)

    if isinstance(value, str):
        value = float(value)
    
    frac = Fraction(value).limit_denominator(1000000)
    return Real(numerator=frac.numerator, denominator=frac.denominator)


def parse_atom_from_dict(atom_dict: Dict) -> Atom:
    # Handle symbol atoms (strings)
    symbol_atom = atom_dict.get("symbol_atom", [])
    if isinstance(symbol_atom, str):
        symbol_atom = [symbol_atom]
    
    # Handle integer atoms
    int_atom = atom_dict.get("int_atom", [])
    if isinstance(int_atom, int):
        int_atom = [int_atom]
    int_atom = [int(i) for i in int_atom]
    
    # Handle boolean atoms
    boolean_atom = atom_dict.get("boolean_atom", [])
    if isinstance(boolean_atom, bool):
        boolean_atom = [boolean_atom]
    elif isinstance(boolean_atom, list):
        # Convert string representations to bool
        boolean_atom = [
            b if isinstance(b, bool) else str(b).lower() == "true"
            for b in boolean_atom
        ]
    
    # Handle real atoms
    real_atom_raw = atom_dict.get("real_atom", [])
    if not isinstance(real_atom_raw, list):
        real_atom_raw = [real_atom_raw]
    real_atom = [parse_real_from_value(r) for r in real_atom_raw]
    
    return Atom(
        symbol_atom=symbol_atom,
        int_atom=int_atom,
        real_atom=real_atom,
        boolean_atom=boolean_atom
    )


def parse_action_instance_from_dict(action_dict: Dict) -> UPFActionInstance:
    # Parse parameters
    parameters = []
    params_raw = action_dict.get("parameters", [])
    for param in params_raw:
        if isinstance(param, dict):
            parameters.append(parse_atom_from_dict(param))
        elif isinstance(param, str):
            # Simple string parameter - wrap in Atom
            parameters.append(Atom(symbol_atom=[param]))
    
    # Parse timing
    time_triggered = bool(action_dict.get("time_triggered", False))
    
    start_time = Real(numerator=0, denominator=1)
    end_time = Real(numerator=0, denominator=1)
    
    if "start_time" in action_dict:
        start_time = parse_real_from_value(action_dict["start_time"])
    
    if "end_time" in action_dict:
        end_time = parse_real_from_value(action_dict["end_time"])
    
    return UPFActionInstance(
        id=str(action_dict.get("id", "")),
        action_name=action_dict.get("action_name", action_dict.get("name", "")),
        parameters=parameters,
        time_triggered=time_triggered,
        start_time=start_time,
        end_time=end_time
    )


def parse_complete_upf_plan_from_dict(plan_dict: Optional[Dict]) -> UPFPlan:
    if plan_dict is None:
        return UPFPlan(kind=0, actions=[])
    
    # Parse plan kind (default to TIME_TRIGGERED=1 for temporal plans)
    kind = int(plan_dict.get("kind", 1))
    
    # Parse action instances
    actions = []
    actions_raw = plan_dict.get("actions", [])
    for action_dict in actions_raw:
        try:
            action_instance = parse_action_instance_from_dict(action_dict)
            actions.append(action_instance)
        except Exception as e:
            # Log warning but continue with other actions
            print(f"Warning: Failed to parse action instance: {e}")
            continue
    
    return UPFPlan(kind=kind, actions=actions)

def create_plan_msg_from_kb(plan_dict_list):
    """
    Parses a list of dictionaries into a list of auspex_msgs.msg.Plan messages.

    :param plan_dict_list: List of dictionaries representing the plan.
    :return: List of auspex_msgs.msg.Plan messages.
    """
    parsed_plans = []

    for plan_json in plan_dict_list:
        plan_dict = plan_json.get("data", {})
        platform_id = plan_dict["platform_id"]
        actions = []

        for action in plan_dict["actions"]:
            action_id = int(action["id"])
            action_name = action["name"]
            action_status = action["status"]
            action_args = action["args"]
            action_task_id = int(action["task_id"])

            # Create auspex_msgs.msg.Action
            action_instance = Action(
                name=action_name,
                status=action_status,
                args=action_args,
                task_id=action_task_id,
                id=action_id
            )
            actions.append(action_instance)

        tasks = []
        for task in plan_dict["tasks"]:
            task_id = int(task["id"])
            task_name = task["name"]
            task_status = task["status"]

            # Use the shared parse_atom_from_dict helper
            parameters = []
            for param in task["parameters"]:
                atom_msg = parse_atom_from_dict(param)
                parameters.append(atom_msg)

            # Create auspex_msgs.msg.Task
            task_instance = Task(
                id=task_id,
                name=task_name,
                parameters=parameters,
                status=task_status
            )
            tasks.append(task_instance)

        # Handle TPN field - convert dict to JSON string if needed
        tpn_value = plan_dict.get("tpn", "")
        if isinstance(tpn_value, dict):
            try:
                tpn_value = json.dumps(tpn_value)
            except Exception:
                tpn_value = ""
        elif not isinstance(tpn_value, str):
            tpn_value = ""

        # Parse upf_plan from dict or JSON string
        upf_plan_raw = plan_dict.get("upf_plan", None)
        if upf_plan_raw:
            upf_plan = parse_complete_upf_plan_from_dict(upf_plan_raw)
        else:
            upf_plan = UPFPlan(kind=0, actions=[])

        # Create auspex_msgs.msg.Plan
        plan_msg = Plan(
            platform_id=platform_id,
            plan_id=int(plan_dict.get("plan_id", "0")),
            team_id=plan_dict.get("team_id", ""),
            priority=int(plan_dict.get("priority", "0")),
            status=plan_dict.get("status", "FAILED"),
            tpn=tpn_value,
            actions=actions,
            tasks=tasks,
            upf_plan=upf_plan
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