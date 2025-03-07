from geometry_msgs.msg import PoseStamped, Pose2D
from auspex_msgs.action import ExecuteSequence
from msg_context.loader import ExecuteAtom
from auspex_executor.action_clients.custom_action_client import CustomActionClient
from rclpy.executors import SingleThreadedExecutor
from tf_transformations import quaternion_from_euler
import rclpy
from geographic_msgs.msg import GeoPoint

from geopy.distance import geodesic
from geopy.point import Point
import math
from geographic_msgs.msg import GeoPose

from auspex_msgs.msg import AltitudeLevel
from auspex_msgs.srv import (
    GetAltitude,
    GetHeighestPoint
) 


class SequenceActionClient(CustomActionClient):
    """
    Action client for sequences of multiple actions. 
    It can be used if the connection to ground control is not guaranteed to be stable and the drone should continue the plan without having to send every result back or having to receive new commands.
    It should use another type of action to contain multiple waypoints: ExecuteSequence.action

    :param Node node: the plan executor node
    :param Callable feedback_callback: function pointer for feedback callback function in plan executor
    :param Callable result_callback: function pointer for result callback function in plan executor
    :param string drone_prefix: drone identifier for the action client, so the correct respective server is called (for multiple drones)
    """
    def __init__(self, node, feedback_callback, result_callback, platform_id, operation_areas, altitude_server, hp_server):
        action_name = platform_id + "/action_sequence"
        super().__init__(node, feedback_callback, result_callback, action_name, ExecuteSequence)
        # manipulate lookup_table globally from plan_executor
        self._operation_areas = operation_areas
        self.action_name="ActionSequence"
        self.node = node
        self._altitude_getter = altitude_server
        self._hp_getter = hp_server

        self._last_planned_2Dwaypoint = GeoPose()
        self.sim_active = True
        
        self._db_executor = SingleThreadedExecutor() 

    def parseAction(self, single_action):
        """
        A really ugly way to do this
        ToDo: Make this better
        """
        execute_atom = ExecuteAtom()
        execute_atom.action_type = single_action.action_name
        actionName = single_action.action_name

        match actionName:
            case "take_off":
                
                if len(single_action.parameters) > 1:
                    execute_atom.goal_pose.position.altitude = float(single_action.parameters[1].real_atom[0].numerator)/ float(single_action.parameters[1].real_atom[0].denominator)
                    execute_atom.altitude_level.value = AltitudeLevel.REL    
                else:
                    execute_atom.goal_pose.position.altitude= 10.0
                    execute_atom.altitude_level.value = AltitudeLevel.REL    
                self._last_planned_2Dwaypoint = self._operation_areas['home']['centre']
                
            case "land":
                
                pass
            
            case "return_home_and_land":
                
                wp_pose = GeoPose()
                wp_resolved = self._operation_areas['home']['centre']
                wp_pose.position.latitude = float(wp_resolved[0])
                wp_pose.position.longitude = float(wp_resolved[1])
                wp_pose.position.altitude = 10
                execute_atom.goal_pose = wp_pose
                execute_atom.altitude_level.value = AltitudeLevel.AGL
                self._last_planned_2Dwaypoint = wp_pose

            case "ascend":
                
                wp_pose = GeoPose()
                wp_pose.position.altitude = float(single_action.parameters[2].real_atom[0].numerator)/ float(single_action.parameters[2].real_atom[0].denominator)
                if single_action.parameters[1].symbol_atom[0] == 'amsl':
                    execute_atom.altitude_level.value = AltitudeLevel.AMSL   
                elif single_action.parameters[1].symbol_atom[0] == 'agl':
                    execute_atom.altitude_level.value = AltitudeLevel.AGL   
                elif single_action.parameters[1].symbol_atom[0] == 'rel':
                    execute_atom.altitude_level.value = AltitudeLevel.REL 
                execute_atom.goal_pose = wp_pose  
                
            case "descend":
                
                wp_pose = GeoPose()
                wp_pose.position.altitude = float(single_action.parameters[2].real_atom[0].numerator)/ float(single_action.parameters[2].real_atom[0].denominator)
                if single_action.parameters[1].symbol_atom[0] == 'amsl':
                    execute_atom.altitude_level.value = AltitudeLevel.AMSL   
                elif single_action.parameters[1].symbol_atom[0] == 'agl':
                    execute_atom.altitude_level.value = AltitudeLevel.AGL   
                elif single_action.parameters[1].symbol_atom[0] == 'rel':
                    execute_atom.altitude_level.value = AltitudeLevel.REL 
                execute_atom.goal_pose = wp_pose  
                
            case "fly_2D":
                
                wp_pose = self.getGeoPoseForAction(index=1, action_atom=single_action)
                if not wp_pose:
                    return None
                execute_atom.goal_pose = wp_pose
                self._last_planned_2Dwaypoint = wp_pose
                
            case "fly_3D" | "fly" | "fly_step_3D":
                
                index = 1
                if actionName == 'fly':
                    index = 2
                    
                wp_pose = self.getGeoPoseForAction(index=index, action_atom=single_action)
                if not wp_pose:
                    return None
            
                height_amsl = self.getAltitudeFromDB(wp_pose)
                if not height_amsl:
                    return None
                        
                wp_pose.position.altitude = height_amsl
                execute_atom.goal_pose = wp_pose
                execute_atom.altitude_level.value = AltitudeLevel.AMSL
                self._last_planned_2Dwaypoint = wp_pose

            case "fly_at_ground_distance":
                wp_pose = GeoPose()
                
                wp_pose = self.getGeoPoseForAction(index=1, action_atom=single_action)
                if not wp_pose:
                    return None
                execute_atom.altitude_level.value = AltitudeLevel.AGL 
                execute_atom.goal_pose = wp_pose
                self._last_planned_2Dwaypoint = wp_pose 

            case "fly_above_highest_point":
                
                height_rel = self.get_real_value(single_action, 1)
                wp_pose = self.getGeoPoseForAction(index=2, action_atom=single_action)
                last_pose = self._last_planned_2Dwaypoint

                gps_lower_left, gps_upper_right = self.get_bounding_box(last_pose, wp_pose, 100) # 100 meter 
                height_amsl = self.getHeighestPointInBoundingBox(gps_lower_left, gps_upper_right)
                if not height_amsl:
                    return None

                wp_pose.position.altitude = height_amsl + height_rel
                execute_atom.goal_pose = wp_pose
                execute_atom.altitude_level.value = AltitudeLevel.AMSL 
                self._last_planned_2Dwaypoint = wp_pose

            case "turn":
                
                wp_pose = GeoPose()
                radians = self.get_radians(self.get_real_value(single_action, 1))
                q = quaternion_from_euler(0, 0, radians)
                wp_pose.orientation.x = q[0]
                wp_pose.orientation.y = q[1]
                wp_pose.orientation.z = q[2]
                wp_pose.orientation.w = q[3]
                execute_atom.goal_pose = wp_pose

            case "hover":
                
                execute_atom.hover_duration = self.get_real_value(single_action, 1)

            case "hover_left":

                wp_pose = self.getGeoPoseForAction(index=1, action_atom=single_action)
                execute_atom.goal_pose = wp_pose
                self._last_planned_2Dwaypoint = wp_pose
                
            case "hover_right":
                
                wp_pose = self.getGeoPoseForAction(index=1, action_atom=single_action)
                execute_atom.goal_pose = wp_pose
                self._last_planned_2Dwaypoint = wp_pose

            case "circle_around_poi":

                wp_pose = GeoPose()
                wp_pose = self.getGeoPoseForAction(index=2, action_atom=single_action)
                if single_action.parameters[1].symbol_atom[0] == 'amsl':
                    execute_atom.altitude_level.value = AltitudeLevel.AMSL   
                elif single_action.parameters[1].symbol_atom[0] == 'agl':
                    execute_atom.altitude_level.value = AltitudeLevel.AGL   
                elif single_action.parameters[1].symbol_atom[0] == 'rel':
                    execute_atom.altitude_level.value = AltitudeLevel.REL 
                execute_atom.goal_pose = wp_pose
                execute_atom.radius = 7.0#self.get_real_value(single_action, 3)
                execute_atom.speed = 5.0#self.get_real_value(single_action, 4) (31.4 meter/ (5 m/s))
                execute_atom.hover_duration = 20000.0 #multiple of 6 for whole circles

                self._last_planned_2Dwaypoint = wp_pose

            case "scanArea" | "searchArea":
                
                index = 3
                if actionName == "searchArea":
                    execute_atom.object_description = single_action.parameters[3].symbol_atom[0]
                    index = 4
                height_amsl = None
                wp_poses = self.getAreaForAction(index=index, action_atom=single_action)
                if not wp_poses:
                    return None
                level = single_action.parameters[1].symbol_atom[0]
                scan_height = self.get_real_value(single_action, 2)

                if level == 'ahp':
                    latitudes = [point.position.latitude for point in wp_poses]
                    longitudes = [point.position.longitude for point in wp_poses]
                    min_latitude = min(latitudes)
                    max_latitude = max(latitudes)
                    min_longitude = min(longitudes)
                    max_longitude = max(longitudes)
                    height_amsl = self.getHeighestPointInBoundingBox(gps_lower_left=self.getGeoPose(min_latitude, min_longitude), gps_upper_right=self.getGeoPose(max_latitude, max_longitude))
                    if not height_amsl:
                        return None

                elif level == 'agl':
                    average_latitude = sum(point.position.latitude for point in wp_poses) / len(wp_poses)
                    average_longitude = sum(point.position.longitude for point in wp_poses) / len(wp_poses)
                    height_amsl = self.getAltitudeFromDB(pose=self.getGeoPose(average_latitude, average_longitude))
                    if not height_amsl:
                        return None
                else:
                    return None

                wp_poses2d = []
                for wp_pose in wp_poses:
                    pose = Pose2D()
                    pose.x = wp_pose.position.latitude
                    pose.y = wp_pose.position.longitude
                    wp_poses2d.append(pose)

                execute_atom.scan_polygon_vertices = wp_poses2d
                execute_atom.height = height_amsl + scan_height 
                execute_atom.altitude_level = AltitudeLevel.AMSL 
                
                self._last_planned_2Dwaypoint = wp_poses[-1]

        return execute_atom

    def create_goalmsg(self, goal_msg):
        '''
        Parsing up_msgs to actions msgs defined in auspex_msgs
        '''
        execute_atoms_list = []

        for single_action in self._action:
            execute_atom = self.parseAction(single_action)
            if not execute_atom:
                return None
            execute_atoms_list.append(execute_atom)

        goal_msg.execute_atoms = execute_atoms_list
        return goal_msg

    def create_goalmsg_from_up(self, action_list):
        '''
        Parsing action_list to actions msgs defined in auspex_msgs
        Only used for addactions2action
        '''
        execute_atoms_list = []

        for single_action in action_list:
            execute_atom = self.parseAction(single_action)
            if not execute_atom:
                return None
            execute_atoms_list.append(execute_atom) 
            
        execute_atoms_list
        return execute_atoms_list
    
    def getAreaForAction(self, index, action_atom):
        wp_poses = []
        wp_pose = GeoPoint()
        wp_pose3 = GeoPoint()
        
        if len(action_atom.parameters[index].symbol_atom) > 0:
            wp = action_atom.parameters[index].symbol_atom[0]
            wp_pose, wp_pose3 = self.getGeoBoundsFromDB(wp)
            if not wp_pose or not wp_pose3:
                return None
        else: 
            wp_pose = self.getGeoPoseForAction(index, action_atom)
            if not wp_pose:
                return None
            wp_pose3 = self.getGeoPoseForAction(index+3, action_atom)
            if not wp_pose:
                return None
            
        wp_poses.append(wp_pose)
        wp_poses.append(self.getGeoPose(wp_pose.position.latitude, wp_pose3.position.longitude))
        wp_poses.append(self.getGeoPose(wp_pose3.position.latitude, wp_pose.position.longitude))
        wp_poses.append(wp_pose3)

        return wp_poses
        
    
    def getGeoPoseForAction(self, index, action_atom):
        wp_pose = GeoPose()
        if len(action_atom.parameters[index].symbol_atom) > 0:
            wp = action_atom.parameters[index].symbol_atom[0]
            wp_pose = self.getGeoPoseFromDB(wp)
            if not wp_pose:
                return None
        else: 
            wp_pose.position.latitude = self.get_real_value(action_atom, index)
            wp_pose.position.longitude = self.get_real_value(action_atom, index+1)
            if len(action_atom.parameters) > index+2 and len(action_atom.parameters[index+2].real_atom)>0:
                wp_pose.position.altitude = self.get_real_value(action_atom, index+2)
        return wp_pose
    
    def getGeoPose(self, lat, lon, alt=0.0):
        pose = GeoPose()
        pose.position.latitude = lat
        pose.position.longitude = lon
        pose.position.altitude = alt
        return pose    
    
    def getGeoPoseFromDB(self, area):
        if area not in self._operation_areas:
            return None
        wp_resolved = self._operation_areas[area]['centre']
        if len(wp_resolved)<=2:
            wp_resolved.append(0.0)
        elif area == 'home':
            wp_resolved[2] = 5.0
        return self.getGeoPose(float(wp_resolved[0]), float(wp_resolved[1]), float(wp_resolved[2]))
    
    def getGeoBoundsFromDB(self, area):
        wp_pose = GeoPose()
        if area not in self._operation_areas:
            return None
        if 'lower_bounds' not in self._operation_areas[area]:
            return None
        wp_lb = self._operation_areas[area]['lower_bounds']
        wp_ub = self._operation_areas[area]['upper_bounds']
        if len(wp_ub)<=2:
            wp_ub.append(0.0)
        if len(wp_lb)<=2:
            wp_lb.append(0.0)
        return [self.getGeoPose(float(wp_lb[0]), float(wp_lb[1]), float(wp_lb[2])), self.getGeoPose(float(wp_ub[0]), float(wp_ub[1]), float(wp_ub[2]))] 
    
    def getAltitudeFromDB(self, pose):
        srv = GetAltitude.Request()
        srv.gps_position.latitude = pose.position.latitude
        srv.gps_position.longitude = pose.position.longitude
        srv.resolution = 1
        
        if not self._altitude_getter.wait_for_service(timeout_sec=10.0):
            print("Service not available. Setting default value.")
            return None
        else:
            future = self._altitude_getter.call_async(srv)  
            rclpy.spin_until_future_complete(self.node, future, self._db_executor)
            if future.result().success == True:
                height_amsl = pose.position.altitude + float(future.result().altitude_amsl)
                return height_amsl
            return None
        
    def getHeighestPointInBoundingBox(self, gps_lower_left, gps_upper_right):
        srv = GetHeighestPoint.Request()
        srv.region_bb = [gps_lower_left, gps_upper_right]
        srv.resolution = 10
        srv.tile_size = 500

        self._hp_getter.wait_for_service()
        future = self._hp_getter.call_async(srv)     
        rclpy.spin_until_future_complete(self.node, future, self._db_executor)

        if future.result().success == True:
            height_amsl = float(future.result().altitude_amsl)
            return height_amsl
        return None

    def get_bounding_box(self, wp1, wp2, buffer_dist=100):
        """Calculate a bounding box around two waypoints with a buffer distance."""
        # Extract latitude and longitude
        lat1, lon1 = wp1.position.latitude, wp1.position.longitude
        lat2, lon2 = wp2.position.latitude, wp2.position.longitude

        # Calculate center point
        center_point = Point((lat1 + lat2) / 2, (lon1 + lon2) / 2)

        # Get bounding box corners
        def offset_point(point, distance, bearing):
            return geodesic(meters=distance).destination(point=point, bearing=bearing)

        sw_point = offset_point(center_point, buffer_dist, 225)  # South-West
        sw_point = offset_point(sw_point, buffer_dist, 270)  

        ne_point = offset_point(center_point, buffer_dist, 45)   # North-East
        ne_point = offset_point(ne_point, buffer_dist, 90)   

        # Create bounding box points
        return self.getGeoPose(sw_point.latitude, sw_point.longitude), self.getGeoPose(ne_point.latitude, ne_point.longitude)

    def get_radians(self, degree):
        return degree * math.pi / 180.0

    def get_degree(self, radians):
        return radians * 180.0/ math.pi

    def get_real_value(self, single_action, pos):
        return float(single_action.parameters[pos].real_atom[0].numerator)/ float(single_action.parameters[pos].real_atom[0].denominator)

'-------alt is always above ground'
'''
['take_off', 'land', 'fly_home_and_land', ['ascend'], ['descend'], ['hover'], ['move2GPS']]
['take_off', height_in_metres] # takeoff to height in metres above ground 
['land'] # Land at current position
['return_home_and_land'] # flies to home position, lands and terminates the program
['ascend', 'amsl|agl|rel', height_in_metres] # ascend either above mean sea level (amsl), above ground level (agl), relative to current height (rel)
['descend', 'amsl|agl|rel', height_in_metres] # descend either above mean sea level (amsl), above ground level (agl), relative to current height (rel)
['hover', duration] # hover for duration in milliseconds (10000ms -> 10 seconds)

['fly_2D', lat, lon] # flies in current height to next lat lon coordinates
['fly_2D', 'wp'] # flies in current height to next waypoint from knowledgebase
['fly_3D', lat, lon, alt] # fly to new 3D lat, lon, alt coordinate
['fly_3D', 'wp'] # fly to now 3D waypoint from knowledgebase
['fly_step_3D', 'wp'] # fly 3D in descend/ascend steps
['fly_step_3D', lat, lon, alt]
['fly_above_highest_point', height_in_metres, 'wp'] # flies to waypoint in height above heighest point in the bounding box
['fly_above_highest_point', height_in_metres, lat, lon] # flies to waypoint in height above heighest point in the bounding box 
['start_detection'] # start the object detection. Is already included in searchArea
['stop_detection'] # stops the object detection. Is already included in searchArea
['capture_image'] # takes an image and sends it to the topic 
['turn', degree] # turn rel to current orientation (+ is clockwise)
['hover_left', 'wp'] # flies with a +30. degree orientation to the next waypoint
['hover_left', lat,lon] # flies with a +30. degree orientation to the next lat lon coordinates
['hover_right', 'wp'] # flies with a -30. degree orientation to the next waypoint
['hover_right', lat, lon] # flies with a -30. degree orientation to the next lat lon coordinates
['circle_around_poi', 'amsl|agl|rel', 'wp'] # circles a given waypoint in a given altitude for one full circle
['circle_around_poi', 'amsl|agl|rel', lat, lon, alt] # circles a given waypoint in a given altitude for one full circle 
['scanArea', 'amsl|agl|ahp', height_in_metres, 'wp']# Scans an area for photogrametry
['searchArea', 'amsl|agl|ahp',height_in_metres, 'object description', lat1, lon1, alt1, lat2, lon2, alt2] # searches an area for object and stops if found
['searchArea', 'amsl|agl|ahp',height_in_metres, 'object description', 'operation_area'] # Search operation area in given height until done or object found.
'''

'''
--['Track_Object']
--[fly_at_ground_distance, 'wp'] -> continuous check
--[fly_at_ground_distance, lat, lon, alt]
--['communicate', 'ros/...']
--['follow_object', 'object_class_for_detection']
--['get_data_from_knowledgebase', 'data']
'''
