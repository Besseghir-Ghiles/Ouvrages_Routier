import geopandas as gpd
import rasterio
from shapely.geometry import MultiLineString, LineString, Point, box
import math
import os
from sklearn.linear_model import LinearRegression
import numpy as np
import logging
import matplotlib.pyplot as plt
from get_data_functions import get_data, get_mnt

class ProfileAnalyzer:
    """
    Class to analyze profiles along a route and classify them as remblai, deblai or rasant
    """
    def __init__(self, mnt_path, output_folder, classification_threshold_remblai, classification_threshold_deblai, route_number):
        self.mnt_path = mnt_path
        self.dem, self.transform, self.boundingbox = self._read_dem()
        self.output_folder = output_folder
        self.classification_threshold_remblai = classification_threshold_remblai
        self.classification_threshold_deblai = classification_threshold_deblai
        self.route_number = route_number
        self.filter_route = f"cpx_numero='{route_number}'"
        self.lines_selected = get_data(self.filter_route, "BDTOPO_V3:troncon_de_route", self.boundingbox)
        self.lines_selected = self.lines_selected[self.lines_selected['nature'] == 'Type autoroutier']
        
        # Save lines_selected to check its contents
        os.makedirs(self.output_folder, exist_ok=True)
        output_file = os.path.join(self.output_folder, "lines_selected.gpkg")
        self.lines_selected.to_file(output_file, driver='GPKG')
        print(f"Saved lines_selected to: {output_file}")
        
        # Setup logging
        os.makedirs(self.output_folder, exist_ok=True)
        log_file = os.path.join(self.output_folder, "profile_analysis.log")
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        self.r2_scores = []  # Add this line to store R² scores

    def _read_dem(self):
        """Read the DEM file and return the elevation data and transform"""
        with rasterio.open(self.mnt_path) as src:
            print(f"DEM bounds: {src.bounds}")
            print(f"DEM shape: {src.shape}")
            print(f"DEM resolution: {src.res}")
            return src.read(1), src.transform, src.bounds

    def get_raster_value(self, point):
        """Get the elevation value from the raster at a given point"""
        try:
            row, col = rasterio.transform.rowcol(self.transform, point.x, point.y)
            if 0 <= row < self.dem.shape[0] and 0 <= col < self.dem.shape[1]:
                return self.dem[row, col]
            else:
                print(f"Point outside raster bounds: row={row}, col={col}")
        except IndexError as e:
            print(f"IndexError: {e}")
        except Exception as e:
            print(f"Other error: {e}")
        return None
    
    def calculate_angle(self, point1, point2):
        """Calculate the angle between two points"""
        dx = point2[0] - point1[0]
        dy = point2[1] - point1[1]
        angle_rad = math.atan2(dy, dx)
        angle_deg = math.degrees(angle_rad)
        return angle_deg

    def calculate_distance(self, point1, point2):
        """Calculate the distance between two points"""
        return math.sqrt((point2.x - point1.x)**2 + (point2.y - point1.y)**2)
    
    def calculate_slope(self, point1, point2):
        Z1 = self.get_raster_value(point1)
        Z2 = self.get_raster_value(point2)
        if Z1 is None or Z2 is None:
            return None
        deltaZ = Z2 - Z1

        dist = self.calculate_distance(point1, point2)
        if dist is None:
            return None
        slope = deltaZ/dist

        return slope
    
    def calculate_perpendicular_line(self, current_distance, line):
        """Calculate the perpendicular line at a given distance along the route"""
        current_point = line.interpolate(current_distance)

        # Calculate angle
        if current_distance <= 15:
            next_point = line.interpolate(current_distance + 10)
            angle = self.calculate_angle((current_point.x, current_point.y), (next_point.x, next_point.y))
        else:
            prev_point = line.interpolate(current_distance - 10)
            angle = self.calculate_angle((prev_point.x, prev_point.y), (current_point.x, current_point.y))
        
        # Calculate perpendicular line endpoints
        dx = 60 * math.cos(math.radians(angle + 90))
        dy = 60 * math.sin(math.radians(angle + 90))
        start_point = (current_point.x - dx, current_point.y - dy)
        end_point = (current_point.x + dx, current_point.y + dy)

        # Calculate perpendicular line of length 160 m
        perpendicular_line = LineString([start_point, end_point])

        return perpendicular_line

    def calculate_average_height(self, perpendicular_line, startpoint, endpoint):
        """Calculate the average height between 2 points on the perpendicular line"""
        # Create intermediate points
        intermediate_points = []
        i = startpoint
        
        print(f"\nCalculating average height:")
        print(f"Perpendicular line length: {perpendicular_line.length}")
        
        while i <= endpoint:
            intermediate_point = perpendicular_line.interpolate(i)
            intermediate_points.append(intermediate_point)
            i += 1
        
        sum_elevations = 0
        valid_points = 0

        for point in intermediate_points:
            elevation = self.get_raster_value(point)
            print(f"Point coordinates: ({point.x}, {point.y}), Elevation: {elevation}")
            if elevation is not None:
                sum_elevations += elevation
                valid_points += 1

        print(f"Valid points found: {valid_points}")
        
        if valid_points == 0:
            print("No valid points found!")
            return None

        average_height = sum_elevations / valid_points
        return average_height
    
    def calculate_minmax_height(self, perpendicular_line, startpoint, endpoint):
        """Calculate the minimum and maximum height along the perpendicular line"""
        intermediate_points = []
        i = startpoint
        while i <= endpoint:
            intermediate_point = perpendicular_line.interpolate(i)
            intermediate_points.append(intermediate_point)
            i += 1

        max_height = 0
        min_height = 1000
        valid_points = 0

        for point in intermediate_points:
            elevation = self.get_raster_value(point)
            print(f"Point coordinates: ({point.x}, {point.y}), Elevation: {elevation}")
            if elevation is not None:
                if elevation > max_height:
                    max_height = elevation
                if elevation < min_height:
                    min_height = elevation
                valid_points += 1

        print(f"Valid points found: {valid_points}")
        
        if valid_points == 0:
            print("No valid points found!")
            return None, None

        return max_height, min_height

    def calculate_height_difference(self, height1, height2):
        """Calculates the height difference between the level of the route and the terrain on the right"""
        if height1 is None or height2 is None:
            return None
        height_difference = height1 - height2
        return height_difference
    
    def calculate_natural_slope(self, perpendicular_line, startpoint1, endpoint1, startpoint2, endpoint2):
        """Determines a linear regression fonction describing the altitude and slope of the natural terrain"""
        intermediate_points = []
        distance = []
        altitude = []

        i = startpoint1
        j = startpoint2
        startpoint_line = perpendicular_line.interpolate(0)

        while i <= endpoint1:
            intermediate_point = perpendicular_line.interpolate(i)
            intermediate_points.append(intermediate_point)
            i += 1
        while j <= endpoint2:
            intermediate_point = perpendicular_line.interpolate(j)
            intermediate_points.append(intermediate_point)
            j += 1

        for point in intermediate_points:
            dist = self.calculate_distance(startpoint_line, point)
            alt = self.get_raster_value(point)
            if alt is not None:
                distance.append(dist)
                altitude.append(alt)

        if not distance or not altitude:
            print("No valid elevation data found for natural slope calculation")
            return None

        dist_arr = np.array(distance).reshape(-1, 1)
        alt_arr = np.array(altitude).reshape(-1, 1)

        try:
            reg = LinearRegression().fit(dist_arr, alt_arr)
            r2_score = reg.score(dist_arr, alt_arr)
            self.logger.info(f"R² score: {r2_score}")
            
            # Store R² score with distance information
            current_distance = perpendicular_line.interpolate(0).distance(self.lines_selected.iloc[0].geometry)
            self.r2_scores.append({
                'distance': current_distance,
                'r2_score': r2_score,
                'coefficients': reg.coef_[0][0],
                'intercept': reg.intercept_[0]
            })
            
            return reg, reg.coef_[0][0]
        except Exception as e:
            print(f"Error in linear regression: {e}")
            return None

    def calculate_interpolated_altitude(self, distance, reg):
        """Calculate the interpolated altitude using the regression model"""
        if reg is None:
            return None

        distance_reshaped = np.array([distance]).reshape(-1,1)    
        altitude = reg.predict(distance_reshaped)
        return altitude[0][0]

    def calculate_attributes_deblai(self, perpendicular_line, reg, coef):
        """Calculate attributes for deblai profile"""
        # Find the minimum to determine starting point
        alt_min = 1000
        dist_min = 60
        calculation_points = []  # Store points used for calculation

        for i in range(60, 45, -1):
            point = perpendicular_line.interpolate(i)
            altitude = self.get_raster_value(point)
            if altitude is None:
                continue
            if altitude < alt_min:
                alt_min = altitude
                dist_min = i

        j = dist_min
        natural_slope = coef

        self.logger.info(f"\nNatural slope: {natural_slope}")

        # Calculate initial interpolated altitude
        #interpolated_natural_altitude = self.calculate_interpolated_altitude(j, reg)

        # Initialize variables to track intersection
        prev_difference = None

        # Iterate over points to find the end of the ouvrage
        while j > 30:
            # Calculate current slope
            point1 = perpendicular_line.interpolate(j+0.5)
            point2 = perpendicular_line.interpolate(j-0.5)
            current_slope = self.calculate_slope(point1, point2)

            # Get real altitude current point
            point = perpendicular_line.interpolate(j)
            current_altitude = self.get_raster_value(point)

            # Calculate interpolated altitude at current distance
            interpolated_altitude = self.calculate_interpolated_altitude(j, reg)

            # Calculate difference between actual and interpolated altitude
            if current_altitude is None or interpolated_altitude is None:
                j -= 0.5
                self.logger.warning(f"No elevation data at distance {j}, skipping")
                continue
            current_difference = current_altitude - interpolated_altitude

            # Store points and their data for visualization
            calculation_points.append({
                'point': point,
                'elevation': current_altitude,
                'slope': current_slope,
                'distance': j
            })

            self.logger.info(f"\nAt distance {j}:")
            self.logger.info(f"Point 1 elevation: {current_altitude}")
            self.logger.info(f"Point 2 elevation: {self.get_raster_value(point2)}")
            self.logger.info(f"Calculated slope: {current_slope}")
            self.logger.info(f"Natural slope range: {natural_slope - 0.05} to {natural_slope + 0.05}")

            # Check for intersection (sign change)
            if prev_difference is not None:
                if (prev_difference * current_difference <= 0):  # Sign change occurred
                    self.logger.info(f"Intersection found at distance {j}")
                    break

            prev_difference = current_difference
            j -= 0.5
        
        # Calculate properties of ouvrage
        alt_max = current_altitude
        dist_max = j
        distance = abs(dist_min - j)
        height_difference = None
        slope_ouvrage_total = None
        if alt_max is not None and alt_min is not None:
            height_difference = alt_max - alt_min
            slope_ouvrage_total = height_difference / distance

        # Slopes at the top and bottom of an ouvrage are not relaible, so we calculate the slope based on the middle section, if possible
        slope_ouvrage_section = None
        safety_margin = 1.5
        if distance > (safety_margin * 2):
            point_min = perpendicular_line.interpolate(dist_min+2)
            point_max = perpendicular_line.interpolate(dist_max-2)
            slope_ouvrage_section = self.calculate_slope(point_min, point_max)

        # Slopes of the middle section are more reliable
        slope_ouvrage_middle = None
        section_length = 3
        if distance > section_length:
            point_middle_min = perpendicular_line.interpolate(dist_min + (distance / 2) - (section_length / 2))
            point_middle_max = perpendicular_line.interpolate(dist_min + (distance / 2) + (section_length / 2))
            slope_ouvrage_middle = self.calculate_slope(point_middle_min, point_middle_max)

        self.logger.info(f"\nFound significant slope change:")
        self.logger.info(f"Final slope: {slope_ouvrage_total}")
        self.logger.info(f"Height difference: {height_difference}")

        return slope_ouvrage_total, slope_ouvrage_section, slope_ouvrage_middle, height_difference, calculation_points

    def calculate_attributes_remblai(self, perpendicular_line, reg, coef):
        """Calculate attributes for remblai profile"""
        # Find the maximum to determine starting point
        alt_max = 0
        i = 60
        slope = 0
        calculation_points = []

        self.logger.info("Starting calculate_attributes_remblai")
        
        # Find initial slope
        while slope < 0.08 and i > 30:
            point1 = perpendicular_line.interpolate(i+1)
            point2 = perpendicular_line.interpolate(i-0.5)
            slope = abs(self.calculate_slope(point1, point2))
            i -= 0.5
            
        alt_max = self.get_raster_value(point1)
        dist_max = i

        self.logger.info(f"Found starting point: dist_max={dist_max}, alt_max={alt_max}")

        j = dist_max
        natural_slope = coef
        max_iterations = 60
        iteration_count = 0

        self.logger.info(f"Natural slope: {natural_slope}")
        prev_difference = None
        current_altitude = self.get_raster_value(perpendicular_line.interpolate(i))

        while j > 30 and iteration_count < max_iterations:
            iteration_count += 1
            
            # Calculate current slope
            point1 = perpendicular_line.interpolate(j+0.5)
            point2 = perpendicular_line.interpolate(j-0.5)
            current_slope = self.calculate_slope(point1, point2)

            # Get real altitude current point
            point = perpendicular_line.interpolate(j)
            current_altitude = self.get_raster_value(point)
            
            if current_altitude is None:
                self.logger.warning(f"No elevation data at distance {j}")
                j -= 0.5
                continue

            # Calculate interpolated altitude
            interpolated_altitude = self.calculate_interpolated_altitude(j, reg)
            if interpolated_altitude is None:
                self.logger.warning(f"Could not interpolate altitude at distance {j}")
                j -= 0.5
                continue

            # Calculate difference
            if current_altitude is None or interpolated_altitude is None:
                j -= 0.5
                self.logger.warning(f"No elevation data at distance {j}, skipping")
                continue
            current_difference = current_altitude - interpolated_altitude

            # Store points
            calculation_points.append({
                'point': point,
                'elevation': current_altitude,
                'slope': current_slope,
                'distance': j
            })

            # Fixed logging statement with proper None handling
            current_diff_str = f"{current_difference:.2f}" if current_difference is not None else "None"
            prev_diff_str = f"{prev_difference:.2f}" if prev_difference is not None else "None"
            self.logger.info(f"Distance {j}: current_diff={current_diff_str}, prev_diff={prev_diff_str}")

            # Check for intersection
            if prev_difference is not None and current_difference is not None:
                if (prev_difference * current_difference <= 0):
                    self.logger.info(f"Intersection found at distance {j}")
                    break

            prev_difference = current_difference
            j -= 0.5

        # Check if we hit the iteration limit
        if iteration_count >= max_iterations:
            self.logger.warning("Max iterations reached without finding intersection")
            return None, None, None, None, calculation_points

        # Calculate final attributes
        alt_min = current_altitude
        dist_min = j + 0.5
        distance = abs(dist_max - dist_min)

        if distance == 0:
            self.logger.warning("Zero distance found, cannot calculate slope")
            return None, None, None, None, calculation_points

        height_difference = None
        if alt_max is not None and alt_min is not None:
            height_difference = alt_max - alt_min

        if height_difference > 50:
            height_difference = None
        
        slope_ouvrage_total = None
        if height_difference is not None and distance is not None:
            slope_ouvrage_total = height_difference / distance

        # Slopes at the top and bottom of an ouvrage are not relaible, so we calculate the slope based on the middle section, if possible
        slope_ouvrage_section = None
        safety_margin = 1.5
        if distance > (safety_margin * 2):
            point_min = perpendicular_line.interpolate(dist_min+2)
            point_max = perpendicular_line.interpolate(dist_max-2)
            slope_ouvrage_section = self.calculate_slope(point_min, point_max)

        # Slopes of the middle section are more reliable
        slope_ouvrage_middle = None
        section_length = 3
        if distance > section_length:
            point_middle_min = perpendicular_line.interpolate(dist_min + (distance / 2) - (section_length / 2))
            point_middle_max = perpendicular_line.interpolate(dist_min + (distance / 2) + (section_length / 2))
            slope_ouvrage_middle = self.calculate_slope(point_middle_min, point_middle_max)

        if height_difference is not None and slope_ouvrage_total is not None:
            self.logger.info(f"Final calculations: height_diff={height_difference:.2f}, slope={slope_ouvrage_total:.2f}")

        return slope_ouvrage_total, slope_ouvrage_section, slope_ouvrage_middle, height_difference, calculation_points

    def classify_point(self, height_difference):
        """Classify point as zone de remblai, zone de deblai ou en profil rasant"""
        if height_difference is None:
            return "unknown"

        if height_difference >= self.classification_threshold_remblai:
            return "remblai"
        elif height_difference <= self.classification_threshold_deblai:
            return "deblai"
        else:
            return "rasant"
    
    def determine_routewidth(self, line_index):
        """Determine the route width and other parameters based on the number of lanes"""
        ref_route_start = 57
        ref_route_end = 63
        '''
        ref_terrain_start = 20
        ref_terrain_end = 30
        ref_minmax_start = 20
        ref_minmax_end = 40
        ref_slope_start = 50
        ref_slope_end = 30
        ref_terrain_start1 = 0
        ref_terrain_end1 = 30
        ref_terrain_start2 = 90
        ref_terrain_end2 = 120'''
        if self.lines_selected.iloc[line_index]['nombre_de_voies'] == 2:
            ref_terrain_start = 20
            ref_terrain_end = 30
            ref_minmax_start = 20
            ref_minmax_end = 40
            ref_slope_start = 50
            ref_slope_end = 30
            ref_terrain_start1 = 0
            ref_terrain_end1 = 30
            ref_terrain_start2 = 90
            ref_terrain_end2 = 120
        else:
            ref_terrain_start = 15
            ref_terrain_end = 25
            ref_minmax_start = 25
            ref_minmax_end = 45
            ref_slope_start = 45
            ref_slope_end = 25
            ref_terrain_start1 = 0
            ref_terrain_end1 = 25
            ref_terrain_start2 = 95
            ref_terrain_end2 = 120
        
        return ref_route_start, ref_route_end, ref_terrain_start, ref_terrain_end, ref_minmax_start, ref_minmax_end, ref_slope_start, ref_slope_end, ref_terrain_start1, ref_terrain_end1, ref_terrain_start2, ref_terrain_end2

    def visualize_profile(self, i, perpendicular_line, reg, coef, current_distance, output_folder):
        """Visualize the profile and regression line at a specific distance."""
        intermediate_points = []
        distances = []
        elevations = []

        # Generate intermediate points along the perpendicular line
        for i in range(0, int(perpendicular_line.length) + 1):
            point = perpendicular_line.interpolate(i)
            elevation = self.get_raster_value(point)
            if elevation is not None:
                intermediate_points.append(point)
                distances.append(i)
                elevations.append(elevation)

        # Plot the profile
        plt.figure(figsize=(10, 6))
        plt.plot(distances, elevations, label="Terrain Profile", marker="o", linestyle="-")

        # Add regression line if available
        if reg is not None:
            regression_distances = np.array(distances).reshape(-1, 1)
            regression_elevations = reg.predict(regression_distances)
            plt.plot(distances, regression_elevations, label="Regression Line", color="red", linestyle="--")

        middle_value = (max(elevations) + min(elevations)) / 2

        y_min = middle_value - 10
        y_max = middle_value + 10

        # Add labels and legend
        plt.title(f"Profile Visualization at Distance {current_distance} m")
        plt.xlabel("Distance along Perpendicular Line (m)")
        plt.ylabel("Elevation (m)")
        plt.legend()
        plt.grid(True)
        plt.ylim(y_min, y_max)
        plt.tight_layout()

        # Save the plot as a PNG file
        output_file = os.path.join(output_folder, f"profile{self.route_number}_{i}_{int(current_distance)}m.png")
        plt.savefig(output_file)
        plt.close()

        self.logger.info(f"Profile visualization saved: {output_file}")

    def analyze_profile(self):
        """Analyze the profile and classify it"""
        self.logger.info("Starting profile analysis")
        self.logger.info(f"Number of selected lines: {len(self.lines_selected)}")
        all_segments = []  # List to store all segments
        all_calculation_points = []  # Store all calculation points for visualization

        for i in range(len(self.lines_selected)):
            self.logger.info(f"\nProcessing line {i+1}/{len(self.lines_selected)}")
            geometry = self.lines_selected.iloc[i].geometry
        
            # Handle both LineString and MultiLineString
            if isinstance(geometry, MultiLineString):
                line = geometry.geoms[0]
            elif isinstance(geometry, LineString):  # Assume it's a LineString
                line = geometry
            else:
                self.logger.warning(f"Unsupported geometry type: {type(geometry)}")
                continue

            length = line.length
            ref_route_start, ref_route_end, ref_terrain_start, ref_terrain_end, ref_minmax_start, ref_minmax_end, ref_slope_start, ref_slope_end, ref_terrain_start1, ref_terrain_end1, ref_terrain_start2, ref_terrain_end2 = self.determine_routewidth(i)
            
            current_distance = 0
            points = []

            while current_distance <= length:
                current_point = line.interpolate(current_distance)
                next_point = line.interpolate(current_distance + 1)

                perpendicular_line = self.calculate_perpendicular_line(current_distance, line)
                average_height_route = self.calculate_average_height(perpendicular_line, ref_route_start, ref_route_end)
                #average_height_terrain = self.calculate_average_height(perpendicular_line, ref_terrain_start, ref_terrain_end)
                #max_height_terrain, min_height_terrain = self.calculate_minmax_height(perpendicular_line, ref_minmax_start, ref_minmax_end)
                reg, coef = self.calculate_natural_slope(perpendicular_line, ref_terrain_start1, ref_terrain_end1, ref_terrain_start2, ref_terrain_end2)
                interpolated_height_nat_terrain_route = self.calculate_interpolated_altitude(60, reg)
                height_difference_nat_terrain = average_height_route - interpolated_height_nat_terrain_route

                profile_type = self.classify_point(height_difference_nat_terrain)
                self.logger.info(f"\nAt distance {current_distance}:")
                self.logger.info(f"Profile type: {profile_type}")
                self.logger.info(f"Height difference: {height_difference_nat_terrain}")

                # Initialize max_height_difference as None
                max_height_difference = None
                slope_ouvrage_total = None
                slope_ouvrage_section = None
                calculation_points = None

                if profile_type == "deblai":
                    slope_ouvrage_total, slope_ouvrage_section, slope_ouvrage_middle, max_height_difference, calculation_points = self.calculate_attributes_deblai(perpendicular_line, reg, coef)
                elif profile_type == "remblai":
                    slope_ouvrage_total, slope_ouvrage_section, slope_ouvrage_middle, max_height_difference, calculation_points = self.calculate_attributes_remblai(perpendicular_line, reg, coef)
                
                if calculation_points:
                    all_calculation_points.extend(calculation_points)
                    
                point = Point(current_point)
                points.append({
                    'geometry': point,
                    'classification': profile_type,
                    'height_difference_nat_terrain': height_difference_nat_terrain,
                    'average_height_route': average_height_route,
                    'interpolated_height_nat_terrain_route': interpolated_height_nat_terrain_route,
                    #'average_height_terrain': average_height_terrain,
                    'num_voies': self.lines_selected.iloc[i]['nombre_de_voies'],
                    #'distance': current_distance,
                    'largeur_route': self.lines_selected.iloc[i]['largeur_de_chaussee'],
                    'num_route': self.lines_selected.iloc[i]['cpx_numero'],
                    'max_height_difference': max_height_difference,
                    'slope_ouvrage_total': slope_ouvrage_total,
                    'slope_ouvrage_section': slope_ouvrage_section,
                    'slope_ouvrage_middle': slope_ouvrage_middle
                })

                # Visualize the profile every 100 meters
                """if int(current_distance) % 100 == 0:
                    self.visualize_profile(i, perpendicular_line, reg, coef, current_distance, self.output_folder)
                """

                current_distance += 1

            all_segments.extend(points)

        # Create GeoDataFrames for visualization
        points_gdf = gpd.GeoDataFrame(all_segments, crs=self.lines_selected.crs)
        
        # Create a GeoDataFrame for calculation points
        if all_calculation_points:
            calculation_points_data = []
            for point_data in all_calculation_points:
                calculation_points_data.append({
                    'geometry': point_data['point'],
                    'elevation': point_data['elevation'],
                    'slope': point_data['slope'],
                    'distance': point_data['distance']
                })
            calculation_points_gdf = gpd.GeoDataFrame(calculation_points_data, crs=self.lines_selected.crs)
        else:
            calculation_points_gdf = None

        self.logger.info("\nAnalysis completed successfully")
        return points_gdf, calculation_points_gdf

    def save_output(self, points_gdf, calculation_points_gdf):
        """Save the classified profiles, calculation points, and R² scores"""
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Save R² scores to CSV
        r2_output_file = os.path.join(self.output_folder, f"r2_scores_{self.route_number}.csv")
        import pandas as pd
        r2_df = pd.DataFrame(self.r2_scores)
        r2_df.to_csv(r2_output_file, index=False)
        print(f"R² scores saved to: {r2_output_file}")
        
        # Save segments
        output_file = os.path.join(self.output_folder, "classified_profiles.gpkg")
        points_gdf.to_file(output_file, driver='GPKG', layer='points')
        
        # Save calculation points if they exist
        if calculation_points_gdf is not None:
            calculation_points_gdf.to_file(output_file, driver='GPKG', layer='calculation_points')
        
        print(f"Classified profiles saved as: {output_file}")
        print("Layers created: 'points' and 'calculation_points'")
