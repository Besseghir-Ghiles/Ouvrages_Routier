import geopandas as gpd
import rasterio
import shapely
from shapely.geometry import MultiLineString, LineString, Point
from shapely.ops import linemerge, unary_union
from centerline.geometry import Centerline
import pygeoops
from get_data_functions import get_data, get_mnt
from tqdm import tqdm
import os
import math
import matplotlib.pyplot as plt

def connect_segments(route, buffer_distance=5):
    """
    Connect all LineString segments that are within buffer_distance meters of each other
    Args:
        route: GeoDataFrame with LineString geometries
        buffer_distance: Maximum distance in meters between segments to connect (default 5 m)
    """
    segments = route.copy()  # tous les morceaux de la route 
    connected_chains = []  # groupe de segments connectés
    
    # Keep processing while there are segments left
    with tqdm(total=len(segments), desc="Processing segments") as pbar:
        while not segments.empty: # tq reste des segments , on construit de nvl chaine
            # Get the first segment as the start of a new chain
            first_segment = segments.iloc[0]
            current_geom = first_segment.geometry
            chain = [first_segment]
            segments = segments.drop(segments.index[0])
            
            # Keep adding segments to the chain until no more connections are found
            chain_modified = True
            while chain_modified and not segments.empty:
                chain_modified = False
                
                # Create buffer around current chain end points
                start_point = Point(current_geom.coords[0])
                end_point = Point(current_geom.coords[-1])
                start_buffer = start_point.buffer(buffer_distance)
                end_buffer = end_point.buffer(buffer_distance)
                
                # Check each remaining segment
                for idx in segments.index:
                    segment = segments.loc[idx]
                    seg_start = Point(segment.geometry.coords[0])
                    seg_end = Point(segment.geometry.coords[-1])
                    
                    # Check if segment endpoints are within buffer distance
                    if end_buffer.intersects(seg_start):
                        chain.append(segment)
                        segments = segments.drop(idx)
                        current_geom = segment.geometry
                        chain_modified = True
                        pbar.update(1)
                        break
                    elif start_buffer.intersects(seg_end):
                        chain.append(segment)
                        segments = segments.drop(idx)
                        current_geom = segment.geometry
                        chain_modified = True
                        pbar.update(1)
                        break
            
            # Add the completed chain to our results
            connected_chains.append(chain)
            pbar.update(1)
    
    # Convert chains to LineStrings
    merged_lines = []
    for chain in connected_chains:
        coords = []
        for segment in chain:
            coords.extend(list(segment.geometry.coords))
        merged_lines.append(LineString(coords))
    ######  
    # Buffer and merge the linestrings
    print("\nCreating buffer and centerline...")
    buffered_lines = [line.buffer(250) for line in merged_lines]
    merged_polygon = unary_union(buffered_lines)

    #buffered_gdf = gpd.GeoDataFrame({'geometry': buffered_lines}, crs=route.crs)
    #buffered_gdf.to_file("buffered_lines.gpkg", driver="GPKG")
    #print("buffered_lines saved to buffered_lines.gpkg")

    #merged_gdf = gpd.GeoDataFrame({'geometry': [merged_polygon]}, crs=route.crs)
    #merged_gdf.to_file("merged_polygon.gpkg", driver="GPKG")
    #print("merged_polygon saved to merged_polygon.gpkg")
    
    #centerline = Centerline(merged_polygon)
    centerline = pygeoops.centerline(merged_polygon, simplifytolerance=0)

    #centerline_gdf = gpd.GeoDataFrame({'geometry': [centerline]}, crs=route.crs)
    #centerline_gdf.to_file("centerline_gdf.gpkg", driver="GPKG")
    #print("centerline_gdf saved to centerline_gdf.gpkg")
        
    return centerline

def calculate_angle(point1, point2):
    """Calculate the angle between two points"""
    dx = point2[0] - point1[0]
    dy = point2[1] - point1[1]
    angle_rad = math.atan2(dy, dx)
    angle_deg = math.degrees(angle_rad)
    return angle_deg

def calculate_perpendicular_line(current_distance, line):
        """Calculate the perpendicular line at a given distance along the route"""
        current_point = line.interpolate(current_distance)

        # Calculate angle
        if current_distance <= 15:
            next_point = line.interpolate(current_distance + 10)
            angle = calculate_angle((current_point.x, current_point.y), (next_point.x, next_point.y))
        else:
            prev_point = line.interpolate(current_distance - 10)
            angle = calculate_angle((prev_point.x, prev_point.y), (current_point.x, current_point.y))
        
        # Calculate perpendicular line endpoints
        dx = 50 * math.cos(math.radians(angle + 90))
        dy = 50 * math.sin(math.radians(angle + 90))
        start_point = (current_point.x - dx, current_point.y - dy)
        end_point = (current_point.x + dx, current_point.y + dy)

        # Calculate perpendicular line of length 160 m
        perpendicular_line = LineString([start_point, end_point])

        return perpendicular_line

def get_raster_value(point):
        """Get the elevation value from the raster at a given point"""
        with rasterio.open("data/mnt.tif") as src:
            print(f"DEM bounds: {src.bounds}")
            print(f"DEM shape: {src.shape}")
            print(f"DEM resolution: {src.res}")
            dem = src.read(1)
            transform = src.transform
        try:
            row, col = rasterio.transform.rowcol(transform, point.x, point.y)
            if 0 <= row < dem.shape[0] and 0 <= col < dem.shape[1]:
                return dem[row, col]
            else:
                print(f"Point outside raster bounds: row={row}, col={col}")
        except IndexError as e:
            print(f"IndexError: {e}")
        except Exception as e:
            print(f"Other error: {e}")
        return None

def find_closest_PR(point, PR_route):
    """
    Find the closest PR point with the smallest PR number
    """
    # Utiliser l'index spatial pour trouver rapidement les candidats les plus proches
    spatial_index_PR = PR_route.sindex
    try:
        # Créer un buffer autour du point pour la recherche
        buffer_dist = 1500  # métres
        buffered_bounds = (
            point.x - buffer_dist,
            point.y - buffer_dist,
            point.x + buffer_dist,
            point.y + buffer_dist
        )
        
        # Trouver les points dans ce buffer
        possible_matches_index = list(spatial_index_PR.intersection(buffered_bounds))
        print(f"Nombre de points de repère trouvés: {len(possible_matches_index)}")
        
        if not possible_matches_index:
            return None
            
        possible_matches = PR_route.iloc[possible_matches_index]

        # Filter possible_matches to keep only rows where 'numero' can be converted to an integer
        #possible_matches = possible_matches[possible_matches['numero'].apply(PR_route.is_convertible_to_int)]
        print(f"Points de repère restants après filtrage: {len(possible_matches)}")

        if possible_matches.empty:
            return None

        # Chercher les 2 PR les plus proches du point
        closest_two = possible_matches.distance(point).nsmallest(4).index
        possible_matches = possible_matches.loc[closest_two]

        print(f"Nombre dans possible matches: {len(possible_matches)}")

        # Parmi ces candidats, trouver le minimal
        candidates = []
        for index, row in possible_matches.iterrows():
            try:
                # Extract PR number
                pr_number = int(row['numero'])
                candidates.append((row, pr_number))
            except (IndexError, ValueError) as e:
                print(f"Error extracting PR number")
                continue
    
        if not candidates:
            return None
        else:
            # Sort candidates by PR number
            candidates.sort(key=lambda x: x[1])
            minimal_PR = candidates[0][0]
            
            return minimal_PR
        
    except Exception as e:
        print(f"Erreur lors de la recherche du PR de référence: {e}")
        return None

def visualize_profile(perpendicular_line, segment, current_distance, output_folder, route_number, PR_route):
    """Visualize the profile at a specific distance."""
    # Create profiles subfolder
    profiles_folder = os.path.join(output_folder, "profiles")
    os.makedirs(profiles_folder, exist_ok=True)

    intermediate_points = []
    distances = []
    elevations = []

    # Open raster once
    with rasterio.open("data/mnt.tif") as src:
        dem = src.read(1)
        transform = src.transform
        
        # Generate intermediate points along the perpendicular line
        for i in range(0, int(perpendicular_line.length) + 1):
            point = perpendicular_line.interpolate(i)
            try:
                row, col = rasterio.transform.rowcol(transform, point.x, point.y)
                if 0 <= row < dem.shape[0] and 0 <= col < dem.shape[1]:
                    elevation = dem[row, col]
                    if elevation is not None:
                        intermediate_points.append(point)
                        distances.append(i)
                        elevations.append(elevation)
            except IndexError:
                continue

    if not elevations:  # Skip if no valid elevations found
        print(f"No valid elevations found for profile at distance {current_distance}m")
        return

    # Plot the profile
    plt.figure(figsize=(20, 8))
    plt.plot(distances, elevations, label="Altitude terrain", marker="o", linestyle="-")

    middle_value = (max(elevations) + min(elevations)) / 2
    y_min = middle_value - 20
    y_max = middle_value + 20

    point_on_route = shapely.intersection(perpendicular_line, segment)
    print(f"Point on route: {point_on_route}")
    PR_before = find_closest_PR(point_on_route, PR_route)
    print(f"PR before: {PR_before['numero']}")
    PR_before_on_route = shapely.ops.nearest_points(segment, PR_before.geometry)[0]
    print(f"PR before on route: {PR_before_on_route}")

    start_substring = segment.project(PR_before_on_route)
    end_substring = segment.project(point_on_route)
    substring_to_point = shapely.ops.substring(segment, start_substring, end_substring)
    print(f"Substring to point: {substring_to_point}")

    profile_location_m = round(shapely.length(substring_to_point)/10) * 10

    # Add labels and legend
    plt.title(f"Profile à la distance PR{PR_before['numero']} + {profile_location_m} m")
    plt.xlabel("Distance depuis le début de la ligne perpendiculaire (m)")
    plt.ylabel("Altitude (m)")
    plt.legend()
    plt.grid(True)
    plt.xticks(range(0, max(distances) + 10, 10))
    plt.ylim(y_min, y_max)
    plt.tight_layout()

    # Save the plot
    output_file = os.path.join(
        profiles_folder, 
        f"profile_{route_number}_PR{PR_before['numero']}-{profile_location_m}m.png"
    )
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"\nProfile visualization saved: {output_file}")

def main():
    route_number = input("Saisissez le code de la route (ex. A33): ")
    segment_start_PR = input("Saisissez le PR de début (ex. 10): PR")
    segment_start_meters = input("Spécifiez combien de mètres après le PR de début que vous voulez commencer (ex. 100): ")
    segment_end_PR = input("Saisissez le PR de fin (ex. 15): PR")
    segment_end_meters = input("Saisissez combien de mètres après le PR de fin que vous voulez arrêter (ex. 500): ")
    espacement = input("Saisissez l'espacement entre les profiles (par défaut 25): ")

    output_folder = f"output_{route_number}"

    route_path = f'output_{route_number}/ouvrages_{route_number}.gpkg'
    route = gpd.read_file(route_path)

    print(f"\nNombre de segments avant la connexion: {len(route)}")

    with rasterio.open("data/mnt.tif") as src:
        print(f"DEM bounds: {src.bounds}")
        bbox = src.bounds

    filter_PR = f"route='{route_number}'"
    PR_route = get_data(filter_PR, "BDTOPO_V3:point_de_repere", bbox)
    
    # Get connected segments and centerline
    centerline = connect_segments(route)
    
    # Ensure centerline is a LineString
    if isinstance(centerline, shapely.geometry.MultiLineString):
        # Take the longest LineString from the MultiLineString
        centerline = max(centerline.geoms, key=lambda l: l.length)
    
    # Create GeoDataFrame with centerline
    route_buffered = gpd.GeoDataFrame(
        {'geometry': [centerline]}, 
        crs=route.crs
    )
    
    print(f"Centerline created successfully")
    
    # Save centerline
    output_centerline = os.path.join(output_folder, f"centerline_{route_number}.gpkg")
    route_buffered.to_file(output_centerline, driver="GPKG")
    print(f"\nCenterline has been saved to: {output_centerline}")

    # Get PR points and handle potential missing data
    PR_start_df = PR_route[PR_route['numero'] == segment_start_PR]
    PR_end_df = PR_route[PR_route['numero'] == segment_end_PR]

    # Check if we found the PR points
    if not PR_start_df.empty and not PR_end_df.empty:
        PR_start = PR_start_df.geometry.iloc[0]
        PR_end = PR_end_df.geometry.iloc[0]
        
        # Find closest points on centerline
        start_point_on_line = shapely.ops.nearest_points(centerline, PR_start)[0]
        end_point_on_line = shapely.ops.nearest_points(centerline, PR_end)[0]
        
        # Create points GeoDataFrame
        points_df = gpd.GeoDataFrame(
            {
                'type': ['start', 'end'],
                'PR': [segment_start_PR, segment_end_PR],
                'geometry': [start_point_on_line, end_point_on_line]
            },
            crs=route.crs
        )
        
        # Save points
        output_points = os.path.join(output_folder, f"pr_points_{route_number}.gpkg")
        points_df.to_file(output_points, driver="GPKG")
        print(f"\nPoints de début et de fin ont été sauvegardés dans: {output_points}")
        
    else:
        print("Les PRs spécifiés ne sont pas trouvés dans la route ou vous n'avez pas spécifié des PRs valides.")
        return
    
    # Create segment from startpoint + abscisse to endpoint + abscisse
    start_distance_PR = centerline.project(start_point_on_line)
    start_distance_total = start_distance_PR + int(segment_start_meters)
    start_chosen_segment = centerline.interpolate(start_distance_total)

    end_distance_PR = centerline.project(end_point_on_line)
    end_distance_total = end_distance_PR + int(segment_end_meters)
    end_chosen_segment = centerline.interpolate(end_distance_total)

    # Create point GeoDataFrame for visualization
    segment_points_df = gpd.GeoDataFrame(
        {
            'type': ['segment_start', 'segment_end'],
            'distance': [start_distance_total, end_distance_total],
            'geometry': [start_chosen_segment, end_chosen_segment]
        },
        crs=route.crs
    )
    
    # Save the point
    output_chosen_segment = os.path.join(output_folder, f"segment_start_{route_number}.gpkg")
    segment_points_df.to_file(output_chosen_segment, driver="GPKG")
    print(f"\nPoint de début du segment choisi sauvegardé dans: {output_chosen_segment}")
    
    # Get the substring between the two points
    chosen_segment = shapely.ops.substring(centerline, start_distance_total, end_distance_total)
    
    # Create GeoDataFrame for the chosen segment
    segment_df = gpd.GeoDataFrame(
        {
            'type': ['chosen_segment'],
            'start_PR': [f"PR{segment_start_PR}+{segment_start_meters}"],
            'end_PR': [f"PR{segment_end_PR}+{segment_end_meters}"],
            'geometry': [chosen_segment]
        },
        crs=route.crs
    )
    
    # Save the segment
    output_segment = os.path.join(output_folder, f"chosen_segment_{route_number}.gpkg")
    segment_df.to_file(output_segment, driver="GPKG")
    print(f"\nSegment choisi sauvegardé dans: {output_segment}")

    # Calculate perpendicular lines at intervals of X meters
    print("\nCalcul des lignes perpendiculaires...")
    if espacement:
        espacement = int(espacement)
    else:
        espacement = 25
    distances = list(range(0, int(chosen_segment.length), espacement))
    perpendicular_lines = []
    for distance in distances:
        perp_line = calculate_perpendicular_line(distance, chosen_segment)
        perpendicular_lines.append(perp_line)

    # Create GeoDataFrame for perpendicular lines
    perp_lines_df = gpd.GeoDataFrame(
        {'geometry': perpendicular_lines},
        crs=route.crs
    )

    # Save the perpendicular lines
    output_perpendicular_lines = os.path.join(output_folder, f"perpendicular_lines_{route_number}.gpkg")
    perp_lines_df.to_file(output_perpendicular_lines, driver="GPKG")
    print(f"\nLignes perpendiculaires sauvegardées dans: {output_perpendicular_lines}")

    print("\nCréation des profils d'élévation...")
    with tqdm(total=len(perpendicular_lines), desc="Generating profiles") as pbar:
        for i, perp_line in enumerate(perpendicular_lines):
            current_distance = i * espacement
            visualize_profile(
                perp_line, 
                chosen_segment,
                current_distance, 
                output_folder, 
                route_number, 
                PR_route
            )
            pbar.update(1)

if __name__ == "__main__":
    main()