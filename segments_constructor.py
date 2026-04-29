from shapely.geometry import LineString, Point, MultiLineString
import geopandas as gpd
import os
import math
from tqdm import tqdm
import time
from get_data_functions import get_data

class SegmentConstructor:
    def __init__(self, classified_profiles, output_folder, route_number, lines_selected):
        self.classified_profiles = classified_profiles
        self.current_crs = classified_profiles.crs
        self.current_bounds = tuple(classified_profiles.total_bounds)
        self.output_folder = output_folder
        self.route_number = route_number
        self.filter_route = f"numero='{route_number}'"
        self.route = lines_selected

        self.filter_PR = f"route='{route_number}'"
        self.PR_route = get_data(self.filter_PR, "BDTOPO_V3:point_de_repere", self.current_bounds)
        
        # Créer un index spatial pour accélérer la recherche de points
        print("Création de l'index spatial...")
        self.spatial_index = self.classified_profiles.sindex
        print("Index spatial créé")

    def calculate_distance(self, point1, point2):
        """Calculate the distance between two points"""
        return math.sqrt((point2.x - point1.x)**2 + (point2.y - point1.y)**2)
    
    def determine_closest_point(self, given_point):
        """Trouve le point le plus proche avec une recherche optimisée."""
        # Utiliser l'index spatial pour trouver rapidement les candidats les plus proches
        try:
            # Créer un buffer autour du point pour la recherche
            buffer_dist = 20  # métres
            buffered_bounds = (
                given_point.x - buffer_dist,
                given_point.y - buffer_dist,
                given_point.x + buffer_dist,
                given_point.y + buffer_dist
            )
            
            # Trouver les points dans ce buffer
            possible_matches_index = list(self.spatial_index.intersection(buffered_bounds))
            
            if not possible_matches_index:
                return None, float('inf')
                
            possible_matches = self.classified_profiles.iloc[possible_matches_index]
            
            # Parmi ces candidats, trouver le plus proche
            closest_row = None
            min_distance = float('inf')
            
            for index, row in possible_matches.iterrows():
                distance = self.calculate_distance(row.geometry, given_point)
                if distance < min_distance:
                    min_distance = distance
                    closest_row = row
                    
            return closest_row, min_distance
            
        except Exception as e:
            print(f"Erreur lors de la recherche du point le plus proche: {e}")
            return None, float('inf')
        
    def is_convertible_to_int(self, x):
        try:
            int(str(x))
            return True
        except ValueError:
            return False

    def find_closest_PR(self, point, PR_current):
        """
        Find the closest PR point with the smallest PR number
        """
        # Utiliser l'index spatial pour trouver rapidement les candidats les plus proches
        spatial_index_PR = PR_current.sindex
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
                
            possible_matches = PR_current.iloc[possible_matches_index]

            # Filter possible_matches to keep only rows where 'numero' can be converted to an integer
            possible_matches = possible_matches[possible_matches['numero'].apply(self.is_convertible_to_int)]
            print(f"Points de repère restants après filtrage: {len(possible_matches)}")

            if possible_matches.empty:
                return None

            # Chercher les 2 PR les plus proches du point
            closest_two = possible_matches.distance(point).nsmallest(2).index
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

    def construct_segments(self):
        all_ouvrages = []
        start_time = time.time()
        gap_lines = []

        print("Début de construct_segments()")
        print(f"Nombre de points dans classified_profiles: {len(self.classified_profiles)}")
        print(f"CRS de la route: {self.route.crs}")
        print(f"CRS des profils: {self.classified_profiles.crs}")

        if len(self.classified_profiles) == 0:
            print("classified_profiles est vide. Aucun segment ne sera généré.")
            return gpd.GeoDataFrame(columns=["geometry", "startpoint", "endpoint", "length", "classification"], crs=self.current_crs)

        for index, geom in enumerate(self.route.geometry):

            if geom.geom_type == "MultiLineString":
                lines = geom.geoms
            elif geom.geom_type == "LineString":
                lines = [geom]
            else:
                continue

            for line_idx, line in enumerate(lines):

                if not isinstance(line, LineString):
                    continue

                i = 0
                length_line = line.length
                print(f"Traitement de la ligne {index+1}.{line_idx+1} - Longueur: {length_line:.2f} m")
                """ 
                line_buffer = line.buffer(1)
                PR_current = self.PR_route[self.PR_route.geometry.intersects(line_buffer)]
                """ 
                PR_current = self.PR_route
                print(f"Nombre de points de repère dans la ligne {index+1}.{line_idx+1}: {len(PR_current)}")

                with tqdm(total=int(length_line), desc=f"Processing Line {index+1}.{line_idx+1}") as pbar:

                    while i < length_line:

                        if time.time() - start_time > 3600:
                            break

                        pointi_geo = line.interpolate(i)

                        closest_row, min_distance = self.determine_closest_point(pointi_geo)

                        if closest_row is None:
                            i += 1
                            pbar.update(1)
                            continue

                        if min_distance < 20:

                            profile_type = closest_row['classification']

                            list_points = [pointi_geo]
                            j = i + 1
                            max_search = min(i + 1000, length_line)

                            iteration_count = 0
                            max_iterations = 1000

                            hauteurs = []
                            pentes = []
                            gap_count = 0
                            max_gap = 5

                            while j < max_search and iteration_count < max_iterations:

                                pointj_geo = line.interpolate(j)
                                closest_row_j, min_distance_j = self.determine_closest_point(pointj_geo)

                                if closest_row_j is None or min_distance_j > 20:
                                    break

                                if closest_row_j['classification'] != profile_type:

                                    #  calcul longueur du trou
                                    gap_length = self.calculate_distance(list_points[-1], pointj_geo)

                                    #  éviter les gros trous
                                    if gap_length > 15:   
                                        break

                                    opposite_class = self.get_opposite_class(line, j)

                                    #if opposite_class is not None and profile_type in opposite_class:
                                    if opposite_class is not None:

                                        # detecte changement
                                        change_point = list_points[-1]

                                        if not hasattr(self, "break_points"):
                                            self.break_points = []

                                        self.break_points.append({
                                            "geometry": change_point,
                                            "class_from": profile_type,
                                            "class_to": closest_row_j['classification'],
                                            "gap_distance": min_distance_j,
                                            "type": "changement_classification"
                                        })
                                        

                                        # si plusieurs classes prendre la première ou dominante
                                        if isinstance(opposite_class, list):
                                            from collections import Counter
                                            forced_class = Counter(opposite_class).most_common(1)[0][0]
                                        else:
                                            forced_class = opposite_class

                                        gap_lines.append({
                                            "geometry": LineString([list_points[-1], pointj_geo]),
                                            "classification": forced_class,
                                            "class_original": closest_row_j['classification'],
                                            "class_corrected": forced_class,
                                            "length": self.calculate_distance(list_points[-1], pointj_geo),
                                            "type": "gap_filled"
                                        })

                                        gap_count += 1

                                        if gap_count > max_gap:
                                            break



                                        list_points.append(pointj_geo)
                                        j += 1
                                        continue

                                    break


                                hauteur = closest_row_j['max_height_difference']
                                gap_count = 0
                                hauteurs.append(hauteur)

                                if closest_row_j['slope_ouvrage_section'] is not None:
                                    pente = closest_row_j['slope_ouvrage_section']
                                else:
                                    pente = closest_row_j['slope_ouvrage_total']

                                pentes.append(pente)

                                list_points.append(pointj_geo)
                                j += 1
                                iteration_count += 1

                            if len(list_points) >= 2:

                                segment = LineString(list_points)

                                segment_startpoint = segment.interpolate(0)
                                segment_endpoint = segment.interpolate(-1)

                                PR_start = self.find_closest_PR(segment_startpoint, PR_current)
                                PR_end = self.find_closest_PR(segment_endpoint, PR_current)
                                """ 
                                if PR_start is None or PR_end is None:
                                    i += 1
                                    pbar.update(1)
                                    continue
                                """

                                if PR_start is None or PR_end is None:
                                    pr_start_lib = None
                                    pr_end_lib = None
                                    abcisse_start = None
                                    abcisse_end = None
                                    segment_name = f"{self.route_number}_NO_PR"
                                else:
                                    closest_point_on_line_PR_start = line.interpolate(line.project(PR_start.geometry))
                                    closest_point_on_line_PR_end = line.interpolate(line.project(PR_end.geometry))

                                    abcisse_start = line.project(segment_startpoint) - line.project(closest_point_on_line_PR_start)
                                    abcisse_end = line.project(segment_endpoint) - line.project(closest_point_on_line_PR_end)

                                    pr_start_lib = PR_start['libelle']
                                    pr_end_lib = PR_end['libelle']
                                    segment_name = f"{self.route_number}_PR{PR_start['numero']}-{int(round(abcisse_start, -1))}_{PR_start['cote']}"
                                
                                """ 
                                closest_point_on_line_PR_start = line.interpolate(line.project(PR_start.geometry))
                                closest_point_on_line_PR_end = line.interpolate(line.project(PR_end.geometry))

                                abcisse_start = line.project(segment_startpoint) - line.project(closest_point_on_line_PR_start)
                                abcisse_end = line.project(segment_endpoint) - line.project(closest_point_on_line_PR_end)

                                segment_name = f"{self.route_number}_PR{PR_start['numero']}-{int(round(abcisse_start, -1))}_{PR_start['cote']}"

                                """

                                all_ouvrages.append({
                                    'geometry': segment,
                                    'length': j - i,
                                    'classification': profile_type,
                                    'hauteur_max': max(hauteurs) if hauteurs else 0,
                                    'pente_max': max(pentes) if pentes else 0,
                                    'hauteur_moyenne': sum(hauteurs)/len(hauteurs) if hauteurs else 0,
                                    'pente_moyenne': sum(pentes)/len(pentes) if pentes else 0,
                                    #'PR_start': PR_start['libelle'],
                                    #'PR_end': PR_end['libelle'],
                                    'PR_start': pr_start_lib,
                                    'PR_end': pr_end_lib,

                                    #'abcisse_start': round(abcisse_start, -1),
                                    #'abcisse_end': round(abcisse_end, -1),
                                    
                                    'abcisse_start': round(abcisse_start, -1) if abcisse_start is not None else None,
                                    'abcisse_end': round(abcisse_end, -1) if abcisse_end is not None else None,
                                    'nom': segment_name,
                                    'route': self.route_number
                                })

                                delta = j - i
                                pbar.update(delta)
                                i = j

                            else:
                                i += 1
                                pbar.update(1)

                        else:
                            i += 1
                            pbar.update(1)

        if not all_ouvrages:
            print("all_ouvrages est vide après traitement.")
            return gpd.GeoDataFrame(columns=["geometry", "startpoint", "endpoint", "length", "classification", "hauteur_max", "pente_max"], crs=self.current_crs)

        print(f"Segments générés: {len(all_ouvrages)}")

        ouvrages_gdf = gpd.GeoDataFrame(all_ouvrages, crs=self.current_crs, geometry="geometry")

        
        # VISUALISATION DES LIGNE ROUTE

        lines = []

        for geom in self.route.geometry:
            if geom.geom_type == "MultiLineString": # si la route est décpuper en plusiseurs
                for idx, line in enumerate(geom.geoms):
                    lines.append({
                        "geometry": line, # la ligne
                        "line_id": idx + 1 # id pour lasser 
                    })
            elif geom.geom_type == "LineString":
                lines.append({
                    "geometry": geom,
                    "line_id": 1
                })

        lines_gdf = gpd.GeoDataFrame(lines, crs=self.route.crs) # transfomrer  la liste en couche SIG

        output_lines = os.path.join(self.output_folder, f"route_lines_{self.route_number}.gpkg")
        """ # on supprime si ça exesite deja 
        if os.path.exists(output_lines):
            os.remove(output_lines)
        """
        lines_gdf.to_file(output_lines, layer="lines", driver="GPKG")

        print(f"Lignes route sauvegardées ici : {output_lines}")

        if hasattr(self, "break_points"):
            self.break_points_gdf = gpd.GeoDataFrame(
                self.break_points,
                geometry="geometry",
                crs=self.current_crs
            )
        else:
            self.break_points_gdf = gpd.GeoDataFrame(
                columns=["geometry"],
                geometry="geometry",
                crs=self.current_crs
            )

        if gap_lines:
            self.gap_lines_gdf = gpd.GeoDataFrame(
                gap_lines,
                geometry="geometry",
                crs=self.current_crs
            )

        return ouvrages_gdf

    def get_opposite_class(self, line, distance):

        # point courant
        current_point = line.interpolate(distance)

        # vraie perpendiculaire
        perp = self.calculate_perpendicular_line(distance, line)

        for geom in self.route.geometry:

            # ignorer la même ligne
            if geom.equals(line):
                continue

            # intersection avec l'autre voie
            inter = perp.intersection(geom)

            if inter.is_empty:
                continue

            # gérer les cas géométriques
            if inter.geom_type == "Point":
                target_point = inter

            elif inter.geom_type == "MultiPoint":
                # prendre le point le plus proche du centre
                target_point = min(
                    list(inter.geoms),
                    key=lambda p: p.distance(current_point)
                )

            else:
                continue

            # récupérer le profil le plus proche de ce point
            row, dist = self.determine_closest_point(target_point)

            if row is not None and dist < 20:
                return row["classification"]

        return None

    def calculate_perpendicular_line(self, current_distance, line):
        current_point = line.interpolate(current_distance)

        if current_distance <= 15:
            next_point = line.interpolate(current_distance + 10)
            angle = math.atan2(next_point.y - current_point.y, next_point.x - current_point.x)
        else:
            prev_point = line.interpolate(current_distance - 10)
            angle = math.atan2(current_point.y - prev_point.y, current_point.x - prev_point.x)

        dx = 60 * math.cos(angle + math.pi / 2)
        dy = 60 * math.sin(angle + math.pi / 2)

        start_point = (current_point.x - dx, current_point.y - dy)
        end_point = (current_point.x + dx, current_point.y + dy)

        return LineString([start_point, end_point])

    def save_output(self, ouvrages_gdf):
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)
        file_name = f"ouvrages_{self.route_number}.gpkg"
        output_file = os.path.join(self.output_folder, file_name)
        
        # Save segments
        ouvrages_gdf.to_file(output_file, driver='GPKG', layer='segments')

        if hasattr(self, "break_points_gdf") and not self.break_points_gdf.empty:
            self.break_points_gdf.to_file(
                os.path.join(self.output_folder, "break_points.gpkg"),
                driver="GPKG"
    )
            
        if hasattr(self, "gap_lines_gdf") and not self.gap_lines_gdf.empty:
            self.gap_lines_gdf.to_file(
                os.path.join(self.output_folder, "gap_corrections.gpkg"),
                driver="GPKG"
            )
        
        print(f"Ouvrage segments saved as: {output_file}")