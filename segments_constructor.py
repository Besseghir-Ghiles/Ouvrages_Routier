from shapely.geometry import LineString, Point, MultiLineString
import geopandas as gpd
import os
import math
from tqdm import tqdm
import time
from get_data_functions import get_data
from collections import Counter
import pandas as pd
from shapely.ops import unary_union, linemerge, snap

class SegmentConstructor:
    def __init__(self, classified_profiles, output_folder, route_number, lines_selected):
        self.classified_profiles = classified_profiles
        self.current_crs = classified_profiles.crs
        self.current_bounds = tuple(classified_profiles.total_bounds)
        self.output_folder = output_folder
        self.route_number = route_number
        self.filter_route = f"numero='{route_number}'"
        self.route = lines_selected
        pr_bounds = (
            self.current_bounds[0]-100,
            self.current_bounds[1]-100,
            self.current_bounds[2]+100,
            self.current_bounds[3]+100
        )
        self.filter_PR = f"route='{route_number}'"
        self.PR_route = get_data(self.filter_PR, "BDTOPO_V3:point_de_repere", pr_bounds)

        # supprimer les CS
        self.PR_route = self.PR_route[
            ~self.PR_route["libelle"]
            .astype(str)
            .str.startswith("CS")
        ].copy()

        # supprimer FS / DS
        self.PR_route = self.PR_route[
            self.PR_route["numero"].apply(
                self.is_convertible_to_int
            )
        ].copy()

        
        pr_290 = self.PR_route[
            self.PR_route["numero"] == 290
        ]

        print("\n===== PR 290 =====")
        print(pr_290[["numero","libelle","cote"]])

        print("\n===== GEOMETRY 290 =====")
        print(pr_290.geometry.bounds)

        print("Nombre PR récupérés IGN :", len(self.PR_route))
        print(self.PR_route[["numero","libelle"]])
        # Créer un index spatial pour accélérer la recherche de points
        
        print("Création de l'index spatial...")
        self.spatial_index = self.classified_profiles.sindex
        print("Index spatial créé")

    def calculate_distance(self, point1, point2):
        """Calculate the distance between two points"""
        return math.sqrt((point2.x - point1.x)**2 + (point2.y - point1.y)**2)
    
    def get_signed_side(self,point):

        d=self.central_line.project(point)

        proj=self.central_line.interpolate(d)

        delta=5

        d2=min(
            d+delta,
            self.central_line.length
        )

        proj2=self.central_line.interpolate(d2)

        tx=proj2.x-proj.x
        ty=proj2.y-proj.y

        vx=point.x-proj.x
        vy=point.y-proj.y

        cross=(tx*vy)-(ty*vx)

        return cross
    
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
    
    # projection des pr
    def project_PR_on_route(self):
        print("Projection PR sur ligne centrale")
        projected_pr=[]
        central_line=self.central_line

        for _,pr in self.PR_route.iterrows():

            local_distance=central_line.project(pr.geometry)
            projected_point=central_line.interpolate(local_distance)

            projected_pr.append({
                "geometry":projected_point,
                "distance_route":local_distance,
                "numero":pr["numero"],
                "libelle":pr["libelle"],
                "cote":pr["cote"]
            })

        projected_gdf=gpd.GeoDataFrame(
            projected_pr,
            geometry="geometry",
            crs=self.current_crs
        )

        return projected_gdf
    
    def project_PR_on_side(self):
        print("Projection PR sur chaussées")
        projected_pr=[]

        for _,pr in self.PR_route.iterrows():

            best_distance=float("inf")
            best_line=None

            for _,row in self.classified_lines_gdf.iterrows():

                line=row["geometry"]
                d=pr.geometry.distance(line)

                if d<best_distance:
                    best_distance=d
                    best_line=line

            if best_line is None:
                continue

            # IMPORTANT :
            # prendre uniquement les PR du même côté
            pr_same_side=self.PR_route[self.PR_route["cote"]==pr["cote"]]

            # remettre la ligne dans le bon sens
            best_line=self.orient_line_pr_croissant(best_line,pr_same_side)

            local_distance=best_line.project(pr.geometry)
            projected_point=best_line.interpolate(local_distance)

            projected_pr.append({
                "geometry":projected_point,
                "distance_route":local_distance,
                "numero":pr["numero"],
                "libelle":pr["libelle"],
                "cote":pr["cote"]
            })

        projected_side_gdf=gpd.GeoDataFrame(
            projected_pr,
            geometry="geometry",
            crs=self.current_crs
        )

        return projected_side_gdf

    def orient_line_pr_croissant(self,line,pr_gdf):

        if pr_gdf is None or pr_gdf.empty:
            return line

        pr_valides=pr_gdf.copy()

        pr_valides=pr_valides[
            pr_valides["numero"].apply(
                self.is_convertible_to_int
            )
        ]

        if len(pr_valides)<2:
            return line

        pr_valides["numero"]=pr_valides["numero"].astype(int)

        pr_valides=pr_valides.sort_values(
            "numero"
        ).reset_index(drop=True)

        pr_min=pr_valides.iloc[0]
        pr_max=pr_valides.iloc[-1]

        if "distance_route" in pr_valides.columns:

            pos_min=pr_min["distance_route"]
            pos_max=pr_max["distance_route"]

        else:

            pos_min=line.project(pr_min.geometry)
            pos_max=line.project(pr_max.geometry)

        if pos_min>pos_max:

            return LineString(
                list(line.coords)[::-1]
            )

        return line

    def find_closest_PR(self,point,line,PR_current,offset):

        try:

            route_distance=self.central_line.project(point)
            PR_current=PR_current.copy()

            PR_current["numero"]=PR_current["numero"].astype(int)
            PR_current=PR_current.sort_values("distance_route")

            previous_prs=PR_current[PR_current["distance_route"]<=route_distance]

            if not previous_prs.empty:
                return previous_prs.iloc[-1]

            next_prs=PR_current[PR_current["distance_route"]>route_distance]

            if next_prs.empty:
                return None

            next_pr=next_prs.iloc[0]

            fake_previous=next_pr.copy()
            fake_previous["numero"]=int(next_pr["numero"])-1
            fake_previous["distance_route"]=next_pr["distance_route"]-1000

            return fake_previous

        except Exception as e:

            print(f"Erreur PR:{e}")

            return None
        
    def prepare_route_points(self,ouvrages_gdf,line):

        points=[]

        for idx,row in ouvrages_gdf.iterrows():

            points.append({
                "segment_id":idx,
                "geometry":row["startpoint"],
                "point_type":"start",
                "chaussee":row["chaussee"]
            })

            points.append({
                "segment_id":idx,
                "geometry":row["endpoint"],
                "point_type":"end",
                "chaussee":row["chaussee"]
            })

        points_gdf=gpd.GeoDataFrame(points,geometry="geometry",crs=self.current_crs)

        points_gdf["distance_route"]=points_gdf.geometry.apply(lambda p:self.central_line.project(p))

        points_gdf["ordre_chaussee"]=points_gdf["chaussee"].map({"D":0,"G":1})

        points_gdf=points_gdf.sort_values(["ordre_chaussee","distance_route"]).reset_index(drop=True)

        points_gdf.drop(columns=["ordre_chaussee"],inplace=True)

        return points_gdf

    def compute_PR_reference(self,point,line,offset,PR_current):

        chaussee=PR_current.iloc[0]["cote"]

        PR_current=self.projected_PR[
            self.projected_PR["cote"]==chaussee
        ].copy()

        PR_current["numero"]=PR_current["numero"].astype(int)

        route_distance=self.central_line.project(point)

        PR_current=PR_current.sort_values("distance_route")

        previous_pr=PR_current[
            PR_current["distance_route"]<=route_distance
        ].sort_values("distance_route")

        # CAS 1 : aucun PR précédent
        if previous_pr.empty:

            next_pr=PR_current.sort_values(
                "distance_route"
            ).iloc[0]

            pr_num=int(next_pr["numero"])-1

            abcisse=route_distance-(next_pr["distance_route"]-1000)

            global_abscisse=pr_num*1000+abcisse

            return pr_num,abcisse,global_abscisse

        # CAS 2 : PR précédent trouvé
        ref_pr=previous_pr.iloc[-1]

        pr_num=int(ref_pr["numero"])

        abcisse=route_distance-ref_pr["distance_route"]

        global_abscisse=pr_num*1000+abcisse

        return pr_num,abcisse,global_abscisse
    
    def get_global_distance(self,point,line,offset):

        local_distance = line.project(point)
        return offset + local_distance
    
    def initialize_reference_system(self):
        print("Initialisation référence centrale...")
        classified_lines=[]
        cumulative_distance=0
        pr_g=self.PR_route[
            self.PR_route["cote"].astype(str).str.startswith("G")
        ]
        pr_d=self.PR_route[
            self.PR_route["cote"].astype(str).str.startswith("D")
        ]
        for index,geom in enumerate(self.route.geometry):
            if geom.geom_type=="MultiLineString":
                lines=geom.geoms
            elif geom.geom_type=="LineString":
                lines=[geom]
            else:
                continue
            for line_idx,line in enumerate(lines):
                if not isinstance(line,LineString):
                    continue
                line_id=f"{index+1}_{line_idx+1}"
                mid=line.interpolate(line.length/2)

                side=self.get_signed_side(mid)
                if side>0:
                    chaussee="G"
                    PR_current=pr_g
                else:
                    chaussee="D"
                    PR_current=pr_d
                line=self.orient_line_pr_croissant(
                    line,
                    PR_current
                )
                classified_lines.append({
                    "geometry":line,
                    "line_id":line_id,
                    "chaussee":chaussee,
                    "offset":cumulative_distance
                })
                cumulative_distance+=line.length
        self.classified_lines_gdf=gpd.GeoDataFrame(
            classified_lines,
            geometry="geometry",
            crs=self.current_crs
        )
        
        self.projected_PR_side=self.project_PR_on_side()
        self.projected_PR_side = self.projected_PR_side[self.projected_PR_side["numero"].apply(self.is_convertible_to_int)].copy()

        self.projected_PR=self.project_PR_on_route()

        self.projected_PR = self.projected_PR[self.projected_PR["numero"].apply(self.is_convertible_to_int)].copy()
        
        self.projected_PR["cote"]=self.projected_PR["cote"].astype(str).str.strip().str.upper()
        self.projected_PR["numero"]=self.projected_PR["numero"].astype(int)
        self.projected_PR=self.projected_PR.sort_values(
            ["cote","numero"]
        ).reset_index(drop=True)

        self.projected_PR_gauche=self.projected_PR_side[
            self.projected_PR_side["cote"].str.startswith("G")
        ].sort_values("numero").reset_index(drop=True)

        self.projected_PR_droite=self.projected_PR_side[
            self.projected_PR_side["cote"].str.startswith("D")
        ].sort_values("numero").reset_index(drop=True)

        print("PR gauche :",len(self.projected_PR_gauche))
        print("PR droite :",len(self.projected_PR_droite))

    def construct_segments(self):
        cumulative_distance = 0 #wagi  
        all_ouvrages = []
        start_time = time.time()
        log_file = open(
            os.path.join(
                self.output_folder,
                "debug_profils_selection.log"
            ),
            "w",
            encoding="utf-8"
        )
        
        gap_lines = []
        classified_lines = []

        print("Début de construct_segments()")
        print(f"Nombre de points dans classified_profiles: {len(self.classified_profiles)}")
        print(f"CRS de la route: {self.route.crs}")
        print(f"CRS des profils: {self.classified_profiles.crs}")

        if len(self.classified_profiles) == 0:
            print("classified_profiles est vide. Aucun segment ne sera généré.")
            return gpd.GeoDataFrame(columns=["geometry", "startpoint", "endpoint", "length", "classification"], crs=self.current_crs)
        
        self.build_central_line()
        self.initialize_reference_system()
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
                line_id = f"{index+1}_{line_idx+1}"
                mid = line.interpolate(line.length/2)
                side=self.get_signed_side(mid)

                if side>0:

                    chaussee="G"
                    PR_current=self.projected_PR_gauche

                else:

                    chaussee="D"
                    PR_current=self.projected_PR_droite

                line = self.orient_line_pr_croissant(line,PR_current)
                classified_lines.append({
                    "geometry": line,
                    "line_id": line_id,
                    "chaussee": chaussee,
                    "offset": cumulative_distance
                })

                current_offset = cumulative_distance
                cumulative_distance += line.length
                print(f"Ligne {line_id} classée : {chaussee}")
                print(f"Ligne {index+1}.{line_idx+1} -> chaussée {chaussee}")
                if PR_current is None:
                    print("PROBLÈME DE CHARGEMENT")
                    continue

                if PR_current.empty:
                    print(" aucun PR dans cette zone")
                    continue

                print(f"Nombre de PR: {len(PR_current)}")

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

                            hauteurs_centre = []
                            profils_hauteur_talus = []
                            profils_hauteur_centre = []
                            profils_pente = []

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
                                #deb    
                                with open(
                                    os.path.join(
                                        self.output_folder,
                                        "debug_talus.txt"
                                    ),
                                    "a",
                                    encoding="utf-8"
                                ) as f:

                                    f.write(
                                        f"\n=== PROFIL ===\n"
                                        f"profile_id={closest_row_j.name}\n"
                                        f"class={closest_row_j['classification']}\n"
                                        f"distance_route={j}\n"
                                        f"max_height_difference={closest_row_j['max_height_difference']}\n"
                                        f"talus_dist_min={closest_row_j['talus_dist_min']}\n"
                                        f"talus_dist_max={closest_row_j['talus_dist_max']}\n"
                                    ) #

                                #hauteur = closest_row_j['max_height_difference']

                                left_height = closest_row_j["max_height_difference"]
                                right_height = closest_row_j["right_height"]

                                left_height = 0 if pd.isna(left_height) else left_height
                                right_height = 0 if pd.isna(right_height) else right_height

                                if right_height > left_height:

                                    hauteur = right_height

                                    talus_dist_min = closest_row_j["right_dist_min"]
                                    talus_alt_min = closest_row_j["right_alt_min"]

                                    talus_dist_max = closest_row_j["right_dist_max"]
                                    talus_alt_max = closest_row_j["right_alt_max"]

                                else:

                                    hauteur = left_height

                                    talus_dist_min = closest_row_j["left_dist_min"]
                                    talus_alt_min = closest_row_j["left_alt_min"]

                                    talus_dist_max = closest_row_j["left_dist_max"]
                                    talus_alt_max = closest_row_j["left_alt_max"]
                                #deb
                                if pd.isna(hauteur):

                                    with open(
                                        os.path.join(
                                            self.output_folder,
                                            "debug_talus.txt"
                                        ),
                                        "a",
                                        encoding="utf-8"
                                    ) as f:

                                        f.write(
                                            ">>> HAUTEUR NAN <<<\n"
                                        )
                                #deb      
                                gap_count = 0
                                #hauteurs.append(hauteur)
                                if pd.notna(hauteur):
                                    hauteurs.append(hauteur)

                                    with open(
                                        os.path.join(
                                            self.output_folder,
                                            "debug_profile_id.txt"
                                        ),
                                        "a",
                                        encoding="utf-8"
                                    ) as f:

                                        f.write(
                                            f"\nindex={closest_row_j.name}"
                                            f" distance_profil={closest_row_j['distance_profil']}"
    )

                                    profils_hauteur_talus.append({
                                    "distance_route": self.central_line.project(
                                        closest_row_j.geometry
                                    ),
                                    "distance_ouvrage": j - i,
                                    "valeur": hauteur,
                                    "geometry": closest_row_j.geometry,
                                    "profile_id": closest_row_j.name,

                                    #"talus_dist_min": closest_row_j["talus_dist_min"],
                                    #"talus_alt_min": closest_row_j["talus_alt_min"],

                                    #"talus_dist_max": closest_row_j["talus_dist_max"],
                                    #"talus_alt_max": closest_row_j["talus_alt_max"],

                                    "talus_dist_min": talus_dist_min,
                                    "talus_alt_min": talus_alt_min,

                                    "talus_dist_max": talus_dist_max,
                                    "talus_alt_max": talus_alt_max,

                                    "left_dist_min": closest_row_j["left_dist_min"],
                                    "left_alt_min": closest_row_j["left_alt_min"],
                                    "left_dist_max": closest_row_j["left_dist_max"],
                                    "left_alt_max": closest_row_j["left_alt_max"],

                                    "right_dist_min": closest_row_j["right_dist_min"],
                                    "right_alt_min": closest_row_j["right_alt_min"],
                                    "right_dist_max": closest_row_j["right_dist_max"],
                                    "right_alt_max": closest_row_j["right_alt_max"],
                                    
                                })

                                hauteur_centre = (
                                    closest_row_j['hauteur_centre']
                                )
                                hauteurs_centre.append(hauteur_centre)

                                profils_hauteur_centre.append({
                                    "distance_route": self.central_line.project(closest_row_j.geometry),
                                    "distance_ouvrage": j - i,
                                    "valeur": hauteur_centre,
                                    "geometry": closest_row_j.geometry,
                                    "profile_id": closest_row_j.name,
                                })


                                """#
                                if closest_row_j['slope_ouvrage_section'] is not None:
                                    pente = closest_row_j['slope_ouvrage_section']
                                else:
                                    pente = closest_row_j['slope_ouvrage_total']
                                """

                                if pd.notna(
                                    closest_row_j['slope_ouvrage_section']
                                ):

                                    pente = (
                                        closest_row_j[
                                            'slope_ouvrage_section'
                                        ]
                                    )

                                elif pd.notna(
                                    closest_row_j['slope_ouvrage_total']
                                ):

                                    pente = (
                                        closest_row_j[
                                            'slope_ouvrage_total'
                                        ]
                                    )

                                else:

                                    pente = None


                                if pente is not None:
                                    pentes.append(pente)

                                    if pente is not None:

                                        profils_pente.append({
                                            "distance_route": self.central_line.project(closest_row_j.geometry),
                                            "distance_ouvrage": j - i,
                                            "valeur": pente,
                                            "geometry": closest_row_j.geometry,
                                            "profile_id": closest_row_j.name,
                                        })
                                #pentes.append(pente)
                                list_points.append(pointj_geo)
                                j += 1
                                iteration_count += 1

                            if len(list_points) >= 2:
                                segment = LineString(list_points)

                                start_dist = self.central_line.project(Point(segment.coords[0]))

                                end_dist = self.central_line.project(Point(segment.coords[-1]))

                                # si le segment est dans le mauvais sens
                                if start_dist > end_dist:
                                    segment = LineString(list(segment.coords)[::-1])
                                segment_startpoint = Point(segment.coords[0])
                                segment_endpoint = Point(segment.coords[-1])

                                PR_start = self.find_closest_PR(segment_startpoint, line, PR_current,current_offset)
                                PR_end = self.find_closest_PR(segment_endpoint,line, PR_current,current_offset)
                                if PR_start is None or PR_end is None:
                                    pr_start_lib = None
                                    pr_end_lib = None
                                    abcisse_start = None
                                    abcisse_end = None
                                    segment_name = f"{self.route_number}_NO_PR"
                                else:
                                    pr_num_start, abcisse_start, global_start = self.compute_PR_reference(
                                        segment_startpoint,
                                        line,
                                        current_offset,
                                        PR_current
                                    )

                                    pr_num_end, abcisse_end, global_end = self.compute_PR_reference(
                                        segment_endpoint,
                                        line, current_offset,
                                        PR_current
                                    )

                                    pr_start_lib = f"{self.route_number}PR{pr_num_start}{chaussee}"
                                    pr_end_lib = f"{self.route_number}PR{pr_num_end}{chaussee}"
                                    point_debut = f"{self.route_number}PR{pr_num_start}+{int(round(abcisse_start,-1))}_{chaussee}"
                                    point_fin = f"{self.route_number}PR{pr_num_end}+{int(round(abcisse_end,-1))}_{chaussee}"
                        
                                    segment_name = (

                                        f"{self.route_number}"
                                        f"_PR{pr_num_start}"
                                        f"+{int(round(abcisse_start,-1))}"
                                        f"_{chaussee}"

                                    )
                                pr_test = PR_current.copy()

                                pr_test["numero"] = (pr_test["numero"].astype(int))

                                pr_min = pr_test.loc[pr_test["numero"].idxmin()]

                                pr_max = pr_test.loc[pr_test["numero"].idxmax()]


                                profil_talus_max = max(
                                    profils_hauteur_talus,
                                    key=lambda x: x["valeur"]
                                ) if profils_hauteur_talus else None

                                if profil_talus_max:

                                    log_file.write(
                                        f"\nPROFILE TALUS MAX\n"
                                        f"profile_id={profil_talus_max['profile_id']}\n"
                                        f"distance_route={profil_talus_max['distance_route']}\n"
                                        f"distance_ouvrage={profil_talus_max['distance_ouvrage']}\n"
                                        f"valeur={profil_talus_max['valeur']}\n"
                                    )

                                pr_talus = None
                                abcisse_talus = None

                                if profil_talus_max:

                                    pr_talus, abcisse_talus, _ = (
                                        self.compute_PR_reference(
                                            profil_talus_max["geometry"],
                                            line,
                                            current_offset,
                                            PR_current
                                        )
                                    )

                                profil_centre_max = max(
                                    profils_hauteur_centre,
                                    key=lambda x: abs(x["valeur"])
                                ) if profils_hauteur_centre else None

                                profil_pente_max = max(
                                    profils_pente,
                                    key=lambda x: x["valeur"]
                                ) if profils_pente else None

                                all_ouvrages.append({
                                    'geometry': segment,
                                    'startpoint': segment_startpoint,
                                    'endpoint': segment_endpoint,
                                    'cote': chaussee,
                                    'chaussee': chaussee,
                                    'length': j - i,
                                    'classification': profile_type,
                                    'hauteur_talus_max':max(hauteurs) if hauteurs else 0,
                                    'hauteur_talus_moyenne':sum(hauteurs)/len(hauteurs)if hauteurs else 0,
                                    'profil_talus_route':profil_talus_max["distance_route"]if profil_talus_max else None,
                                    'profil_talus_ouvrage':profil_talus_max["distance_ouvrage"]if profil_talus_max else None,
                                    'hauteur_centre_max':profil_centre_max["valeur"]if profil_centre_max else 0,
                                    'profil_centre_route':profil_centre_max["distance_route"]if profil_centre_max else None,
                                    'profil_centre_ouvrage':profil_centre_max["distance_ouvrage"]if profil_centre_max else None,
                                    'hauteur_centre_moyenne':sum(hauteurs_centre)/ len(hauteurs_centre)if hauteurs_centre else 0,
                                    'pente_max': max(pentes) if pentes else 0,
                                    'profil_pente_route':profil_pente_max["distance_route"]if profil_pente_max else None,
                                    'profil_pente_ouvrage':profil_pente_max["distance_ouvrage"]if profil_pente_max else None,
                                    'pente_moyenne': sum(pentes)/len(pentes) if pentes else 0,
                                    'PR_start': pr_start_lib,
                                    'PR_end': pr_end_lib,                             
                                    'abcisse_start': round(abcisse_start, -1) if abcisse_start is not None else None,
                                    'abcisse_end': round(abcisse_end, -1) if abcisse_end is not None else None,
                                    'point_debut': point_debut,
                                    'point_fin': point_fin,
                                    'nom': segment_name,
                                    'route': self.route_number,
                                    "profil_talus_id":
                                    profil_talus_max["profile_id"]
                                    if profil_talus_max else None,

                                    "talus_dist_min":
                                    profil_talus_max["talus_dist_min"]
                                    if profil_talus_max else None,

                                    "talus_alt_min":
                                    profil_talus_max["talus_alt_min"]
                                    if profil_talus_max else None,

                                    "talus_dist_max":
                                    profil_talus_max["talus_dist_max"]
                                    if profil_talus_max else None,

                                    "talus_alt_max":
                                    profil_talus_max["talus_alt_max"]
                                    if profil_talus_max else None,

                                    "profil_centre_id":
                                    profil_centre_max["profile_id"]
                                    if profil_centre_max else None,

                                    "profil_pente_id":
                                    profil_pente_max["profile_id"]
                                    if profil_pente_max else None,
                                    "profil_talus_PR": pr_talus,
                                    "profil_talus_abcisse": abcisse_talus,

                                    "left_dist_min":
                                    profil_talus_max["left_dist_min"]
                                    if profil_talus_max else None,

                                    "left_alt_min":
                                    profil_talus_max["left_alt_min"]
                                    if profil_talus_max else None,

                                    "left_dist_max":
                                    profil_talus_max["left_dist_max"]
                                    if profil_talus_max else None,

                                    "left_alt_max":
                                    profil_talus_max["left_alt_max"]
                                    if profil_talus_max else None,

                                    "right_dist_min":
                                    profil_talus_max["right_dist_min"]
                                    if profil_talus_max else None,

                                    "right_alt_min":
                                    profil_talus_max["right_alt_min"]
                                    if profil_talus_max else None,

                                    "right_dist_max":
                                    profil_talus_max["right_dist_max"]
                                    if profil_talus_max else None,

                                    "right_alt_max":
                                    profil_talus_max["right_alt_max"]
                                    if profil_talus_max else None,
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

        classified_lines_gdf = gpd.GeoDataFrame(
            classified_lines,
            geometry="geometry",
            crs=self.current_crs
        )
        self.classified_lines_gdf = classified_lines_gdf

        selected_line = classified_lines_gdf.geometry.union_all()
        self.selected_line = selected_line
        ouvrages_gdf = gpd.GeoDataFrame(all_ouvrages, crs=self.current_crs, geometry="geometry")
        points_gdf = self.prepare_route_points(
            ouvrages_gdf,
            selected_line
        )
        ouvrages_gdf["PR_num"] = ouvrages_gdf["PR_start"].str.extract(r'PR(\d+)')[0].astype(int)

        ouvrages_gdf = ouvrages_gdf.sort_values(["chaussee","PR_num","abcisse_start"]).reset_index(drop=True)
        
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

        classified_lines_gdf.to_file(
            os.path.join(
                self.output_folder,
                f"classified_lines_{self.route_number}.gpkg"
            ),
            layer="lines",
            driver="GPKG"
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

    
    def build_central_line(self, step=5):

        print("Construction ligne centrale par perpendiculaires...")

        lines = []

        for geom in self.route.geometry:

            if geom is None or geom.is_empty:
                continue

            if geom.geom_type == "LineString":
                lines.append(geom)

            elif geom.geom_type == "MultiLineString":
                lines.extend(list(geom.geoms))

        print("Morceaux trouvés :", len(lines))

        if len(lines) < 2:
            raise ValueError(
                "Pas assez de lignes pour construire la ligne centrale"
            )

        union = unary_union(lines)

        snapped_lines = [
            snap(line, union, 5)
            for line in lines
        ]

        merged = linemerge(
            unary_union(snapped_lines)
        )

        if merged.geom_type == "LineString":
            raise ValueError(
                "Une seule chaussée détectée"
            )

        merged_lines = list(merged.geoms)

        merged_lines = sorted(
            merged_lines,
            key=lambda l: l.length,
            reverse=True
        )

        line1 = merged_lines[0]
        line2 = merged_lines[1]

        print(f"Longueur voie 1 : {line1.length:.2f}")
        print(f"Longueur voie 2 : {line2.length:.2f}")

        if (
            Point(line1.coords[0]).distance(Point(line2.coords[0]))
            >
            Point(line1.coords[0]).distance(Point(line2.coords[-1]))
        ):
            line2 = LineString(
                list(line2.coords)[::-1]
            )

        points = []

        normal_length = 120

        #n = int(line1.length / step)
        n = int(
            min(
                line1.length,
                line2.length
            ) / step
)

        last_offset_x = None
        last_offset_y = None

        for i in range(n + 1):
            t = i / n
            p1 = line1.interpolate(t,normalized=True)

            distance_on_line = (t * line1.length)
            d_before = max(distance_on_line - 5,0)
            d_after = min(distance_on_line + 5,line1.length)

            p_before = line1.interpolate(d_before)
            p_after = line1.interpolate(d_after)

            dx = p_after.x - p_before.x
            dy = p_after.y - p_before.y

            norm = math.sqrt(
                dx * dx +
                dy * dy
            )

            if norm == 0:
                continue

            dx = dx / norm
            dy = dy / norm

            nx = -dy
            ny = dx

            normal_line = LineString([
                (p1.x - normal_length * nx,p1.y - normal_length * ny),
                (p1.x + normal_length * nx,p1.y + normal_length * ny)])
            
            candidates = []
            search_offsets = [0,-5, 5,-10, 10,-20, 20]

            for offset in search_offsets:

                test_distance = min(max(distance_on_line + offset, 0), line1.length)
                test_point = line1.interpolate(test_distance)

                d_before_test = max(test_distance - 5, 0)
                d_after_test = min(test_distance + 5, line1.length)

                p_before_test = line1.interpolate(d_before_test)
                p_after_test = line1.interpolate(d_after_test)

                dx_test = p_after_test.x - p_before_test.x
                dy_test = p_after_test.y - p_before_test.y

                norm_test = math.sqrt(dx_test * dx_test + dy_test * dy_test)

                if norm_test == 0:
                    continue

                dx_test /= norm_test
                dy_test /= norm_test

                nx_test = -dy_test
                ny_test = dx_test

                test_normal = LineString([
                    (test_point.x -normal_length * nx_test, test_point.y -normal_length * ny_test),
                    (test_point.x +normal_length * nx_test, test_point.y +normal_length * ny_test)])

                inter = test_normal.intersection(line2)

                if inter.is_empty:
                    continue

                if inter.geom_type == "Point":
                    candidates.append(inter)

                elif inter.geom_type == "MultiPoint":
                    candidates.extend(list(inter.geoms))

                elif inter.geom_type in ["LineString","MultiLineString"]:
                    d = line2.project(test_point)
                    candidates.append(line2.interpolate(d))

                elif inter.geom_type == "GeometryCollection":
                    for g in inter.geoms:
                        if g.geom_type == "Point":
                            candidates.append(g)

                if candidates:
                    break

            if candidates:
                p2 = min(candidates,key=lambda p: p.distance(p1))
                last_offset_x = (p2.x - p1.x)
                last_offset_y = (p2.y - p1.y)

            else:
                if (last_offset_x is not None and last_offset_y is not None):
                    p2 = Point(p1.x + last_offset_x,p1.y + last_offset_y)
                else:
                    d = line2.project(p1)
                    p2 = line2.interpolate(d)
                    last_offset_x = (p2.x - p1.x)
                    last_offset_y = (p2.y - p1.y)

            middle = Point(
                (p1.x + p2.x) / 2,(p1.y + p2.y) / 2)

            points.append(middle)

        if len(points) < 2:
            raise ValueError("Impossible de construire la ligne centrale")
        
        self.central_line = LineString(points)

        pr_valides = self.PR_route.copy()

        pr_valides = pr_valides[pr_valides["numero"].apply(self.is_convertible_to_int)].copy()

        pr_valides["numero"] = (pr_valides["numero"].astype(int))

        pr_min = pr_valides.loc[pr_valides["numero"].idxmin()]
        pr_max = pr_valides.loc[pr_valides["numero"].idxmax()]

        pos_min = self.central_line.project(pr_min.geometry)
        pos_max = self.central_line.project(pr_max.geometry)

        if pos_min > pos_max:
            self.central_line = LineString(list(self.central_line.coords)[::-1])
            print("Ligne centrale inversée pour avoir PR croissant")

        central_gdf = gpd.GeoDataFrame(
            [{
                "name": "central_line",
                "geometry": self.central_line
            }],
            geometry="geometry",
            crs=self.route.crs
        )

        central_path = os.path.join(self.output_folder,"central_line_profil.gpkg")

        central_gdf.to_file(central_path,layer="central_line",driver="GPKG")
        print(f"Ligne centrale sauvegardée : {central_path}")
        print(f"Longueur centrale : {self.central_line.length:.2f}")
        return self.central_line

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
        
        if hasattr(self,"projected_PR"):
            self.projected_PR.to_file(os.path.join(self.output_folder,"projected_PR.gpkg"),
                layer="PR_projection",
                driver="GPKG"
            )

        ouvrage_points=[]

        for _,row in ouvrages_gdf.iterrows():

            ouvrage_points.append({
                "geometry":row["startpoint"],
                "nom":row["nom"],
                "point_type":"start",
                "PR":row["PR_start"],
                "abcisse":row["abcisse_start"],
                "point_nom":row["point_debut"],
                "chaussee":row["chaussee"]

            })

            ouvrage_points.append({
                "geometry":row["endpoint"],
                "nom":row["nom"],
                "point_type":"end",
                "PR":row["PR_end"],
                "abcisse":row["abcisse_end"],
                "point_nom":row["point_fin"],
                "chaussee":row["chaussee"]

            })
        ouvrage_points_gdf = gpd.GeoDataFrame(
            ouvrage_points,
            geometry="geometry",
            crs=self.current_crs
        )
        ouvrage_points_gdf["distance_route"] = (
            ouvrage_points_gdf.geometry.apply(
                #lambda p: self.selected_line.project(p)
                lambda p: self.central_line.project(p)
            )
        )
        ouvrage_points_gdf["ordre_chaussee"] = (
            ouvrage_points_gdf["chaussee"]
            .map({
                "D":0,
                "G":1
            })
        )
        ouvrage_points_gdf = (
            ouvrage_points_gdf
            .sort_values(
                [
                    "ordre_chaussee",
                    "distance_route",
                    "point_type"
                ]
            )
            .drop(
                columns=["ordre_chaussee"]
            )
            .reset_index(
                drop=True
            )
        )
        ouvrage_points_file = os.path.join(
            self.output_folder,
            f"ouvrage_points_{self.route_number}.gpkg"
        )

        ouvrage_points_gdf.to_file(
            ouvrage_points_file,
            layer="points",
            driver="GPKG"
        )
        print(f"Points ouvrages sauvegardés : {ouvrage_points_file}")
        print(f"Ouvrage segments saved as: {output_file}")