import os
import math
import geopandas as gpd
import rasterio
import shapely
import matplotlib.pyplot as plt

from scipy.spatial import cKDTree
from shapely.geometry import LineString, Point
from shapely.ops import (
    unary_union,
    substring,
    linemerge,
    snap
)

from tqdm import tqdm
from get_data_functions import get_data
import laspy
import numpy as np


MNT_PATH = "data/mnt.tif"
LIDAR_PATH = "data/lidar.las"


# CENTERLINE ROBUSTE

def build_clean_centerline(lines_selected, step=5):

    lines = []

    for geom in lines_selected.geometry:

        if geom is None or geom.is_empty:
            continue

        if geom.geom_type == "LineString":
            lines.append(geom)

        elif geom.geom_type == "MultiLineString":
            lines.extend(list(geom.geoms))

    print("Nombre de morceaux trouvés :", len(lines))

    if len(lines) < 2:
        raise ValueError(
            "Pas assez de lignes pour construire une centerline."
        )

    # reconnecter petits gaps
    union = unary_union(lines)

    snapped_lines = [
        snap(line, union, 5)
        for line in lines
    ]

    merged = linemerge(unary_union(snapped_lines))

    # Si une seule ligne
    if merged.geom_type == "LineString":
        return merged

    merged_lines = list(merged.geoms)

    print("Lignes fusionnées :", len(merged_lines))

    # retirer petits morceaux parasites
    merged_lines = [
        l for l in merged_lines
        if l.length > 100
    ]

    merged_lines = sorted(
        merged_lines,
        key=lambda l: l.length,
        reverse=True
    )

    if len(merged_lines) < 2:
        raise ValueError(
            "Impossible de trouver deux chaussées principales."
        )

    # garder les 2 plus longues
    line1 = merged_lines[0]
    line2 = merged_lines[1]

    print("Longueur voie 1 :", line1.length)
    print("Longueur voie 2 :", line2.length)

    # remettre dans le même sens
    d1 = Point(line1.coords[0]).distance(Point(line2.coords[0]))
    d2 = Point(line1.coords[0]).distance(Point(line2.coords[-1]))

    if d2 < d1:
        line2 = LineString(list(line2.coords)[::-1])

    n = int(min(line1.length, line2.length) / step)

    points = []

    for i in range(n + 1):

        t = i / n

        p1 = line1.interpolate(t, normalized=True)
        p2 = line2.interpolate(t, normalized=True)

        midpoint = Point(
            (p1.x + p2.x) / 2,
            (p1.y + p2.y) / 2
        )

        points.append(midpoint)

    centerline = LineString(points)

    # léger lissage
    centerline = centerline.simplify(1)

    return centerline


# ANGLE / PERPENDICULAIRES

def calculate_angle(point1, point2):

    dx = point2[0] - point1[0]
    dy = point2[1] - point1[1]

    return math.degrees(math.atan2(dy, dx))


def calculate_perpendicular_line(
    distance_on_line,
    line,
    half_length=50
):

    current_point = line.interpolate(distance_on_line)

    if distance_on_line <= 15:

        next_distance = min(
            distance_on_line + 10,
            line.length
        )

        next_point = line.interpolate(next_distance)

        angle = calculate_angle(
            (current_point.x, current_point.y),
            (next_point.x, next_point.y)
        )

    else:

        prev_distance = max(
            distance_on_line - 10,
            0
        )

        prev_point = line.interpolate(prev_distance)

        angle = calculate_angle(
            (prev_point.x, prev_point.y),
            (current_point.x, current_point.y)
        )

    dx = half_length * math.cos(
        math.radians(angle + 90)
    )

    dy = half_length * math.sin(
        math.radians(angle + 90)
    )

    start_point = (
        current_point.x - dx,
        current_point.y - dy
    )

    end_point = (
        current_point.x + dx,
        current_point.y + dy
    )

    return LineString([start_point, end_point])


# EXTRACTION MNT

def extract_elevation_profile(
    perpendicular_line,
    dem,
    transform
):

    distances = []
    elevations = []

    for i in range(
        0,
        int(perpendicular_line.length) + 1
    ):

        point = perpendicular_line.interpolate(i)

        try:

            row, col = rasterio.transform.rowcol(
                transform,
                point.x,
                point.y
            )

            if (
                0 <= row < dem.shape[0]
                and
                0 <= col < dem.shape[1]
            ):

                elevation = dem[row, col]

                if elevation is not None:

                    distances.append(i)
                    elevations.append(elevation)

        except Exception:
            continue

    return distances, elevations


# EXTRACTION LIDAR

def extract_lidar_profile(
    perpendicular_line,
    lidar,
    tree
):

    distances = []
    elevations = []

    z = lidar.z

    for i in range(
        0,
        int(perpendicular_line.length) + 1
    ):

        point = perpendicular_line.interpolate(i)

        idx = tree.query_ball_point(
            [point.x, point.y],
            r=2
        )

        if idx:

            elevations.append(np.mean(z[idx]))
            distances.append(i)

    return distances, elevations

# PNG PROFIL

def save_profile_png(
    distances,
    elevations,
    current_distance,
    output_folder,
    route_number
):

    profiles_folder = os.path.join(
        output_folder,
        "profiles"
    )

    os.makedirs(profiles_folder, exist_ok=True)

    if not elevations:
        print(
            f"Aucune altitude valide "
            f"pour le profil à {current_distance} m"
        )
        return

    plt.figure(figsize=(20, 8))

    plt.plot(
        distances,
        elevations,
        label="Altitude terrain",
        marker="o",
        linestyle="-"
    )

    middle_value = (
        max(elevations) + min(elevations)
    ) / 2

    plt.ylim(
        middle_value - 20,
        middle_value + 20
    )

    plt.title(
        f"Profil à {current_distance} m "
        f"le long de la route"
    )

    plt.xlabel(
        "Distance sur la perpendiculaire (m)"
    )

    plt.ylabel("Altitude (m)")

    plt.legend()
    plt.grid(True)

    plt.xticks(
        range(
            0,
            max(distances) + 10,
            10
        )
    )

    plt.tight_layout()

    output_file = os.path.join(
        profiles_folder,
        f"profile_{route_number}_{current_distance}m.png"
    )

    plt.savefig(
        output_file,
        dpi=300,
        bbox_inches="tight"
    )

    plt.close()

    print(f"Profil sauvegardé : {output_file}")


# MAIN
def main():

    use_lidar = (
        input("Utiliser LiDAR ? (y/n): ").lower() == "y"
    )

    lidar_data = None

    if use_lidar:

        if not os.path.exists(LIDAR_PATH):

            raise FileNotFoundError(
                f"Fichier LiDAR introuvable : {LIDAR_PATH}"
            )

        print(f"Chargement du LiDAR : {LIDAR_PATH}")

        lidar_data = laspy.read(LIDAR_PATH)

    route_number = input(
        "Saisissez le code de la route (ex. A31): "
    )

    start_distance = int(
        input(
            "Distance de début en mètres (ex. 0): "
        )
    )

    end_distance = int(
        input(
            "Distance de fin en mètres (ex. 1800): "
        )
    )

    spacing = int(
        input(
            "Espacement entre profils en mètres (ex. 20): "
        )
    )

    output_folder = f"output_{route_number}"

    os.makedirs(output_folder, exist_ok=True)

    route_path = os.path.join(
        output_folder,
        f"ouvrages_{route_number}.gpkg"
    )

    if not os.path.exists(route_path):

        raise FileNotFoundError(
            f"Fichier introuvable : {route_path}"
        )

    route_gdf = gpd.read_file(route_path)

    if route_gdf.empty:
        raise ValueError(
            "Le fichier ouvrages est vide."
        )

    route_gdf = route_gdf[
        route_gdf.geometry.notnull()
    ]

    route_gdf = route_gdf[
        ~route_gdf.geometry.is_empty
    ]

    print(
        f"Nombre de segments lus : "
        f"{len(route_gdf)}"
    )

    # MNT
   
    with rasterio.open(MNT_PATH) as src:

        print(f"DEM bounds: {src.bounds}")
        print(f"DEM shape: {src.shape}")
        print(f"DEM resolution: {src.res}")

        bbox = src.bounds

        dem = src.read(1)
        transform = src.transform

    # CHARGEMENT BD TOPO

    print("Chargement des chaussées BD TOPO...")

    filter_route_lines = (
        f"cpx_numero='{route_number}'"
    )

    lines_selected = get_data(
        filter_route_lines,
        "BDTOPO_V3:troncon_de_route",
        bbox
    )

    lines_selected = lines_selected.explode(
        index_parts=False
    )

    lines_selected = lines_selected.reset_index(
        drop=True
    )

    lines_selected = lines_selected[
        lines_selected['nature'].isin([
            'Type autoroutier',
            'Route à 2 chaussées',
            'Route à 1 chaussée'
        ])
    ]

    print(
        "Nombre de lignes BD TOPO :",
        len(lines_selected)
    )

    # CENTERLINE
    centerline = build_clean_centerline(
        lines_selected
    )

    print(
        f"Longueur de la centerline : "
        f"{centerline.length:.2f} m"
    )

    # CONTRÔLES DISTANCES
    if start_distance < 0:

        raise ValueError(
            "La distance de début "
            "ne peut pas être négative."
        )

    if end_distance > centerline.length:

        print(
            f"Attention : distance de fin trop grande. "
            f"Elle est ramenée à "
            f"{int(centerline.length)} m."
        )

        end_distance = int(centerline.length)

    if start_distance >= end_distance:

        raise ValueError(
            "La distance de début doit être "
            "inférieure à la distance de fin."
        )

    # EXPORT CENTERLINE

    centerline_gdf = gpd.GeoDataFrame(
        {"geometry": [centerline]},
        crs=route_gdf.crs
    )

    centerline_path = os.path.join(
        output_folder,
        f"centerline_{route_number}.gpkg"
    )

    centerline_gdf.to_file(
        centerline_path,
        driver="GPKG"
    )

    print(
        f"Centerline sauvegardée : "
        f"{centerline_path}"
    )

    # SEGMENT CHOISI

    chosen_segment = substring(
        centerline,
        start_distance,
        end_distance
    )

    chosen_segment_gdf = gpd.GeoDataFrame(
        {
            "start_m": [start_distance],
            "end_m": [end_distance],
            "geometry": [chosen_segment]
        },
        crs=route_gdf.crs
    )

    chosen_segment_path = os.path.join(
        output_folder,
        f"chosen_segment_{route_number}.gpkg"
    )

    chosen_segment_gdf.to_file(
        chosen_segment_path,
        driver="GPKG"
    )

    print(
        f"Segment choisi sauvegardé : "
        f"{chosen_segment_path}"
    )
    # LIDAR

    if use_lidar and lidar_data is not None:

        print("Filtrage du nuage LiDAR...")

        buffer_zone = chosen_segment.buffer(100)

        mask = (
            (lidar_data.x > buffer_zone.bounds[0])
            &
            (lidar_data.x < buffer_zone.bounds[2])
            &
            (lidar_data.y > buffer_zone.bounds[1])
            &
            (lidar_data.y < buffer_zone.bounds[3])
        )

        try:

            lidar_data.x = lidar_data.x[mask]
            lidar_data.y = lidar_data.y[mask]
            lidar_data.z = lidar_data.z[mask]

        except:

            lidar_data = lidar_data[mask]

        print(
            f"Points LiDAR gardés : "
            f"{len(lidar_data.x)}"
        )

        coords = np.vstack((
            lidar_data.x,
            lidar_data.y
        )).T

        tree = cKDTree(coords)

    # PERPENDICULAIRES

    distances_on_route = list(range(
        start_distance,
        end_distance + 1,
        spacing
    ))

    perpendicular_lines = []

    print("Calcul des lignes perpendiculaires...")

    for distance in distances_on_route:

        perp_line = calculate_perpendicular_line(
            distance,
            centerline
        )

        perpendicular_lines.append({
            "distance_m": distance,
            "geometry": perp_line
        })

    perp_gdf = gpd.GeoDataFrame(
        perpendicular_lines,
        crs=route_gdf.crs
    )

    perp_path = os.path.join(
        output_folder,
        f"perpendicular_lines_{route_number}.gpkg"
    )

    perp_gdf.to_file(
        perp_path,
        driver="GPKG"
    )

    print(
        f"Lignes perpendiculaires sauvegardées : "
        f"{perp_path}"
    )

    # PROFILS

    print("Création des profils d'altitude...")

    for item in tqdm(
        perpendicular_lines,
        desc="Génération des profils"
    ):

        distance_m = item["distance_m"]
        perp_line = item["geometry"]

        if use_lidar and lidar_data is not None:

            profile_distances, profile_elevations = (
                extract_lidar_profile(
                    perp_line,
                    lidar_data,
                    tree
                )
            )

        else:

            profile_distances, profile_elevations = (
                extract_elevation_profile(
                    perp_line,
                    dem,
                    transform
                )
            )

        save_profile_png(
            profile_distances,
            profile_elevations,
            distance_m,
            output_folder,
            route_number
        )

    print("Traitement terminé.")


if __name__ == "__main__":
    main()
    