import os
import math
import rasterio
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt

from shapely.geometry import LineString
from sklearn.linear_model import LinearRegression
import pandas as pd


class DrawSelectedProfiles:
    def __init__(self, route_number, output_folder, mnt_path="data/mnt.tif"):
        self.route_number = route_number
        self.output_folder = output_folder
        self.mnt_path = mnt_path

        with rasterio.open(self.mnt_path) as src:
            self.dem = src.read(1)
            self.transform = src.transform

        self.selected_path = os.path.join(
            output_folder,
            "selected_ouvrages.gpkg"
        )

        self.lines_path = os.path.join(
            output_folder,
            "lines_selected.gpkg"
        )

        self.centerline_path = os.path.join(
            output_folder,
            "central_line_profil.gpkg"
        )

        self.out_dir = os.path.join(
            output_folder,
            "profils_selected_ouvrages"
        )

        os.makedirs(self.out_dir, exist_ok=True)

        self.selected = gpd.read_file(
            self.selected_path,
            layer="ouvrages"
        )

        self.classified_profiles = gpd.read_file(
            os.path.join(
                output_folder,
                "classified_profiles.gpkg"
            ),
            layer="points"
        )

        self.perpendicular_profiles = gpd.read_file(
            os.path.join(
                output_folder,
                "perpendicular_profil.gpkg"
            )
)

        with open(
            os.path.join(output_folder, "debug_classified_profiles.txt"),
            "w",
            encoding="utf-8"
        ) as f:

            f.write("===== COLONNES =====\n")
            f.write(
                str(self.classified_profiles.columns.tolist())
            )

            f.write("\n\n===== 20 PREMIERES LIGNES =====\n")
            f.write(
                self.classified_profiles.head(20).to_string()
            )

        self.lines_selected = gpd.read_file(
            self.lines_path
        )

        self.centerline = gpd.read_file(
            self.centerline_path,
            layer="central_line"
        ).iloc[0].geometry

    def get_raster_value(self, point):
        row, col = rasterio.transform.rowcol(
            self.transform,
            point.x,
            point.y
        )

        if (
            0 <= row < self.dem.shape[0]
            and 0 <= col < self.dem.shape[1]
        ):
            return self.dem[row, col]

        return None

    def calculate_distance(self, p1, p2):
        return math.sqrt(
            (p2.x - p1.x) ** 2
            + (p2.y - p1.y) ** 2
        )

    def calculate_slope(self, p1, p2):
        z1 = self.get_raster_value(p1)
        z2 = self.get_raster_value(p2)

        if z1 is None or z2 is None:
            return None

        dist = self.calculate_distance(p1, p2)

        if dist == 0:
            return None

        return abs(z2 - z1) / dist

    def determine_routewidth(self, nearest_line):
        ref_route_start = 57
        ref_route_end = 63

        if nearest_line["nombre_de_voies"] == 2:
            ref_terrain_start1 = 0
            ref_terrain_end1 = 30
            ref_terrain_start2 = 90
            ref_terrain_end2 = 120
        else:
            ref_terrain_start1 = 0
            ref_terrain_end1 = 25
            ref_terrain_start2 = 95
            ref_terrain_end2 = 120

        return (
            ref_route_start,
            ref_route_end,
            ref_terrain_start1,
            ref_terrain_end1,
            ref_terrain_start2,
            ref_terrain_end2
        )

    def calculate_perpendicular_line(self, point_on_center):
        distances = self.lines_selected.geometry.distance(
            point_on_center
        )

        nearest_line = self.lines_selected.iloc[
            distances.idxmin()
        ]

        line = nearest_line.geometry

        if line.geom_type == "MultiLineString":
            line = list(line.geoms)[0]

        d = line.project(point_on_center)

        current_point = line.interpolate(d)

        if d <= 15:
            next_point = line.interpolate(d + 10)
            angle = math.atan2(
                next_point.y - current_point.y,
                next_point.x - current_point.x
            )
        else:
            prev_point = line.interpolate(d - 10)
            angle = math.atan2(
                current_point.y - prev_point.y,
                current_point.x - prev_point.x
            )

        dx = 60 * math.cos(angle + math.pi / 2)
        dy = 60 * math.sin(angle + math.pi / 2)

        start = (
            current_point.x - dx,
            current_point.y - dy
        )

        end = (
            current_point.x + dx,
            current_point.y + dy
        )

        return LineString([start, end]), nearest_line

    def average_height(self, perpendicular_line, start, end):
        values = []

        for d in range(start, end + 1):
            p = perpendicular_line.interpolate(d)
            z = self.get_raster_value(p)

            if z is not None:
                values.append(z)

        if not values:
            return None

        return sum(values) / len(values)

    def calculate_regression(
        self,
        perpendicular_line,
        start1,
        end1,
        start2,
        end2
    ):
        x = []
        y = []

        start_point = perpendicular_line.interpolate(0)

        for d in list(range(start1, end1 + 1)) + list(range(start2, end2 + 1)):
            p = perpendicular_line.interpolate(d)
            z = self.get_raster_value(p)

            if z is not None:
                x.append(
                    self.calculate_distance(start_point, p)
                )
                y.append(z)

        if not x:
            return None

        reg = LinearRegression().fit(
            np.array(x).reshape(-1, 1),
            np.array(y).reshape(-1, 1)
        )

        return reg

    def interpolated_altitude(self, distance, reg):
        if reg is None:
            return None

        return reg.predict(
            np.array([distance]).reshape(-1, 1)
        )[0][0]
    
    def calculate_perpendicular_line_from_line(
        self,
        current_distance,
        line
    ):

        current_point = line.interpolate(
            current_distance
        )

        if current_distance <= 15:

            next_point = line.interpolate(
                current_distance + 10
            )

            angle = math.atan2(
                next_point.y - current_point.y,
                next_point.x - current_point.x
            )

        else:

            prev_point = line.interpolate(
                current_distance - 10
            )

            angle = math.atan2(
                current_point.y - prev_point.y,
                current_point.x - prev_point.x
            )

        dx = 60 * math.cos(
            angle + math.pi / 2
        )

        dy = 60 * math.sin(
            angle + math.pi / 2
        )

        start = (
            current_point.x - dx,
            current_point.y - dy
        )

        end = (
            current_point.x + dx,
            current_point.y + dy
        )

        return LineString([start, end])


    def draw_one_profile(self, row, profile_kind, profile_id):

        if pd.isna(profile_id):
            return

        try:

            print("profile_id demandé :", profile_id)

            profile_row = self.classified_profiles.loc[
                int(profile_id)
            ]

            center_distance = profile_row[
                "center_distance"
            ]
            print(
                "profile_id =", profile_id,
                "center_distance =", center_distance
            )

            distance_profil = profile_row["distance_profil"]

            perpendicular_row = self.perpendicular_profiles[
                self.perpendicular_profiles["distance"] == distance_profil
            ].iloc[0]

            average_route = profile_row[
                "average_height_route"
            ]

            z_tn_center = profile_row[
                "interpolated_height_nat_terrain_route"
            ]

            coef = profile_row[
                "reg_coef"
            ]

            intercept = profile_row[
                "reg_intercept"
            ]
        except:

            with open(
                os.path.join(
                    self.output_folder,
                    "debug_draw.txt"
                ),
                "a",
                encoding="utf-8"
            ) as f:

                f.write(
                    f"\nprofile_id demandé={profile_id}"
                    f"\nindex min={self.classified_profiles.index.min()}"
                    f"\nindex max={self.classified_profiles.index.max()}"
                    f"\ncolonnes={list(self.classified_profiles.columns)}"
                    f"\n----------------------"
                )
            
            print(
                f"Profil introuvable : {profile_id}"
            )
            return

        perpendicular_line = perpendicular_row.geometry

        distances = []
        elevations = []

        for d in range(
            0,
            int(perpendicular_line.length) + 1
        ):

            p = perpendicular_line.interpolate(d)

            z = self.get_raster_value(p)

            if z is not None:

                distances.append(d)

                elevations.append(z)

        if not elevations:
            return

        

        reg_y = [
            coef * d + intercept
            for d in distances
        ]

        plt.figure(figsize=(12, 6))

        plt.plot(
            distances,
            elevations,
            marker="o",
            linewidth=2,
            label="Profil terrain réel"
        )

        if reg_y is not None:

            plt.plot(
                distances,
                reg_y,
                linestyle="--",
                linewidth=2,
                label="Terrain naturel par régression"
            )
        
        plt.axvline(
            x=center_distance,
            linestyle=":",
            linewidth=2,
            label="Centre route"
        )


        if average_route is not None:

            plt.scatter(
                [center_distance],
                [average_route],
                s=80,
                label="Altitude moyenne route"
            )

        if z_tn_center is not None:

            plt.scatter(
                [center_distance],
                [z_tn_center],
                s=80,
                label="TN au centre"
            )

        plt.axhline(
            y=average_route,
            linestyle=":",
            linewidth=1,
            label=f"Route centre = {average_route:.2f}"
        )

        plt.axhline(
            y=z_tn_center,
            linestyle=":",
            linewidth=1,
            label=f"TN centre = {z_tn_center:.2f}"
        )

        title = (
            f"{self.route_number}"
            f"_PR{int(row['profil_talus_PR'])}"
            f"+{int(round(row['profil_talus_abcisse'], -1))}"
            #f"_{row['chaussee']}"
            f" | {profile_kind}"
            #f"{row['nom']} | {profile_kind}\n"
            f"{row['PR_start']}+{row['abcisse_start']} → "
            f"{row['PR_end']}+{row['abcisse_end']}\n"
            f"Classe={row['classification']} | "
            f"H talus={row['hauteur_talus_max']:.2f} m | "
            #f"H centre={row['hauteur_centre_max']:.2f} m | "
            #f"Pente max={row['pente_max']:.2f}"
        )
        # VISUALISATION TALUS
        if profile_kind == "talus_max":

            # TALUS GAUCHE
            if pd.notna(row["left_dist_min"]):

                plt.scatter(
                    [row["left_dist_min"]],
                    [row["left_alt_min"]],
                    s=180,
                    marker="o",
                    #label="Talus gauche bas"
                )

                plt.scatter(
                    [row["left_dist_max"]],
                    [row["left_alt_max"]],
                    s=180,
                    marker="o",
                    #label="Talus gauche haut"
                )

                plt.plot(
                    [row["left_dist_min"], row["left_dist_max"]],
                    [row["left_alt_min"], row["left_alt_max"]],
                    linewidth=4,
                    label="Talus gauche"
                )

            # TALUS DROIT
            if pd.notna(row["right_dist_min"]):

                plt.scatter(
                    [row["right_dist_min"]],
                    [row["right_alt_min"]],
                    s=180,
                    marker="s",
                    #label="Talus droit bas"
                )

                plt.scatter(
                    [row["right_dist_max"]],
                    [row["right_alt_max"]],
                    s=180,
                    marker="s",
                    #label="Talus droit haut"
                )

                plt.plot(
                    [row["right_dist_min"], row["right_dist_max"]],
                    [row["right_alt_min"], row["right_alt_max"]],
                    linewidth=4,
                    label="Talus droit"
                )

            plt.title(title)

        plt.xlabel(
            "Distance sur le profil transversal (m)"
        )

        plt.ylabel(
            "Altitude (m)"
        )

        plt.grid(True)

        plt.legend(fontsize=8)

        plt.tight_layout()

        safe_name = (
            str(row["nom"])
            .replace("/", "_")
            .replace("\\", "_")
        )

        filename = (
            f"{row.name:03d}_"
            f"{safe_name}_"
            f"{profile_kind}.png"
        )

        output_path = os.path.join(
            self.out_dir,
            filename
        )

        plt.savefig(
            output_path,
            dpi=200
        )

        plt.close()

    def generate(self, max_ouvrages=None):
        count = 0

        for idx, row in self.selected.iterrows():
            if max_ouvrages is not None and count >= max_ouvrages:
                break

            row.name = idx

            self.draw_one_profile(
                row,
                "talus_max",
                row["profil_talus_id"]
            )


            count += 1

        print(
            f"Profils dessinés dans : {self.out_dir}"
        )