import geopandas as gpd
import os
import pandas as pd
from get_data_functions import get_ponts_from_geom
from openpyxl.styles import Font
from openpyxl.styles import Alignment

class OuvragesSelector:
    def __init__(self, ouvrages_gdf, output_folder, route_number, lines_selected):
        self.ouvrages_gdf = ouvrages_gdf
        self.route = lines_selected
        self.output_folder = output_folder
        self.filter_route = f"numero='{route_number}'"
                # géométrie globale de la route
        route_geom = self.ouvrages_gdf.unary_union
        self.ponts_gdf = get_ponts_from_geom(route_geom, "BDTOPO_V3:construction_surfacique")
        self.ponts2_gdf = get_ponts_from_geom(route_geom, "BDTOPO_V3:construction_lineaire")
        self.rejected_ouvrages=[]


        #  buffer zone autour de la route
        buffer_distance = 10

        #  filtre spatial
        self.ponts_gdf = self.ponts_gdf[self.ponts_gdf.intersects(route_geom.buffer(buffer_distance))]
        self.ponts2_gdf = self.ponts2_gdf[self.ponts2_gdf.intersects(route_geom.buffer(buffer_distance))]

    def merge_close_segments(self, gdf):
            if len(gdf) <= 1:
                return gdf
                
            # Create buffer around segments
            buffered = gdf.geometry.buffer(5)  # 5m buffer to detect 10m gaps
            
            # Dissolve overlapping buffers
            merged = buffered.unary_union
            
            # Convert merged result back to segments
            if merged.geom_type == 'MultiPolygon':
                merged_segments = []
                for polygon in merged.geoms:
                    # Get original segments that intersect with this merged area
                    intersecting = gdf[gdf.geometry.intersects(polygon)]
                    if not intersecting.empty:
                        # Create a new merged segment
                        merged_geom = intersecting.geometry.unary_union
                        merged_segment = {
                            'geometry': merged_geom,
                            'nom': intersecting.iloc[0]['nom'],
                            'classification': intersecting.iloc[0]['classification'],
                            'chaussee':intersecting.iloc[0]['chaussee'],
                            'PR_start': intersecting.iloc[0]['PR_start'],
                            'abcisse_start': intersecting.iloc[0]['abcisse_start'],
                            'PR_end': intersecting.iloc[-1]['PR_end'],
                            'abcisse_end': intersecting.iloc[-1]['abcisse_end'],
                            'length': intersecting.geometry.length.sum(),
                            #'hauteur_max': intersecting['hauteur_max'].max(),
                            #'pente_max': intersecting['pente_max'].max(),
                            #'hauteur_moyenne': intersecting['hauteur_moyenne'].mean(),
                            #'pente_moyenne': intersecting['pente_moyenne'].mean(),
                            'hauteur_talus_max':intersecting['hauteur_talus_max'].max(),
                            'hauteur_talus_moyenne':intersecting['hauteur_talus_moyenne'].mean(),
                            #'hauteur_centre_max':intersecting['hauteur_centre_max'].max(),
                            'hauteur_centre_max':intersecting.loc[intersecting['hauteur_centre_max'].abs().idxmax(),'hauteur_centre_max'],
                            'hauteur_centre_moyenne':intersecting['hauteur_centre_moyenne'].mean(),
                            'pente_max':intersecting['pente_max'].max(),
                            'pente_moyenne':intersecting['pente_moyenne'].mean(),
                            'route': intersecting.iloc[0]['route'],
                            'profil_talus_route':intersecting.iloc[0]['profil_talus_route'],

                            'profil_talus_ouvrage':
                            intersecting.iloc[0]['profil_talus_ouvrage'],

                            'profil_centre_route':
                            intersecting.iloc[0]['profil_centre_route'],

                            'profil_centre_ouvrage':
                            intersecting.iloc[0]['profil_centre_ouvrage'],

                            'profil_pente_route':
                            intersecting.iloc[0]['profil_pente_route'],

                            'profil_pente_ouvrage':
                            intersecting.iloc[0]['profil_pente_ouvrage'],

                            'profil_talus_id':
                            intersecting.iloc[0]['profil_talus_id'],

                            'profil_centre_id':
                            intersecting.iloc[0]['profil_centre_id'],

                            'profil_pente_id':
                            intersecting.iloc[0]['profil_pente_id'],
                            'talus_dist_min':
                            intersecting.iloc[0]['talus_dist_min'],

                            'talus_alt_min':
                            intersecting.iloc[0]['talus_alt_min'],

                            'talus_dist_max':
                            intersecting.iloc[0]['talus_dist_max'],

                            'talus_alt_max':
                            intersecting.iloc[0]['talus_alt_max'],
                                                    }
                        merged_segments.append(merged_segment)
                
                if merged_segments:
                    result = gpd.GeoDataFrame(merged_segments, crs=gdf.crs)
                    for col in result.columns:
                        if col in gdf.columns:
                            result[col] = result[col].astype(gdf[col].dtype)
                    return result
                
            return gdf

    def remove_overlapping_zones(self, linestring, zones_a_filtrer, buffer_distance=10):
        for element in zones_a_filtrer.geometry:
            if element.geom_type == 'MultiPolygon':
                element = element.buffer(buffer_distance)
                linestring = linestring.difference(element)
            elif element.geom_type == 'MultiLineString':
                element = element.buffer(buffer_distance)
                linestring = linestring.difference(element)
            elif element.geom_type == "LineString":
                element = element.buffer(buffer_distance)
                linestring = linestring.difference(element)
            else:
                raise ValueError(f"Unsupported geometry type: {element.geom_type}")

        return linestring

    def select_ouvrages(self):
    
        #selected_ouvrages=self.ouvrages_gdf[self.ouvrages_gdf['classification'].isin(['remblai','deblai'])]
        selected_ouvrages=self.ouvrages_gdf.copy()

        # Remove all zones that overlap with bridges
        selected_ouvrages.loc[:,'geometry']=selected_ouvrages['geometry'].apply(lambda x:self.remove_overlapping_zones(x,self.ponts_gdf))

        selected_ouvrages.loc[:,'geometry']=selected_ouvrages['geometry'].apply(lambda x:self.remove_overlapping_zones(x,self.ponts2_gdf))
        for _,row in selected_ouvrages.iterrows():

            if row.geometry.is_empty:

                self.rejected_ouvrages.append({

                    "nom":row["nom"],

                    "PR_start":row["PR_start"],

                    "PR_end":row["PR_end"],

                    "classification":row["classification"],

                    "chaussee":row["chaussee"],

                    "raison_rejet":"supprime_par_pont"

                })
        selected_ouvrages=selected_ouvrages[~selected_ouvrages.is_empty]

        # Create separate GeoDataFrames
        remblai=selected_ouvrages[selected_ouvrages['classification']=="remblai"]
        deblai=selected_ouvrages[selected_ouvrages['classification']=="deblai"]
        rasant=selected_ouvrages[selected_ouvrages['classification']=="rasant"]

        merged_results=[]

        for classe_gdf in [remblai,deblai,rasant]:

            for chaussee in ["D","G"]:

                subset=classe_gdf[
                    classe_gdf["chaussee"]==chaussee
                ]

                if not subset.empty:

                    merged_results.append(
                        self.merge_close_segments(subset)
                    )

        if not merged_results:

            return gpd.GeoDataFrame(
                crs=selected_ouvrages.crs
            )

        merged_ouvrages=pd.concat(
            merged_results,
            ignore_index=True
        )

        
        merged_ouvrages = merged_ouvrages.drop_duplicates(
            subset=[
                "classification",
                "chaussee",
                "PR_start",
                "abcisse_start",
                "PR_end",
                "abcisse_end"
            ]
        ).reset_index(drop=True)
        

        rejected_length=merged_ouvrages[
            merged_ouvrages.geometry.length<=20
        ]

        for _,row in rejected_length.iterrows():

            self.rejected_ouvrages.append({

                "nom":row["nom"],

                "PR_start":row["PR_start"],

                "PR_end":row["PR_end"],

                "classification":row["classification"],

                "chaussee":row["chaussee"],

                "raison_rejet":"longueur<20m"

            })

        selected_ouvrages=merged_ouvrages[
            merged_ouvrages.geometry.length>20
        ].copy()

        selected_ouvrages["PR_num"]=selected_ouvrages["PR_start"].astype(str).str.extract(r'PR(\d+)')[0].astype(int)

        selected_ouvrages["ordre_chaussee"]=selected_ouvrages["PR_start"].astype(str).str[-1].map({
            "D":0,
            "G":1
        })

        selected_ouvrages=selected_ouvrages.sort_values(
            [
                "ordre_chaussee",
                "PR_num",
                "abcisse_start",
                "abcisse_end"
            ]
        ).drop(
            columns=[
                "ordre_chaussee",
                "PR_num"
            ]
        ).reset_index(drop=True)

        pd.DataFrame(
            self.rejected_ouvrages
        ).to_csv(

            os.path.join(
                self.output_folder,
                "rejected_ouvrages.csv"
            ),

            index=False,
            sep=";"
        )

        return selected_ouvrages
    
    """#   
    
    def save_output(self, selected_gdf):
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)

        selected_gdf=selected_gdf.copy()

        cols_to_round=[
            "length",
            "hauteur_max",
            "pente_max",
            "hauteur_moyenne",
            "pente_moyenne"
        ]

        for col in cols_to_round:
            if col in selected_gdf.columns:
                selected_gdf[col]=selected_gdf[col].round(3)
                selected_gdf[col]=selected_gdf[col].astype(float)


        output_file = os.path.join(self.output_folder, "selected_ouvrages.gpkg")
        selected_gdf.to_file(output_file, driver='GPKG', layer='ouvrages') 
        # ponts surface 
        self.ponts_gdf.to_file(
            os.path.join(self.output_folder, "ponts_surface.gpkg"),
            driver='GPKG'
        )

        #  ponts lineaire 
        self.ponts2_gdf.to_file(
            os.path.join(self.output_folder, "ponts_lineaire.gpkg"),
            driver='GPKG'
        )
    """

    def save_output(self, selected_gdf):

        # Créer le dossier si nécessaire
        os.makedirs(self.output_folder, exist_ok=True)

        selected_gdf = selected_gdf.copy()

        # Colonnes numériques à arrondir
        cols_to_round = [
            "length",
            "hauteur_talus_max",
            "talus_dist_min",
            "talus_alt_min",
            "talus_dist_max",
            "talus_alt_max",
            "hauteur_talus_moyenne",
            "hauteur_centre_max",
            "hauteur_centre_moyenne",
            "pente_max",
            "pente_moyenne",
            "abcisse_start",
            "abcisse_end"
        ]

        for col in cols_to_round:
            if col in selected_gdf.columns:
                selected_gdf[col] = (
                    selected_gdf[col]
                    .astype(float)
                    .round(2)
                )

        # Sauvegarde GPKG
        output_file = os.path.join(
            self.output_folder,
            "selected_ouvrages.gpkg"
        )

        selected_gdf.to_file(
            output_file,
            driver='GPKG',
            layer='ouvrages'
        )

        # EXPORT EXCEL
        excel_df = selected_gdf.drop(
            columns=["geometry"],
            errors="ignore"
        )

        colonnes = [
            "nom",
            "classification",
            #"chaussee",
            "PR_start",
            "abcisse_start",
            "PR_end",
            "abcisse_end",
            "length",
            #"hauteur_max",
            #"hauteur_moyenne",
            "hauteur_talus_max",
            #"profil_talus_ouvrage",
            #"profil_pente_ouvrage",
            "hauteur_talus_moyenne",
            "hauteur_centre_max",
            #"profil_centre_ouvrage",
            #"profil_centre_route",
            "hauteur_centre_moyenne",
            "pente_max",
            #"profil_talus_route",
            #"profil_pente_route",
            "pente_moyenne",
            #"profil_talus_id",
            #"profil_centre_id",
            #"profil_pente_id",
            #"route"
        ]

        excel_df["PR_start"] = (
            excel_df["PR_start"]
            .astype(str)
            .str.extract(r"PR(\d+)")[0]
        )

        excel_df["PR_end"] = (
            excel_df["PR_end"]
            .astype(str)
            .str.extract(r"PR(\d+)")[0]
        )

        colonnes_existantes = [
            c for c in colonnes
            if c in excel_df.columns
        ]

        excel_df = excel_df[
            colonnes_existantes
        ]

        excel_path = os.path.join(
            self.output_folder,
            "selected_ouvrages.xlsx"
        )

        with pd.ExcelWriter(
            excel_path,
            engine='openpyxl'
        ) as writer:

            excel_df.to_excel(
                writer,
                sheet_name='Ouvrages',
                index=False
            )

            worksheet = writer.sheets[
                'Ouvrages'
            ]
            # Style titres
            for cell in worksheet[1]:

                cell.font = Font(
                    bold=True
                )

                cell.alignment = Alignment(
                    horizontal='center',
                    vertical='center'
                )

            # Ajuster largeur automatique
            for column_cells in worksheet.columns:

                max_length = 0

                column = (
                    column_cells[0]
                    .column_letter
                )

                for cell in column_cells:

                    try:
                        if cell.value:

                            max_length = max(
                                max_length,
                                len(str(cell.value))
                            )

                    except:
                        pass

                # largeur plus confortable
                adjusted_width = (
                    (max_length + 4)
                    * 1.5
                )

                adjusted_width = max(
                    15,
                    min(
                        adjusted_width,
                        35
                    )
                )

                worksheet.column_dimensions[
                    column
                ].width = adjusted_width

            # Format nombres
            for col in worksheet.columns:

                for cell in col:

                    if isinstance(
                        cell.value,
                        (int, float)
                    ):

                        cell.number_format = (
                            '0.00'
                        )

                        cell.alignment = Alignment(
                            horizontal='center'
                        )

            # Figer première ligne
            worksheet.freeze_panes = 'A2'

        print(
            f"Excel sauvegardé : {excel_path}"
        )
        # Sauvegarde ponts
        self.ponts_gdf.to_file(
            os.path.join(
                self.output_folder,
                "ponts_surface.gpkg"
            ),
            driver='GPKG'
        )

        self.ponts2_gdf.to_file(
            os.path.join(
                self.output_folder,
                "ponts_lineaire.gpkg"
            ),
            driver='GPKG'
        )