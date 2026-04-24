import geopandas as gpd
import os
import pandas as pd
from get_data_functions import get_ponts

class OuvragesSelector:
    def __init__(self, ouvrages_gdf, output_folder, route_number):
        self.ouvrages_gdf = ouvrages_gdf
        self.output_folder = output_folder
        self.filter_route = f"numero='{route_number}'"
        self.ponts_gdf = get_ponts(self.filter_route, "BDTOPO_V3:construction_surfacique")
        self.ponts2_gdf = get_ponts(self.filter_route, "BDTOPO_V3:construction_lineaire")

        # géométrie globale de la route
        route_geom = self.ouvrages_gdf.unary_union

        #  buffer zone autour de la route
        buffer_distance = 10

        #  filtre spatial
        self.ponts_gdf = self.ponts_gdf[self.ponts_gdf.intersects(route_geom.buffer(buffer_distance))]
        self.ponts2_gdf = self.ponts2_gdf[self.ponts2_gdf.intersects(route_geom.buffer(buffer_distance))]

    def merge_close_segments(self, gdf):
            if len(gdf) <= 1:
                return gdf
                
            # Create buffer around segments
            buffered = gdf.geometry.buffer(15)  # 5m buffer to detect 10m gaps
            
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
                            'PR_start': intersecting.iloc[0]['PR_start'],
                            'abcisse_start': intersecting.iloc[0]['abcisse_start'],
                            'PR_end': intersecting.iloc[-1]['PR_end'],
                            'abcisse_end': intersecting.iloc[-1]['abcisse_end'],
                            'length': intersecting.geometry.length.sum(),
                            'hauteur_max': intersecting['hauteur_max'].max(),
                            'pente_max': intersecting['pente_max'].max(),
                            'hauteur_moyenne': intersecting['hauteur_moyenne'].mean(),
                            'pente_moyenne': intersecting['pente_moyenne'].mean(),
                            'route': intersecting.iloc[0]['route']
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
        # Filter the ouvrages with classification "remblai" or "deblai"
        #selected_ouvrages = self.ouvrages_gdf[self.ouvrages_gdf['classification'].isin(['remblai', 'deblai'])]
        selected_ouvrages = self.ouvrages_gdf

        # Remove all zones that overlap with bridges
        selected_ouvrages.loc[:,'geometry'] = selected_ouvrages['geometry'].apply(lambda x: self.remove_overlapping_zones(x, self.ponts_gdf))
        selected_ouvrages.loc[:,'geometry'] = selected_ouvrages['geometry'].apply(lambda x: self.remove_overlapping_zones(x, self.ponts2_gdf))
        selected_ouvrages = selected_ouvrages[~selected_ouvrages.is_empty]
        
        
        # Create separate GeoDataFrames for remblai and deblai
        remblai = selected_ouvrages[selected_ouvrages['classification'] == 'remblai']
        deblai = selected_ouvrages[selected_ouvrages['classification'] == 'deblai']
        rasant = selected_ouvrages[selected_ouvrages['classification'] == 'rasant']
        
        # Merge close segments for each classification
        merged_remblai = self.merge_close_segments(remblai)
        merged_deblai = self.merge_close_segments(deblai)
        merged_rasant = self.merge_close_segments(rasant)
        
        # Combine results
        merged_ouvrages = pd.concat([merged_remblai, merged_deblai, merged_rasant])
        
        # Filter by length
        selected_ouvrages = merged_ouvrages[merged_ouvrages.geometry.length > 20]
        
        return selected_ouvrages
    
    def save_output(self, selected_gdf):
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)

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
