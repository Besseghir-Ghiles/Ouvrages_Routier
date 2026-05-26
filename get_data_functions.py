import requests
import geopandas as gpd
from shapely.geometry import shape, box

def get_data(filter, type_of_data, bbox):
    """
    Fetches data from the WFS service
    """
    url = "https://data.geopf.fr/wfs/ows"

    params = {
        "SERVICE": "WFS",
        "REQUEST": "GetFeature",
        "VERSION": "2.0.0",
        "TYPENAMES": type_of_data,
        "OUTPUTFORMAT": "application/json",
        "CQL_FILTER": filter,
        "SRSNAME": "EPSG:2154"  # Lambert-93
    }
    """ 
    # ajouter le filtre seulement si présent
    if filter:
        params["CQL_FILTER"] = filter

    #if type_of_data == "BDTOPO_V3:troncon_de_route":
        #params.pop("BBOX", None)

                # filtre directement côté IGN
    #if bbox:
    if bbox:

        minx, miny, maxx, maxy = bbox
        margin = 500

        params["BBOX"] = (
            f"{minx},"
            f"{miny},"
            f"{maxx},"
            f"{maxy}"
        )
    
        print("BBOX envoyée au serveur IGN:")
        print(repr(params["BBOX"]))
    
    print(params)
    """

    response = requests.get(url, params=params)

    if response.status_code == 200:
        try:
            content = response.json()
            
            # Create GeoDataFrame from the GeoJSON
            gdf = gpd.GeoDataFrame.from_features(content['features'])            
            # Explicitly set the CRS to Lambert-93
            gdf.set_crs(epsg=2154, inplace=True)

            # Use bounding box to filter relevant sections
            print(f"Bounding box: {bbox}")
            save_bbox_as_geopackage(bbox, "bounding_box1.gpkg")
            print(f"GeoDataFrame bounds: {gdf.total_bounds}")
            """
            if bbox:
                 
                print(f"Bounding box: {bbox}")
                minx, miny, maxx, maxy = bbox
                minx += 100
                miny += 100
                maxx -= 100
                maxy -= 100
                bbox_geom = box(minx, miny, maxx, maxy)
                gdf = gpd.clip(gdf, bbox_geom)
                print(f"Filtered GeoDataFrame bounds: {gdf.total_bounds}")
            """  
            if bbox:
                print("\n===== DEBUG BBOX =====")
                print("BBox originale:", bbox)

                minx, miny, maxx, maxy = bbox
                 
                minx += 100
                miny += 100
                maxx -= 100
                maxy -= 100
                
                print("BBox modifiée:", (minx, miny, maxx, maxy))
                if minx >= maxx or miny >= maxy:
                    print(" BBOX invalide après réduction  on annule le clip")
                else:

                    print("\n===== DEBUG CLIP =====")
                    print("Nombre AVANT clip:", len(gdf))
                    print("Bounds AVANT:", gdf.total_bounds)

                    bbox_geom = box(minx, miny, maxx, maxy)

                    gdf = gpd.clip(gdf, bbox_geom)

                    print("Nombre APRÈS clip:", len(gdf))
                    print("Bounds APRÈS:", gdf.total_bounds)
                    print("Empty ?", gdf.empty)

                    print("\n===== DEBUG VALIDITÉ =====")
                    print("Géométries valides ?", gdf.is_valid.all())
                    save_bbox_as_geopackage(gdf.total_bounds, "bounding_box2.gpkg")
                

            return gdf
            
        except requests.exceptions.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print(f"Request failed with status code: {response.status_code}")

    return None

def get_ponts_from_geom(route_geom, type_of_data):
    url = "https://data.geopf.fr/wfs/ows"

    buffer = route_geom.buffer(1000)
    minx, miny, maxx, maxy = buffer.bounds

    params = {
        "SERVICE": "WFS",
        "REQUEST": "GetFeature",
        "VERSION": "2.0.0",
        "TYPENAMES": type_of_data,
        "OUTPUTFORMAT": "application/json",
        "BBOX": f"{minx},{miny},{maxx},{maxy},EPSG:2154",
        "SRSNAME": "EPSG:2154"
    }

    response = requests.get(url, params=params)
    print("Bridge status:", response.status_code)

    if response.status_code != 200:
        return None

    data = response.json()
    gdf = gpd.GeoDataFrame.from_features(data["features"])
    gdf.set_crs(epsg=2154, inplace=True)

    return gdf

def get_mnt(bbox_values, data_mnt):
        url_raster = "https://data.geopf.fr/wms-r"

        # Calculate width and height maintaining aspect ratio
        minx, miny, maxx, maxy = [float(x) for x in bbox_values]
        bbox_width = maxx - minx
        bbox_height = maxy - miny
        if bbox_height / bbox_width < 1:
            target_width = 2048  # pixels
            target_height = int((bbox_height / bbox_width) * target_width)
        else:
            target_height = 2048
            target_width = int((bbox_width / bbox_height) * target_height)

        params_mnt = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "VERSION": "1.3.0",
            "LAYERS": data_mnt,
            "FORMAT": "image/geotiff",
            "CRS": "EPSG:2154",  # Lambert-93
            "BBOX": f"{minx},{miny},{maxx},{maxy}",
            "WIDTH": target_width,
            "HEIGHT": target_height,
            "STYLES": ""  # Required empty parameter
        }

        response_mnt = requests.get(url_raster, params=params_mnt)
        print(f"MNT response status: {response_mnt.status_code}")
        
        if response_mnt.status_code == 200:
            # Save the GeoTIFF file
            with open("output_mnt.tif", "wb") as f:
                f.write(response_mnt.content)
            print("Saved DEM to output_dem.tif")
        else:
            print(f"MNT request failed with status code: {response_mnt.status_code}")
            print(f"Response content: {response_mnt.text}")

def save_bbox_as_geopackage(bbox, output_path):
    """
    Save the bounding box as a polygon in a GeoPackage.
    bbox: (minx, miny, maxx, maxy)
    output_path: path to the output .gpkg file
    """
    minx, miny, maxx, maxy = bbox
    polygon = box(minx, miny, maxx, maxy)
    gdf = gpd.GeoDataFrame({'geometry': [polygon]}, crs="EPSG:2154")
    gdf.to_file(output_path, driver="GPKG")