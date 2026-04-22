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
                save_bbox_as_geopackage(gdf.total_bounds, "bounding_box2.gpkg")

            return gdf
            
        except requests.exceptions.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print(f"Request failed with status code: {response.status_code}")

    return None

def get_ponts(filter, type_of_data):
    """
    Fetch bridges from the WFS service
    """
    url = "https://data.geopf.fr/wfs/ows"

    # Get road geometry
    road_params = {
        "SERVICE": "WFS",
        "REQUEST": "GetFeature",
        "VERSION": "2.0.0",
        "TYPENAMES": "BDTOPO_V3:route_numerotee_ou_nommee",
        "OUTPUTFORMAT": "application/json",
        "CQL_FILTER": filter,
        "SRSNAME": "EPSG:2154"
    }

    road_response = requests.get(url, params=road_params)
    print(f"Road response status: {road_response.status_code}")

    if road_response.status_code == 200:
        try:
            # Get first geometry and create buffer
            road_content = road_response.json()
            first_geometry = shape(road_content['features'][0]['geometry'])
            buffer = first_geometry.buffer(1000)  # 1km buffer
            minx, miny, maxx, maxy = buffer.bounds
            print(f"Buffer bounds: minX={minx:.2f}, minY={miny:.2f}, maxX={maxx:.2f}, maxY={maxy:.2f}")

            # Create correct polygon syntax for CQL filter
            my_filter = f"{minx}, {miny}, {maxx}, {maxy}, EPSG:2154"

            # Search for bridges within buffer
            bridge_params = {
                "SERVICE": "WFS",
                "REQUEST": "GetFeature",
                "VERSION": "2.0.0",
                "TYPENAMES": type_of_data,
                "OUTPUTFORMAT": "application/json",
                "bbox": my_filter,
                "SRSNAME": "EPSG:2154"
            }
            
            bridge_response = requests.get(url, params=bridge_params)
            print(f"Bridge response status: {bridge_response.status_code}")
            print(f"Bridge response URL: {bridge_response.url}")
            
            if bridge_response.status_code == 200:
                try:
                    content = bridge_response.json()
                    
                    # Create GeoDataFrame from the GeoJSON
                    gdf = gpd.GeoDataFrame.from_features(content['features'])
                    
                    # Explicitly set the CRS to Lambert-93
                    gdf.set_crs(epsg=2154, inplace=True)

                    # Select the features with nature 'Pont'
                    gdf = gdf[gdf['nature'] == 'Pont']

                    return gdf
            
                except requests.exceptions.JSONDecodeError as e:
                    print(f"Failed to parse JSON: {e}")
                except Exception as e:
                    print(f"An error occurred: {e}")
            else:
                print(f"Bridge request failed with status code: {bridge_response.status_code}")
                    
        except requests.exceptions.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
            print(f"Error type: {type(e)}")
    else:
        print(f"Initial road request failed with status code: {road_response.status_code}")

    return None

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