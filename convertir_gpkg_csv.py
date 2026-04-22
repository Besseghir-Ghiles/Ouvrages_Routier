import geopandas as gpd
import pandas as pd

# Charger le fichier GPKG
gpkg_file = 'output_A330/selected_ouvrages.gpkg'
gdf = gpd.read_file(gpkg_file)

# Convertir en DataFrame Pandas
df = pd.DataFrame(gdf)

# Sauvegarder en CSV
csv_file = 'ouvrages_A330_en_csv.csv'
df.to_csv(csv_file, index=False)

print(f"Le fichier a été converti et sauvegardé sous {csv_file}")
