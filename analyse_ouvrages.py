import geopandas as gpd
import webbrowser
import os
import matplotlib.pyplot as plt

# Demander le code de la route à l'utilisateur
route = input("Saisir le code de la route (ex. A33): ")

# Charger le fichier GeoPackage
input_file = f"output_{route}/selected_ouvrages.gpkg"
gdf = gpd.read_file(input_file)

# Split en remblai, deblai et rasant
remblai = gdf[gdf['classification'] == 'remblai']
deblai = gdf[gdf['classification'] == 'deblai']
rasant = gdf[gdf['classification'] == 'rasant']

# Statistiques générales tout
total_ouvrage = round(len(gdf), 2)
total_length = round(gdf['length'].sum(), 2)

# Statistiques sur la longueur tout
length_max = round(gdf['length'].max(), 2)
length_min = round(gdf['length'].min(), 2)
length_mean = round(gdf['length'].mean(), 2)
length_median = round(gdf['length'].median(), 2)

# Statistiques générales remblai
total_ouvrage_remblai = round(len(remblai), 2)
total_length_remblai = round(remblai['length'].sum(), 2)

# Statistiques sur la longueur remblai
length_max_remblai = round(remblai['length'].max(), 2)
length_min_remblai = round(remblai['length'].min(), 2)
length_mean_remblai = round(remblai['length'].mean(), 2)
length_median_remblai = round(remblai['length'].median(), 2)

# Statistiques sur hauteur_moyenne remblai
hauteur_moyenne_max_remblai = round(remblai['hauteur_moyenne'].max(), 2)
hauteur_moyenne_min_remblai = round(remblai['hauteur_moyenne'].min(), 2)
hauteur_moyenne_mean_remblai = round(remblai['hauteur_moyenne'].mean(), 2)
hauteur_moyenne_median_remblai = round(remblai['hauteur_moyenne'].median(), 2)

# Statistique sur hauteur_max remblai
hauteur_max_max_remblai = round(remblai['hauteur_max'].max(), 2)
hauteur_max_min_remblai = round(remblai['hauteur_max'].min(), 2)
hauteur_max_mean_remblai = round(remblai['hauteur_max'].mean(), 2)
hauteur_max_median_remblai = round(remblai['hauteur_max'].median(), 2)

# Statistiques sur pente_moyenne remblai
pente_moyenne_max_remblai = round(remblai['pente_moyenne'].max(), 2)
pente_moyenne_min_remblai = round(remblai['pente_moyenne'].min(), 2)
pente_moyenne_mean_remblai = round(remblai['pente_moyenne'].mean(), 2)
pente_moyenne_median_remblai = round(remblai['pente_moyenne'].median(), 2)

# Statistique sur pente_max remblai
pente_max_max_remblai = round(remblai['pente_max'].max(), 2)
pente_max_min_remblai = round(remblai['pente_max'].min(), 2)
pente_max_mean_remblai = round(remblai['pente_max'].mean(), 2)
pente_max_median_remblai = round(remblai['pente_max'].median(), 2)

# Statistiques générales deblai
total_ouvrage_deblai = round(len(deblai), 2)
total_length_deblai = round(deblai['length'].sum(), 2)

# Statistiques sur la longueur deblai
length_max_deblai = round(deblai['length'].max(), 2)
length_min_deblai = round(deblai['length'].min(), 2)
length_mean_deblai = round(deblai['length'].mean(), 2)
length_median_deblai = round(deblai['length'].median(), 2)

# Statistiques sur hauteur_moyenne deblai
hauteur_moyenne_max_deblai = round(deblai['hauteur_moyenne'].max(), 2)
hauteur_moyenne_min_deblai = round(deblai['hauteur_moyenne'].min(), 2)
hauteur_moyenne_mean_deblai = round(deblai['hauteur_moyenne'].mean(), 2)
hauteur_moyenne_median_deblai = round(deblai['hauteur_moyenne'].median(), 2)

# Statistique sur hauteur_max deblai
hauteur_max_max_deblai = round(deblai['hauteur_max'].max(), 2)
hauteur_max_min_deblai = round(deblai['hauteur_max'].min(), 2)
hauteur_max_mean_deblai = round(deblai['hauteur_max'].mean(), 2)
hauteur_max_median_deblai = round(deblai['hauteur_max'].median(), 2)

# Statistiques sur pente_moyenne deblai
pente_moyenne_max_deblai = round(deblai['pente_moyenne'].max(), 2)
pente_moyenne_min_deblai = round(deblai['pente_moyenne'].min(), 2)
pente_moyenne_mean_deblai = round(deblai['pente_moyenne'].mean(), 2)
pente_moyenne_median_deblai = round(deblai['pente_moyenne'].median(), 2)

# Statistique sur pente_max deblai
pente_max_max_deblai = round(deblai['pente_max'].max(), 2)
pente_max_min_deblai = round(deblai['pente_max'].min(), 2)
pente_max_mean_deblai = round(deblai['pente_max'].mean(), 2)
pente_max_median_deblai = round(deblai['pente_max'].median(), 2)

# Statistiques générales rasant
total_ouvrage_rasant = round(len(rasant), 2)
total_length_rasant = round(rasant['length'].sum(), 2)

# Statistiques sur la longueur rasant
length_max_rasant = round(rasant['length'].max(), 2)
length_min_rasant = round(rasant['length'].min(), 2)
length_mean_rasant = round(rasant['length'].mean(), 2)
length_median_rasant = round(rasant['length'].median(), 2)

# nombre d'ouvrages en remblai avec une hauteur moyenne de plus de 10m, 5-10m ou moins de 5m
remblai_10m = remblai[remblai['hauteur_moyenne'] >= 10].shape[0]
remblai_5_10m = remblai[remblai['hauteur_moyenne'].between(5, 10)].shape[0]
remblai_5m = remblai[remblai['hauteur_moyenne'] < 5].shape[0]

# nombre d'ouvrages en déblai avec une hauteur moyenne de plus de 10m, 5-10m ou moins de 5m
deblai_10m = deblai[deblai['hauteur_moyenne'] >= 10].shape[0]
deblai_5_10m = deblai[deblai['hauteur_moyenne'].between(5, 10)].shape[0]
deblai_5m = deblai[deblai['hauteur_moyenne'] < 5].shape[0]

# nombre d'ouvrages en remblai avec une hauteur maximale de plus de 10m, 5-10m ou moins de 5m
remblai_10m_max = remblai[remblai['hauteur_max'] >= 10].shape[0]
remblai_5_10m_max = remblai[remblai['hauteur_max'].between(5, 10)].shape[0]
remblai_5m_max = remblai[remblai['hauteur_max'] < 5].shape[0]

# nombre d'ouvrages en déblai avec une hauteur maximale de plus de 10m, 5-10m ou moins de 5m
deblai_10m_max = deblai[deblai['hauteur_max'] >= 10].shape[0]
deblai_5_10m_max = deblai[deblai['hauteur_max'].between(5, 10)].shape[0]
deblai_5m_max = deblai[deblai['hauteur_max'] < 5].shape[0]

# nombre d'ouvrages en remblai avec des pentes moyennes de plus de 60%, 30-60% ou moins de 30%
remblai_60 = remblai[remblai['pente_moyenne'] >= 0.6].shape[0]
remblai_30_60 = remblai[remblai['pente_moyenne'].between(0.3, 0.6)].shape[0]
remblai_30 = remblai[remblai['pente_moyenne'] < 0.3].shape[0]

# nombre d'ouvrages en déblai avec des pentes moyennes de plus de 60%, 30-60% ou moins de 30%
deblai_60 = deblai[deblai['pente_moyenne'] >= 0.6].shape[0]
deblai_30_60 = deblai[deblai['pente_moyenne'].between(0.3, 0.6)].shape[0]
deblai_30 = deblai[deblai['pente_moyenne'] < 0.3].shape[0]

# nombre d'ouvrages en remblai avec des pentes maximale de plus de 60%, 30-60% ou moins de 30%
remblai_60_max = remblai[remblai['pente_max'] >= 0.6].shape[0]
remblai_30_60_max = remblai[remblai['pente_max'].between(0.3, 0.6)].shape[0]
remblai_30_max = remblai[remblai['pente_max'] < 0.3].shape[0]

# nombre d'ouvrages en déblai avec des pentes maximale de plus de 60%, 30-60% ou moins de 30%
deblai_60_max = deblai[deblai['pente_max'] >= 0.6].shape[0]
deblai_30_60_max = deblai[deblai['pente_max'].between(0.3, 0.6)].shape[0]
deblai_30_max = deblai[deblai['pente_max'] < 0.3].shape[0]

# Créer des boxplots pour visualiser les distributions des hauteurs moyennes
data_haut_moy = [
    gdf['hauteur_moyenne'].dropna(),
    remblai['hauteur_moyenne'].dropna(),
    deblai['hauteur_moyenne'].dropna()
]
labels = [
    "Remblai & déblai",
    "Remblai",
    "Déblai"
]

plt.figure(figsize=(8, 6))
plt.boxplot(data_haut_moy, tick_labels=labels, patch_artist=True,
            boxprops=dict(facecolor='#4f8ef7', color='#2c3e50'),
            medianprops=dict(color='#e67e22', linewidth=2))
plt.ylabel("Hauteur moyenne (m)")
plt.title("Distribution de la hauteur moyenne par type d'ouvrage")
plt.grid(axis='y', linestyle=':', alpha=0.5)
plt.tight_layout()
boxplot_path = f"output_{route}/boxplot_hauteur_moyenne.png"
plt.savefig(boxplot_path)
plt.close()

# Créer des boxplots pour visualiser les distributions des hauteurs maximales
data_haut_max = [
    gdf['hauteur_max'].dropna(),
    remblai['hauteur_max'].dropna(),
    deblai['hauteur_max'].dropna()
]

plt.figure(figsize=(8, 6))
plt.boxplot(data_haut_max, tick_labels=labels, patch_artist=True,
            boxprops=dict(facecolor='#4f8ef7', color='#2c3e50'),
            medianprops=dict(color='#e67e22', linewidth=2))
plt.ylabel("Hauteur maximales (m)")
plt.title("Distribution de la hauteur maximale par type d'ouvrage")
plt.grid(axis='y', linestyle=':', alpha=0.5)
plt.tight_layout()
boxplot_path = f"output_{route}/boxplot_hauteur_maximale.png"
plt.savefig(boxplot_path)
plt.close()

# Créer des boxplots pour visualiser les distributions des pentes moyennes
data_pente_moy = [
    gdf['pente_moyenne'].dropna(),
    remblai['pente_moyenne'].dropna(),
    deblai['pente_moyenne'].dropna()
]

plt.figure(figsize=(8, 6))
plt.boxplot(data_pente_moy, tick_labels=labels, patch_artist=True,
            boxprops=dict(facecolor='#4f8ef7', color='#2c3e50'),
            medianprops=dict(color='#e67e22', linewidth=2))
plt.ylabel("Pente moyenne")
plt.title("Distribution de la pente moyenne par type d'ouvrage")
plt.grid(axis='y', linestyle=':', alpha=0.5)
plt.tight_layout()
boxplot_path = f"output_{route}/boxplot_pente_moyenne.png"
plt.savefig(boxplot_path)
plt.close()

# Créer des boxplots pour visualiser les distributions des pentes maximales
data_pente_max = [
    gdf['pente_max'].dropna(),
    remblai['pente_max'].dropna(),
    deblai['pente_max'].dropna()
]

plt.figure(figsize=(8, 6))
plt.boxplot(data_pente_max, tick_labels=labels, patch_artist=True,
            boxprops=dict(facecolor='#4f8ef7', color='#2c3e50'),
            medianprops=dict(color='#e67e22', linewidth=2))
plt.ylabel("Pente maximales")
plt.title("Distribution de la pente maximale par type d'ouvrage")
plt.grid(axis='y', linestyle=':', alpha=0.5)
plt.tight_layout()
boxplot_path = f"output_{route}/boxplot_pente_maximale.png"
plt.savefig(boxplot_path)
plt.close()

# CSS
css_content = """
body {
    font-family: 'Segoe UI', Arial, sans-serif;
    background: #f7f7f9;
    color: #222;
    margin: 0;
    padding: 0;
}
.container {
    max-width: 800px;
    margin: 40px auto;
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    padding: 32px 40px 40px 40px;
}
h1, h2, h3 {
    color: #2c3e50;
}
h2, h3 {
    padding-top: 20px;
}
table {
    border-collapse: collapse;
    width: 100%;
    margin-bottom: 32px;
    background: #fafbfc;
}
th, td {
    border: 1px solid #d1d5db;
    padding: 10px 16px;
    text-align: left;
}
th {
    background: #e5e7eb;
    color: #222;
}
tr:nth-child(even) td {
    background: #f3f4f6;
}
hr {
    border: none;
    border-top: 1px solid #e5e7eb;
    margin: 32px 0;
}
.remark {
    font-size: 0.95em;
    color: #555;
    background: #f9fafb;
    border-left: 4px solid #4f8ef7;
    padding: 12px 18px;
    margin-top: 24px;
    border-radius: 4px;
}
"""

# Write the CSS file
output_folder_css = f"output_{route}/statistiques_ouvrages.css"
with open(output_folder_css, "w", encoding="utf-8") as f:
    f.write(css_content)

# HTML content with CSS link and container
report_html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>Analyse statistique des ouvrages de la route {route}</title>
    <link rel="stylesheet" href="statistiques_ouvrages.css">
</head>
<body>
<div class="container">
<h1>Analyse statistique des ouvrages de la route {route}</h1>
<p>Ce document présente les statistiques descriptives calculées à partir du fichier <strong>output_{route}/selected_ouvrages.gpkg</strong>.
Le rapport est généré automatiquement à partir d'un fichier GeoPackage ; il contient donc uniquement des statistiques descriptives, sans interprétation.</p>

<h2>Statistiques globales</h2>
<p>Le fichier contient des informations sur les ouvrages de la route, classés en trois catégories : <strong>remblai</strong>, <strong>déblai</strong> et <strong>rasant</strong>. En total,
il y a <strong>{total_ouvrage} segments</strong>, dont <strong>{total_ouvrage_remblai} de type remblai</strong>, <strong>{total_ouvrage_deblai} de type déblai</strong> 
et <strong>{total_ouvrage_rasant} de type rasant</strong>. On commence par les statistiques globales, prenant en compte l'ensemble des ouvrages.</p>

<h3>1. Longueur</h3>
<p>La longueur totale des segments est de <strong>{total_length} m</strong>, partitionnée sur les deux directions de la route.</p>
<table>
<tr><th>Type d'ouvrage</th><th>Nombre</th><th>Longueur</th></tr>
<tr><td>Remblai</td><td>{total_ouvrage_remblai}</td><td>{total_length_remblai} m</td></tr>
<tr><td>Déblai</td><td>{total_ouvrage_deblai}</td><td>{total_length_deblai} m</td></tr>
<tr><td>Rasant</td><td>{total_ouvrage_rasant}</td><td>{total_length_rasant} m</td></tr>
<tr><td><strong>Total</strong></td><td><strong>{total_ouvrage}</strong></td><td><strong>{total_length} m</strong></td></tr>
</table>
<p>Un segment de type remblai a une longueur moyenne de <strong>{length_mean_remblai} m</strong>, un segment de type déblai de <strong>{length_mean_deblai} m</strong> et un segment
de type rasant de <strong>{length_mean_rasant} m</strong>.</p>

<h2>La hauteur</h2>
<p>Les hauteurs sont calculées pour les segments de type remblai et déblai, mais pas pour les segments de type rasant. On distingue la hauteur moyenne et la hauteur maximale.</p>

<h3>2. Hauteur moyenne</h3>
<p>Dans la figure ci-dessous, on peut voir la distribution de la hauteur moyenne pour les segments de type remblai et déblai, ainsi que les deux types ensemble.
La hauteur moyenne moyenne des segments de type remblai est de <strong>{hauteur_moyenne_mean_remblai} m</strong> et celle des segments de type déblai est de <strong>{hauteur_moyenne_mean_deblai} m</strong>.</p>
<img src="boxplot_hauteur_moyenne.png" alt="Boxplot hauteur moyenne" style="max-width:100%;margin-bottom:32px;">

<p>Voici la répartition des ouvrages de type <strong>remblai</strong> selon leur hauteur moyenne :</p>
<table>
<tr><th>Segments en remblai</th><th>Nombre</th></tr>
<tr><td><strong>Hauteur moyenne > 10 m</strong></td><td>{remblai_10m}</td></tr>
<tr><td><strong>Hauteur moyenne 5 - 10 m</strong></td><td>{remblai_5_10m}</td></tr>
<tr><td><strong>Hauteur moyenne < 5 m</strong></td><td>{remblai_5m}</td></tr>
</table>

<p>Et la répartition des ouvrages de type <strong>déblai</strong> selon leur hauteur moyenne :</p>
<table>
<tr><th>Segments en déblai</th><th>Nombre</th></tr>
<tr><td><strong>Hauteur moyenne > 10 m</strong></td><td>{deblai_10m}</td></tr>
<tr><td><strong>Hauteur moyenne 5 - 10 m</strong></td><td>{deblai_5_10m}</td></tr>
<tr><td><strong>Hauteur moyenne < 5 m</strong></td><td>{deblai_5m}</td></tr>
</table>

<h3>3. Hauteur maximale</h3>
<p>Dans la figure ci-dessous, on peut voir la distribution de la hauteur maximale pour les segments de type remblai et déblai, ainsi que les deux types ensemble.</p>
<img src="boxplot_hauteur_maximale.png" alt="Boxplot hauteur maximale" style="max-width:100%;margin-bottom:32px;">

<p>Voici la répartition des ouvrages de type <strong>remblai</strong> selon leur hauteur maximale :</p>
<table>
<tr><th>Segments en remblai</th><th>Nombre</th></tr>
<tr><td><strong>Hauteur maximale > 10 m</strong></td><td>{remblai_10m_max}</td></tr>
<tr><td><strong>Hauteur maximale 5 - 10 m</strong></td><td>{remblai_5_10m_max}</td></tr>
<tr><td><strong>Hauteur maximale < 5 m</strong></td><td>{remblai_5m_max}</td></tr>
</table>

<p>Et la répartition des ouvrages de type <strong>déblai</strong> selon leur hauteur maximale :</p>
<table>
<tr><th>Segments en déblai</th><th>Nombre</th></tr>
<tr><td><strong>Hauteur maximale > 10 m</strong></td><td>{deblai_10m_max}</td></tr>
<tr><td><strong>Hauteur maximale 5 - 10 m</strong></td><td>{deblai_5_10m_max}</td></tr>
<tr><td><strong>Hauteur maximale < 5 m</strong></td><td>{deblai_5m_max}</td></tr>
</table>

<h2>Les pentes</h2>
<p>Les pentes sont calculées pour les segments de type remblai et déblai, mais pas pour les segments de type rasant. On distingue la pente moyenne et la pente maximale.</p>
<p>La pente est calculée de la manière suivante : <code>pente = hauteur / distance</code>.</p>

<h3>4. Pente moyenne</h3>
<p>Dans la figure ci-dessous, on peut voir la distribution de la pente moyenne pour les segments de type remblai et déblai, ainsi que les deux types ensemble.
La pente moyenne moyenne des segments de type remblai est de <strong>{pente_moyenne_mean_remblai} m</strong> et celle des segments de type déblai est de <strong>{pente_moyenne_mean_deblai} m</strong>.</p>
<img src="boxplot_pente_moyenne.png" alt="Boxplot pente moyenne" style="max-width:100%;margin-bottom:32px;">

<p>Voici la répartition des ouvrages de type <strong>remblai</strong> selon leur pente moyenne :</p>
<table>
<tr><th>Segments en remblai</th><th>Nombre</th></tr>
<tr><td><strong>Pente moyenne > 60%</strong></td><td>{remblai_60}</td></tr>
<tr><td><strong>Pente moyenne 30 - 60%</strong></td><td>{remblai_30_60}</td></tr>
<tr><td><strong>Pente moyenne < 30%</strong></td><td>{remblai_30}</td></tr>
</table>

<p>Et la répartition des ouvrages de type <strong>déblai</strong> selon leur pente moyenne :</p>
<table>
<tr><th>Segments en déblai</th><th>Nombre</th></tr>
<tr><td><strong>Pente moyenne > 60%</strong></td><td>{deblai_60}</td></tr>
<tr><td><strong>Pente moyenne 30 - 60%</strong></td><td>{deblai_30_60}</td></tr>
<tr><td><strong>Pente moyenne < 30%</strong></td><td>{deblai_30}</td></tr>
</table>

<h3>5. Pente maximale</h3>
<p>Dans la figure ci-dessous, on peut voir la distribution de la pente maximale pour les segments de type remblai et déblai, ainsi que les deux types ensemble.</p>
<img src="boxplot_pente_maximale.png" alt="Boxplot pente maximale" style="max-width:100%;margin-bottom:32px;">

<p>Voici la répartition des ouvrages de type <strong>remblai</strong> selon leur pente maximale :</p>
<table>
<tr><th>Segments en remblai</th><th>Nombre</th></tr>
<tr><td><strong>Pente maximale > 60%</strong></td><td>{remblai_60_max}</td></tr>
<tr><td><strong>Pente maximale 30 - 60%</strong></td><td>{remblai_30_60_max}</td></tr>
<tr><td><strong>Pente maximale < 30%</strong></td><td>{remblai_30_max}</td></tr>
</table>

<p>Et la répartition des ouvrages de type <strong>déblai</strong> selon leur pente maximale :</p>
<table>
<tr><th>Segments en déblai</th><th>Nombre</th></tr>
<tr><td><strong>Pente maximale > 60%</strong></td><td>{deblai_60_max}</td></tr>
<tr><td><strong>Pente maximale 30 - 60%</strong></td><td>{deblai_30_60_max}</td></tr>
<tr><td><strong>Pente maximale < 30%</strong></td><td>{deblai_30_max}</td></tr>
</table>

<hr>
<div class="remark">
<strong>Remarques :</strong><br>
- Les valeurs sont extraites automatiquement du fichier GeoPackage <em>"selected_ouvrages.gpkg"</em>.<br>
- Pour obtenir les valeurs numériques, exécutez le script Python <code>analyse_ouvrages.py</code>.
</div>
</div>
</body>
</html>
"""
output_html = f"output_{route}/statistiques_ouvrages.html"
with open(output_html, "w", encoding="utf-8") as f:
    f.write(report_html)

# Open the HTML report in the default web browser
html_path = os.path.abspath(output_html)
webbrowser.open_new_tab(f"file:///{html_path}")



