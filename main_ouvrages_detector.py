from profile_analyzer_viz import ProfileAnalyzer
from segments_constructor import SegmentConstructor
from select_ouvrages import OuvragesSelector

def main():
    route = input("Saisir le code de la route (ex. A33): ")

    output_folder = f"output_{route}"

    classification_threshold_remblai = 2
    classification_threshold_deblai = -2

    analyzer = ProfileAnalyzer(
        mnt_path = "data/mnt.tif",
        output_folder = output_folder,
        classification_threshold_remblai = classification_threshold_remblai,
        classification_threshold_deblai = classification_threshold_deblai,
        route_number = route
    )
    segments_gdf, calculation_points_gdf = analyzer.analyze_profile()
    analyzer.save_output(segments_gdf, calculation_points_gdf)

    constructor = SegmentConstructor(
        classified_profiles = segments_gdf,
        output_folder = output_folder,
        route_number = route,
        lines_selected = analyzer.lines_selected
    )
    ouvrages_gdf = constructor.construct_segments()
    constructor.save_output(ouvrages_gdf)


    selector = OuvragesSelector(
        ouvrages_gdf = ouvrages_gdf,
        output_folder = output_folder,
        route_number = route
    )
    selected_ouvrages = selector.select_ouvrages()
    selector.save_output(selected_ouvrages)

if __name__ == "__main__":
    main()
