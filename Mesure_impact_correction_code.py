# -*- coding: utf-8 -*-
"""Mesure d'impact des corrections - Pipeline Carto-Ouvrages

Version 2. On n'utilise plus lines_selected ni openpyxl (openpyxl causait des
crashes dans certains environnements QGIS sous Windows). L'ecriture Excel passe
maintenant par win32com, ce qui pilote Microsoft Excel directement. On peut choisir
si on remplit la colonne AVANT ou APRES. Le script calcule ce qu'il peut meme sans
zone PR. Si un champ length existe dans la couche, il est utilise en priorite.
"""

import os
import traceback
import unicodedata
from datetime import datetime

from qgis.PyQt.QtWidgets import QInputDialog, QMessageBox, QFileDialog
from qgis.core import QgsProject, QgsMessageLog, Qgis



LOG_TAG = "MesureImpact"

LAYER_NAMES = {
    "classified_points": "classified_profiles",
    "ouvrages_segments": "ouvrages_A31",
    "selected_ouvrages": "selected_ouvrages",
}

SEUIL_MICRO_SEGMENT_M = 5.0

# On suppose qu'un PR vaut 1000 m. C'est une estimation, pas une valeur exacte.
# Elle sert seulement a calculer la longueur de la zone quand l'utilisateur saisit des PR.
PAS_PR_M = 1000.0

INDICATEURS = [
    ("Zone d'etude",        "Route",                              "_meta_route"),
    ("Zone d'etude",        "PR debut",                           "_meta_pr_debut"),
    ("Zone d'etude",        "Abscisse debut (m)",                 "_meta_abs_debut"),
    ("Zone d'etude",        "PR fin",                             "_meta_pr_fin"),
    ("Zone d'etude",        "Abscisse fin (m)",                   "_meta_abs_fin"),
    ("Zone d'etude",        "Longueur de la zone (m)",            "_meta_longueur_zone"),

    ("classified_profiles", "Nb points classifies (total)",       "cp_nb_total"),
    ("classified_profiles", "Nb points remblai",                  "cp_nb_remblai"),
    ("classified_profiles", "Nb points deblai",                   "cp_nb_deblai"),
    ("classified_profiles", "Nb points rasant",                   "cp_nb_rasant"),
    ("classified_profiles", "Couverture spatiale (m)",            "cp_couverture_m"),
    ("classified_profiles", "Taux couverture (%)",                "cp_taux_couverture"),

    ("ouvrages_segments",   "Nb segments (total)",                "ou_nb_total"),
    ("ouvrages_segments",   "Nb segments remblai",                "ou_nb_remblai"),
    ("ouvrages_segments",   "Nb segments deblai",                 "ou_nb_deblai"),
    ("ouvrages_segments",   "Nb segments rasant",                 "ou_nb_rasant"),
    ("ouvrages_segments",   "Nb micro-segments (< 5 m)",          "ou_nb_micro"),
    ("ouvrages_segments",   "Nb segments avec hauteur=None ou 0", "ou_nb_hauteur_none"),
    ("ouvrages_segments",   "Longueur totale segments (m)",       "ou_longueur_totale"),

    ("selected_ouvrages",   "Nb ouvrages finaux",                 "se_nb_total"),
    ("selected_ouvrages",   "Nb remblais finaux",                 "se_nb_remblai"),
    ("selected_ouvrages",   "Nb deblais finaux",                  "se_nb_deblai"),
    ("selected_ouvrages",   "Longueur remblais detectes (m)",     "se_long_remblai"),
    ("selected_ouvrages",   "Longueur deblais detectes (m)",      "se_long_deblai"),
    ("selected_ouvrages",   "Longueur totale ouvrages (m)",       "se_long_totale"),
]


# ============================================================================
# Fonctions de log - messages dans la console et dans le panneau QGIS
# ============================================================================

def log(msg, level="INFO"):
    levels = {
        "INFO": Qgis.Info,
        "WARN": Qgis.Warning,
        "ERROR": Qgis.Critical,
        "SUCCESS": Qgis.Success,
    }

    QgsMessageLog.logMessage(msg, LOG_TAG, levels.get(level, Qgis.Info))

    ts = datetime.now().strftime("%H:%M:%S")
    prefix = {
        "INFO": "[INFO]   ",
        "WARN": "[WARN]   ",
        "ERROR": "[ERROR]  ",
        "SUCCESS": "[OK]     ",
    }.get(level, "[INFO]   ")

    print("{0} {1}{2}".format(ts, prefix, msg))


def log_exc(ctx, exc):
    log("{0} - Exception : {1}".format(ctx, exc), "ERROR")
    log(traceback.format_exc(), "ERROR")


# ============================================================================
# Verification des dependances avant de demarrer
# ============================================================================

def verifier_excel_com():
    """
    On s'assure que win32com est bien installe avant d'aller plus loin.
    openpyxl a provoque un crash QGIS sur certaines machines Windows,
    c'est pourquoi on est passe a win32com pour piloter Excel directement.
    """

    try:
        import win32com.client  # noqa
        log("Module win32com disponible - ecriture Excel via Microsoft Excel")
        return True

    except ImportError:
        QMessageBox.critical(
            None,
            "Module manquant",
            "Le module 'win32com' n'est pas disponible.\n\n"
            "Cette version évite openpyxl car il a provoqué un crash dans QGIS.\n\n"
            "Solution possible depuis l'OSGeo4W Shell :\n\n"
            "    python -m pip install pywin32\n\n"
            "Puis relancer QGIS."
        )

        log("Module win32com introuvable", "ERROR")
        return False


# ============================================================================
# Acces aux couches du projet QGIS
# ============================================================================

def get_layer(name):
    layers = QgsProject.instance().mapLayersByName(name)

    if not layers:
        return None

    if len(layers) > 1:
        log("Plusieurs couches '{0}', la premiere est utilisee".format(name), "WARN")

    return layers[0]


def verifier_couches():
    log("Verification des couches du projet...")

    couches = {}
    manquantes = []

    for cle, nom in LAYER_NAMES.items():
        layer = get_layer(nom)

        if layer is None:
            manquantes.append(nom)
        else:
            couches[cle] = layer
            log("  Couche '{0}' trouvee - {1} entites".format(nom, layer.featureCount()))

    if manquantes:
        QMessageBox.critical(
            None,
            "Couches manquantes",
            "Couches absentes du projet :\n\n"
            + "\n".join("  - " + n for n in manquantes)
            + "\n\nVerifier la constante LAYER_NAMES en haut du script."
        )

        log("{0} couche(s) manquante(s)".format(len(manquantes)), "ERROR")
        return None

    log("Toutes les couches necessaires sont presentes", "SUCCESS")
    return couches


# ============================================================================
# Dialogues : questions posees a l'utilisateur avant le calcul
# ============================================================================

def demander_creation_ou_ouverture():
    box = QMessageBox()
    box.setWindowTitle("Mesure d'impact - Choix du fichier")
    box.setText("Que souhaitez-vous faire ?")

    btn_c = box.addButton("Creer un nouveau fichier", QMessageBox.AcceptRole)
    btn_o = box.addButton("Completer un fichier existant", QMessageBox.AcceptRole)
    box.addButton("Annuler", QMessageBox.RejectRole)

    box.setDefaultButton(btn_c)
    box.exec_()

    if box.clickedButton() == btn_c:
        return "create"

    if box.clickedButton() == btn_o:
        return "open"

    return None


def demander_chemin(mode):
    if mode == "create":
        path, _ = QFileDialog.getSaveFileName(
            None,
            "Creer le fichier Excel",
            "mesure_impact.xlsx",
            "Excel (*.xlsx)"
        )
    else:
        path, _ = QFileDialog.getOpenFileName(
            None,
            "Selectionner le fichier Excel existant",
            "",
            "Excel (*.xlsx)"
        )

    if not path:
        return None

    if mode == "create" and not path.lower().endswith(".xlsx"):
        path = path + ".xlsx"

    return path


def demander_zone():
    """
    Saisie de la zone d'etude. La partie PR est optionnelle :
    si l'utilisateur n'a pas de PR ou ne veut pas filtrer,
    on prend toutes les entites disponibles dans les couches.
    """

    log("Saisie de la zone d'etude...")

    p = {
        "route": "",
        "use_pr": False,
        "pr_debut": None,
        "abs_debut": None,
        "pr_fin": None,
        "abs_fin": None,
    }

    route, ok = QInputDialog.getText(
        None,
        "Zone",
        "Code de la route, si connu, ex : A31.\n"
        "Laisser vide si non applicable :"
    )

    if not ok:
        return None

    route = route.strip().upper()
    p["route"] = route if route else "(non renseignee)"

    box = QMessageBox()
    box.setWindowTitle("Zone PR")
    box.setText(
        "Souhaitez-vous renseigner une zone PR/abscisse ?\n\n"
        "Si vos couches ne contiennent pas de PR, choisissez 'Sans PR'.\n"
        "Le script calculera alors les indicateurs possibles sur toute la couche."
    )

    btn_pr = box.addButton("Avec PR", QMessageBox.AcceptRole)
    btn_no_pr = box.addButton("Sans PR", QMessageBox.AcceptRole)
    box.addButton("Annuler", QMessageBox.RejectRole)

    box.setDefaultButton(btn_no_pr)
    box.exec_()

    clicked = box.clickedButton()

    if clicked == btn_pr:
        p["use_pr"] = True

        pr_d, ok = QInputDialog.getInt(
            None,
            "Zone PR (1/4)",
            "PR de debut :",
            0,
            0,
            9999
        )
        if not ok:
            return None
        p["pr_debut"] = pr_d

        abs_d, ok = QInputDialog.getInt(
            None,
            "Zone PR (2/4)",
            "Abscisse apres PR debut (m) :",
            0,
            0,
            5000
        )
        if not ok:
            return None
        p["abs_debut"] = abs_d

        pr_f, ok = QInputDialog.getInt(
            None,
            "Zone PR (3/4)",
            "PR de fin :",
            pr_d,
            pr_d,
            9999
        )
        if not ok:
            return None
        p["pr_fin"] = pr_f

        abs_f, ok = QInputDialog.getInt(
            None,
            "Zone PR (4/4)",
            "Abscisse apres PR fin (m) :",
            0,
            0,
            5000
        )
        if not ok:
            return None
        p["abs_fin"] = abs_f

        log(
            "Zone : {0} PR{1}+{2} -> PR{3}+{4}".format(
                p["route"],
                p["pr_debut"],
                p["abs_debut"],
                p["pr_fin"],
                p["abs_fin"]
            )
        )

    elif clicked == btn_no_pr:
        p["use_pr"] = False
        log("Zone sans PR : calcul sur toutes les entites disponibles")

    else:
        return None

    return p


def demander_phase():
    """
    On demande explicitement si on est AVANT ou APRES corrections
    pour ne pas ecrire dans la mauvaise colonne du tableau.
    """

    items = ["AVANT corrections", "APRES corrections"]

    item, ok = QInputDialog.getItem(
        None,
        "Phase de mesure",
        "Quelle partie du tableau Excel souhaitez-vous remplir ?",
        items,
        0,
        False
    )

    if not ok:
        return None

    phase = "AVANT" if "AVANT" in item else "APRES"

    log("Phase selectionnee : {0}".format(phase))

    return phase


# ============================================================================
# Petites fonctions utilitaires pour les attributs et les geometries
# ============================================================================

def normaliser_texte(v):
    if v is None:
        return ""

    s = str(v).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

    return s


def to_float(v):
    if v is None:
        return None

    if isinstance(v, str):
        txt = v.strip().replace(",", ".")

        if txt == "" or txt.lower() in ("none", "null", "nan"):
            return None

        v = txt

    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def trouver_champ(layer, candidats):
    champs = {f.name().lower(): f.name() for f in layer.fields()}

    for c in candidats:
        if c.lower() in champs:
            return champs[c.lower()]

    return None


def champs_pr_abs(layer):
    champ_pr = trouver_champ(
        layer,
        [
            "PR_start",
            "pr_start",
            "PR",
            "pr",
            "pr_debut",
            "PR_debut",
            "prdeb",
            "PR_deb",
        ]
    )

    champ_abs = trouver_champ(
        layer,
        [
            "abcisse_start",
            "abscisse_start",
            "abs_start",
            "abscisse",
            "abcisse",
            "abs",
            "ABS",
        ]
    )

    return champ_pr, champ_abs


def feature_dans_pr(feat, params, champ_pr, champ_abs):
    """
    Verifie si une entite appartient a la zone PR saisie par l'utilisateur.
    Cas particulier : si PR debut et PR fin sont identiques, on compare juste les abscisses.
    """

    try:
        pr = feat[champ_pr]
        abs_v = feat[champ_abs]
    except KeyError:
        return False

    if pr is None or abs_v is None:
        return False

    try:
        if isinstance(pr, str):
            txt = pr.strip().upper()

            match = re.search(r'PR\s*(\d+)', txt)

            if not match:
                return False

            pr = int(match.group(1))
        else:
            pr = int(pr)

        abs_v = to_float(abs_v)

        if abs_v is None:
            return False

    except (ValueError, TypeError):
        return False

    pr_d = params["pr_debut"]
    abs_d = params["abs_debut"]
    pr_f = params["pr_fin"]
    abs_f = params["abs_fin"]

    if pr_d is None or abs_d is None or pr_f is None or abs_f is None:
        return True

    if pr_d == pr_f:
        return pr == pr_d and abs_d <= abs_v <= abs_f

    if pr_d < pr < pr_f:
        return True

    if pr == pr_d and abs_v >= abs_d:
        return True

    if pr == pr_f and abs_v <= abs_f:
        return True

    return False


def filtrer_features(layer, params, nom_log):
    """
    Renvoie les entites a utiliser pour le calcul.
    Si l'utilisateur n'a pas defini de zone PR, ou si les champs sont absents,
    on prend tout. Sinon on filtre par PR et abscisse.
    """

    feats = list(layer.getFeatures())

    if not params.get("use_pr"):
        log(
            "{0} : pas de zone PR -> toutes les entites sont utilisees ({1})".format(
                nom_log,
                len(feats)
            )
        )
        return feats

    champ_pr, champ_abs = champs_pr_abs(layer)

    if not champ_pr or not champ_abs:
        log(
            "{0} : champs PR/abscisse absents -> toutes les entites sont utilisees ({1})".format(
                nom_log,
                len(feats)
            ),
            "WARN"
        )
        return feats

    res = [f for f in feats if feature_dans_pr(f, params, champ_pr, champ_abs)]

    log(
        "{0} : filtrage PR avec champs '{1}' / '{2}' -> {3} entites sur {4}".format(
            nom_log,
            champ_pr,
            champ_abs,
            len(res),
            len(feats)
        )
    )

    return res


def longueur_feature(feat, champ_longueur=None):
    """
    Retourne la longueur d'une entite. On regarde d'abord dans les attributs,
    puis on tombe sur la geometrie si le champ est absent, et 0 en dernier recours.
    """

    if champ_longueur:
        try:
            v = to_float(feat[champ_longueur])

            if v is not None:
                return v

        except KeyError:
            pass

    try:
        geom = feat.geometry()

        if geom and not geom.isEmpty():
            return geom.length()

    except Exception:
        pass

    return 0.0


def longueur_zone_depuis_pr(params):
    if not params.get("use_pr"):
        return ""

    pr_d = params.get("pr_debut")
    abs_d = params.get("abs_debut")
    pr_f = params.get("pr_fin")
    abs_f = params.get("abs_fin")

    if None in (pr_d, abs_d, pr_f, abs_f):
        return ""

    longueur = (pr_f - pr_d) * PAS_PR_M + (abs_f - abs_d)

    if longueur < 0:
        return ""

    return round(longueur, 1)


def compter_classes(features, champ_classif):
    if not champ_classif:
        return {
            "remblai": "(champ absent)",
            "deblai": "(champ absent)",
            "rasant": "(champ absent)",
        }

    nb_remblai = 0
    nb_deblai = 0
    nb_rasant = 0

    for f in features:
        c = normaliser_texte(f[champ_classif])

        if c == "remblai":
            nb_remblai += 1
        elif c == "deblai":
            nb_deblai += 1
        elif c == "rasant":
            nb_rasant += 1

    return {
        "remblai": nb_remblai,
        "deblai": nb_deblai,
        "rasant": nb_rasant,
    }


# ============================================================================
# Calcul des indicateurs 
# ============================================================================

def calculer_indicateurs(couches, params):
    log("=" * 60)
    log("Calcul des indicateurs...")

    ind = {}

    ind["_meta_route"] = params.get("route", "")
    ind["_meta_pr_debut"] = params.get("pr_debut", "")
    ind["_meta_abs_debut"] = params.get("abs_debut", "")
    ind["_meta_pr_fin"] = params.get("pr_fin", "")
    ind["_meta_abs_fin"] = params.get("abs_fin", "")
    ind["_meta_longueur_zone"] = longueur_zone_depuis_pr(params)

    longueur_zone = (
        ind["_meta_longueur_zone"]
        if isinstance(ind["_meta_longueur_zone"], (int, float))
        else None
    )

    # ------------------------------------------------------------------------
    # classified_profiles
    # ------------------------------------------------------------------------

    log("--- classified_profiles ---")

    cp = couches["classified_points"]

    cp_classif = trouver_champ(
        cp,
        ["classification", "type", "classe", "class"]
    )

    if not cp_classif:
        log("Champ 'classification' introuvable dans classified_profiles", "WARN")

    pts = filtrer_features(cp, params, "classified_profiles")

    classes_cp = compter_classes(pts, cp_classif)

    ind["cp_nb_total"] = len(pts)
    ind["cp_nb_remblai"] = classes_cp["remblai"]
    ind["cp_nb_deblai"] = classes_cp["deblai"]
    ind["cp_nb_rasant"] = classes_cp["rasant"]

    # On garde l'hypothese qu'un point classifie represente 1 m de couverture lineaire.
    ind["cp_couverture_m"] = len(pts)

    if longueur_zone and longueur_zone > 0:
        ind["cp_taux_couverture"] = round(100.0 * len(pts) / longueur_zone, 1)
    else:
        ind["cp_taux_couverture"] = ""

    log(
        "  points total : {0} | remblai : {1} | deblai : {2} | rasant : {3}".format(
            ind["cp_nb_total"],
            ind["cp_nb_remblai"],
            ind["cp_nb_deblai"],
            ind["cp_nb_rasant"],
        )
    )

    # ------------------------------------------------------------------------
    # ouvrages_segments
    # ------------------------------------------------------------------------

    log("--- ouvrages_segments ---")

    ou = couches["ouvrages_segments"]

    ou_classif = trouver_champ(
        ou,
        ["classification", "type", "classe", "class"]
    )

    ou_haut = trouver_champ(
        ou,
        ["hauteur_max", "hauteur", "height", "h_max"]
    )

    ou_long = trouver_champ(
        ou,
        ["length", "longueur", "long_m", "longueur_m", "len"]
    )

    if ou_long:
        log("ouvrages_segments : champ longueur utilise = '{0}'".format(ou_long))
    else:
        log("ouvrages_segments : champ longueur absent -> longueur geometrique utilisee", "WARN")

    ou_feats = filtrer_features(ou, params, "ouvrages_segments")

    classes_ou = compter_classes(ou_feats, ou_classif)

    ind["ou_nb_total"] = len(ou_feats)
    ind["ou_nb_remblai"] = classes_ou["remblai"]
    ind["ou_nb_deblai"] = classes_ou["deblai"]
    ind["ou_nb_rasant"] = classes_ou["rasant"]

    micro = 0
    long_tot = 0.0
    haut_none = 0

    for f in ou_feats:
        l = longueur_feature(f, ou_long)
        long_tot += l

        if l < SEUIL_MICRO_SEGMENT_M:
            micro += 1

        if ou_haut:
            h = to_float(f[ou_haut])

            if h is None or h == 0.0:
                haut_none += 1

    ind["ou_nb_micro"] = micro
    ind["ou_nb_hauteur_none"] = haut_none if ou_haut else "(champ absent)"
    ind["ou_longueur_totale"] = round(long_tot, 1)

    log(
        "  segments total : {0} | remblai : {1} | deblai : {2} | rasant : {3}".format(
            ind["ou_nb_total"],
            ind["ou_nb_remblai"],
            ind["ou_nb_deblai"],
            ind["ou_nb_rasant"],
        )
    )

    log(
        "  micro : {0} | hauteur None/0 : {1} | longueur totale : {2} m".format(
            ind["ou_nb_micro"],
            ind["ou_nb_hauteur_none"],
            ind["ou_longueur_totale"],
        )
    )

    # ------------------------------------------------------------------------
    # selected_ouvrages
    # ------------------------------------------------------------------------

    log("--- selected_ouvrages ---")

    se = couches["selected_ouvrages"]

    se_classif = trouver_champ(
        se,
        ["classification", "type", "classe", "class"]
    )

    se_long = trouver_champ(
        se,
        ["length", "longueur", "long_m", "longueur_m", "len"]
    )

    if se_long:
        log("selected_ouvrages : champ longueur utilise = '{0}'".format(se_long))
    else:
        log("selected_ouvrages : champ length/longueur absent -> longueur geometrique utilisee", "WARN")

    se_feats = filtrer_features(se, params, "selected_ouvrages")

    ind["se_nb_total"] = len(se_feats)

    if se_classif:
        remblais = []
        deblais = []

        for f in se_feats:
            c = normaliser_texte(f[se_classif])

            if c == "remblai":
                remblais.append(f)
            elif c == "deblai":
                deblais.append(f)

        ind["se_nb_remblai"] = len(remblais)
        ind["se_nb_deblai"] = len(deblais)

        ind["se_long_remblai"] = round(
            sum(longueur_feature(f, se_long) for f in remblais),
            1
        )

        ind["se_long_deblai"] = round(
            sum(longueur_feature(f, se_long) for f in deblais),
            1
        )

    else:
        ind["se_nb_remblai"] = "(champ absent)"
        ind["se_nb_deblai"] = "(champ absent)"
        ind["se_long_remblai"] = "(champ absent)"
        ind["se_long_deblai"] = "(champ absent)"

    ind["se_long_totale"] = round(
        sum(longueur_feature(f, se_long) for f in se_feats),
        1
    )

    log(
        "  ouvrages total : {0} | remblais : {1} | deblais : {2} | longueur totale : {3} m".format(
            ind["se_nb_total"],
            ind["se_nb_remblai"],
            ind["se_nb_deblai"],
            ind["se_long_totale"],
        )
    )

    log("=" * 60)
    log("Calcul termine", "SUCCESS")

    return ind


# ============================================================================
# Ecriture dans Excel via win32com 
# ============================================================================

def excel_rgb(hex_color):
    """
    Excel COM attend la couleur sous forme d'un entier BGR, pas RGB.
    Cette fonction fait la conversion depuis une chaine hexadecimale RRGGBB standard.
    """

    hex_color = hex_color.replace("#", "")

    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)

    return r + g * 256 + b * 65536


def excel_get_sheet(wb, sheet_name):
    """
    Cherche une feuille par son nom dans le classeur. Renvoie None si elle n'existe pas.
    """

    for ws in wb.Worksheets:
        if ws.Name == sheet_name:
            return ws

    return None


def excel_set_border(cell_or_range):
    """
    Bordures fines sur toutes les aretes d'une cellule ou d'une plage.
    """

    for border_id in [7, 8, 9, 10, 11, 12]:
        try:
            cell_or_range.Borders(border_id).LineStyle = 1
            cell_or_range.Borders(border_id).Weight = 2
            cell_or_range.Borders(border_id).Color = excel_rgb("DEDBD4")
        except Exception:
            pass


def excel_value(v):
    """
    Convertit None en chaine vide car Excel n'accepte pas les None Python.
    """

    if v is None:
        return ""

    return v


def structurer_feuille(ws):
    """
    Met en place le tableau : titre en haut, en-tetes de colonnes,
    et toutes les lignes d'indicateurs avec leur mise en forme.
    """

    log("Structuration de la feuille Excel...")

    # Table rase avant de reconstruire la structure
    ws.Cells.Clear()

    # Ligne de titre fusionnee en haut du tableau
    ws.Range("A1:E1").Merge()
    c = ws.Range("A1")
    c.Value = "Mesure d'impact des corrections - Pipeline Carto-Ouvrages"
    c.Font.Name = "Arial"
    c.Font.Size = 14
    c.Font.Bold = True
    c.Font.Color = excel_rgb("FFFFFF")
    c.Interior.Color = excel_rgb("1A1A1A")
    c.HorizontalAlignment = -4108  # centre
    c.VerticalAlignment = -4108
    ws.Rows(1).RowHeight = 26

    # En-tetes de colonnes
    headers = [
        "Section",
        "Indicateur",
        "AVANT corrections",
        "APRES corrections",
        "Ecart"
    ]

    for idx, h in enumerate(headers, start=1):
        c = ws.Cells(2, idx)
        c.Value = h
        c.Font.Name = "Arial"
        c.Font.Size = 11
        c.Font.Bold = True
        c.Font.Color = excel_rgb("FFFFFF")
        c.Interior.Color = excel_rgb("534AB7")
        c.HorizontalAlignment = -4108
        c.VerticalAlignment = -4108
        c.WrapText = True
        excel_set_border(c)

    ws.Rows(2).RowHeight = 22

    # Remplissage des lignes d'indicateurs
    sec_cur = None

    for i, (sec, lib, _key) in enumerate(INDICATEURS, start=3):
        # Colonne A : section, affichee une seule fois par groupe
        c = ws.Cells(i, 1)
        c.Value = sec if sec != sec_cur else ""
        c.Font.Name = "Arial"
        c.Font.Size = 10
        c.Font.Bold = True
        c.Interior.Color = excel_rgb("F1EFE8")
        c.HorizontalAlignment = -4131  # gauche
        c.VerticalAlignment = -4108
        c.WrapText = True
        excel_set_border(c)

        sec_cur = sec

        # Colonne B : libelle de l'indicateur
        c = ws.Cells(i, 2)
        c.Value = lib
        c.Font.Name = "Arial"
        c.Font.Size = 10
        c.HorizontalAlignment = -4131
        c.VerticalAlignment = -4108
        c.WrapText = True
        excel_set_border(c)

        # Colonne C : AVANT corrections (fond rose, vide pour l'instant)
        c = ws.Cells(i, 3)
        c.Value = ""
        c.Font.Name = "Arial"
        c.Font.Size = 10
        c.Interior.Color = excel_rgb("FCEBEB")
        c.HorizontalAlignment = -4108
        c.VerticalAlignment = -4108
        c.WrapText = True
        excel_set_border(c)

        # Colonne D : APRES corrections (fond vert clair, vide pour l'instant)
        c = ws.Cells(i, 4)
        c.Value = ""
        c.Font.Name = "Arial"
        c.Font.Size = 10
        c.Interior.Color = excel_rgb("E1F5EE")
        c.HorizontalAlignment = -4108
        c.VerticalAlignment = -4108
        c.WrapText = True
        excel_set_border(c)

        # Colonne E : ecart, calcule au moment du remplissage
        c = ws.Cells(i, 5)
        c.Value = ""
        c.Font.Name = "Arial"
        c.Font.Size = 10
        c.HorizontalAlignment = -4108
        c.VerticalAlignment = -4108
        c.WrapText = True
        excel_set_border(c)

    # Ajustement des largeurs de colonnes
    ws.Columns(1).ColumnWidth = 22
    ws.Columns(2).ColumnWidth = 42
    ws.Columns(3).ColumnWidth = 22
    ws.Columns(4).ColumnWidth = 22
    ws.Columns(5).ColumnWidth = 14

    # Un peu de hauteur pour que les libelles longs soient lisibles
    ws.Rows("3:{0}".format(len(INDICATEURS) + 2)).RowHeight = 28

    # On fige l'en-tete pour qu'il reste visible quand on defile
    try:
        ws.Application.ActiveWindow.SplitRow = 2
        ws.Application.ActiveWindow.FreezePanes = True
    except Exception:
        pass

    log("Feuille structuree", "SUCCESS")


def feuille_a_structurer(ws):
    """
    Verifie que le tableau a bien la structure attendue.
    Si les en-tetes ne correspondent pas, on remet tout en forme.
    """

    try:
        if ws.Cells(2, 1).Value != "Section":
            return True

        if ws.Cells(2, 2).Value != "Indicateur":
            return True

        if ws.Cells(2, 3).Value != "AVANT corrections":
            return True

        if ws.Cells(2, 4).Value != "APRES corrections":
            return True

        return False

    except Exception:
        return True


def creer_fichier(chemin):
    """
    Cree un nouveau fichier Excel vierge et y construit le tableau de mesure.
    """

    import win32com.client

    log("Creation du fichier Excel via Microsoft Excel : {0}".format(chemin))

    excel = None
    wb = None

    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False

        wb = excel.Workbooks.Add()

        # Excel cree souvent 3 feuilles par defaut, on n'en garde qu'une
        while wb.Worksheets.Count > 1:
            wb.Worksheets(wb.Worksheets.Count).Delete()

        ws = wb.Worksheets(1)
        ws.Name = "Mesure impact"

        structurer_feuille(ws)

        # FileFormat=51 correspond au format .xlsx dans l'API COM d'Excel
        wb.SaveAs(os.path.abspath(chemin), FileFormat=51)
        wb.Close(SaveChanges=True)
        wb = None

        log("Fichier cree", "SUCCESS")
        return True

    except Exception as e:
        log_exc("Erreur lors de la creation du fichier Excel", e)

        QMessageBox.critical(
            None,
            "Erreur Excel",
            "Impossible de créer le fichier Excel.\n\n"
            "Erreur : {0}\n\n"
            "Vérifiez que le fichier n'est pas déjà ouvert dans Excel.".format(e)
        )

        return False

    finally:
        try:
            if wb is not None:
                wb.Close(SaveChanges=False)
        except Exception:
            pass

        try:
            if excel is not None:
                excel.Quit()
        except Exception:
            pass


def remplir_fichier(chemin, ind, phase):
    """
    Ouvre le fichier existant et ecrit les indicateurs dans la bonne colonne.
    Colonne C pour AVANT, colonne D pour APRES.
    L'ecart (colonne E) est recalcule automatiquement a chaque remplissage.
    """

    import win32com.client

    log("Ouverture du fichier Excel : {0}".format(chemin))

    excel = None
    wb = None

    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False

        wb = excel.Workbooks.Open(os.path.abspath(chemin))

        ws = excel_get_sheet(wb, "Mesure impact")

        if ws is None:
            log("Feuille 'Mesure impact' absente, creation...", "WARN")
            ws = wb.Worksheets.Add(Before=wb.Worksheets(1))
            ws.Name = "Mesure impact"
            structurer_feuille(ws)

        elif feuille_a_structurer(ws):
            log("Feuille existante incomplete, restructuration...", "WARN")
            structurer_feuille(ws)

        col_phase = 3 if phase == "AVANT" else 4
        col_autre = 4 if phase == "AVANT" else 3

        log("Remplissage colonne {0}...".format(phase))

        nb = 0

        for i, (_section, _libelle, key) in enumerate(INDICATEURS, start=3):
            v = ind.get(key, "")

            ws.Cells(i, col_phase).Value = excel_value(v)

            if v not in (None, "", "(champ absent)"):
                nb += 1

            autre = ws.Cells(i, col_autre).Value

            # On calcule l'ecart seulement si les deux colonnes contiennent des nombres
            if isinstance(v, (int, float)) and isinstance(autre, (int, float)):
                if phase == "APRES":
                    ecart = v - autre
                else:
                    ecart = autre - v

                ws.Cells(i, 5).Value = round(ecart, 2)
            else:
                ws.Cells(i, 5).Value = ""

        wb.Save()
        wb.Close(SaveChanges=True)
        wb = None

        log("{0} indicateur(s) renseigne(s)".format(nb), "SUCCESS")

        return nb

    except Exception as e:
        log_exc("Erreur lors du remplissage du fichier Excel", e)

        QMessageBox.critical(
            None,
            "Erreur Excel",
            "Impossible de remplir le fichier Excel.\n\n"
            "Erreur : {0}\n\n"
            "Vérifiez que le fichier n'est pas déjà ouvert dans Excel.".format(e)
        )

        return 0

    finally:
        try:
            if wb is not None:
                wb.Close(SaveChanges=False)
        except Exception:
            pass

        try:
            if excel is not None:
                excel.Quit()
        except Exception:
            pass


# ============================================================================
# Mise en forme du message de synthese affiche a la fin
# ============================================================================

def format_zone(params):
    if params.get("use_pr"):
        return "{0} PR{1}+{2} -> PR{3}+{4}".format(
            params.get("route", ""),
            params.get("pr_debut", ""),
            params.get("abs_debut", ""),
            params.get("pr_fin", ""),
            params.get("abs_fin", ""),
        )

    return "{0} - sans PR".format(params.get("route", ""))


# ============================================================================
# Fonction principale 
# ============================================================================

def main():
    log("=" * 60)
    log("DEMARRAGE - Mesure d'impact")
    log("=" * 60)

    try:
        if not verifier_excel_com():
            return

        couches = verifier_couches()

        if couches is None:
            return

        mode = demander_creation_ou_ouverture()

        if mode is None:
            log("Annulation", "WARN")
            return

        chemin = demander_chemin(mode)

        if chemin is None:
            log("Aucun fichier selectionne", "WARN")
            return

        if mode == "create":
            ok = creer_fichier(chemin)

            if not ok:
                return

        elif not os.path.exists(chemin):
            log("Fichier introuvable : {0}".format(chemin), "ERROR")
            QMessageBox.critical(
                None,
                "Fichier introuvable",
                "Le fichier sélectionné n'existe pas."
            )
            return

        params = demander_zone()

        if params is None:
            log("Annulation zone", "WARN")
            return

        # On pose la question ici pour eviter toute confusion plus tard dans le tableau
        phase = demander_phase()

        if phase is None:
            log("Annulation phase", "WARN")
            return

        ind = calculer_indicateurs(couches, params)

        nb = remplir_fichier(chemin, ind, phase)

        QMessageBox.information(
            None,
            "Mesure terminee",
            "Mesure enregistree.\n\n"
            "Phase remplie : {0} corrections\n"
            "Zone : {1}\n"
            "Indicateurs renseignes : {2} / {3}\n\n"
            "Fichier : {4}".format(
                phase,
                format_zone(params),
                nb,
                len(INDICATEURS),
                chemin
            )
        )

        log("=" * 60)
        log("FIN", "SUCCESS")
        log("=" * 60)

    except Exception as e:
        log_exc("Erreur fatale dans main()", e)

        QMessageBox.critical(
            None,
            "Erreur",
            "Erreur :\n\n{0}\n\nVoir la console Python pour la trace complete.".format(e)
        )


main()