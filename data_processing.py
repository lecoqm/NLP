import os
import re
import argparse
from pathlib import Path
import pandas as pd


DEFAULT_TEXT_COLUMNS = [
    "id",
    "text",
    "date",
    "contexte-tour",
    "departement",
    "identifiant de circonscription",
    "titulaire-sexe",
    "titulaire-mandat-en-cours",
    "titulaire-mandat-passe",
    "titulaire-soutien",
]


_HYPHEN_RE = re.compile(r"(?<=\w)-\s+(?=\w)")
_ARCHIVE_RE = re.compile(
    r"Sciences\s+Po\s*/\s*fonds\s+CEVIPO[FV]",
    flags=re.IGNORECASE,
)
_SYMBOL_RE = re.compile(r"[☐□■▪◼◻]")
_SPACES_RE = re.compile(r"[ \t]+")


def clean_text(text):
    """Nettoyage des textes de tracts."""
    if pd.isna(text):
        return ""

    text = str(text)

    # Recoller les mots coupés en fin de ligne
    text = _HYPHEN_RE.sub("", text)

    # Enlever la mention d'archive et les symboles parasites
    text = _ARCHIVE_RE.sub("", text)
    text = _SYMBOL_RE.sub(" ", text)

    # Normalisation des espaces horizontaux
    text = _SPACES_RE.sub(" ", text)

    # Filtrage ligne par ligne
    cleaned_lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        low = line.lower()

        if (
            low.startswith("imprimerie")
            or low.startswith("imp.")
            or low.startswith("impr.")
            or low.startswith("ne pas jeter sur la voie publique")
            or low.startswith("édité par")
            or low.startswith("imprimé par")
            or low.startswith("tiré par")
        ):
            continue

        cleaned_lines.append(line)

    return "\n".join(cleaned_lines)


def load_texts(base_dir):
    """
    Lit tous les fichiers .txt présents dans les dossiers legislatives_*.
    Ignore les fichiers contenant 'pdfmasterocr' ou 'pdfmaster' dans leur nom.
    """
    base_path = Path(base_dir)
    all_rows = []

    legislative_dirs = sorted(base_path.glob("legislatives_*"))

    for folder in legislative_dirs:
        if not folder.is_dir():
            continue

        year_match = re.search(r"legislatives_(\d{4})", folder.name)
        year = int(year_match.group(1)) if year_match else None

        for file_path in sorted(folder.glob("*.txt")):
            if "pdfmaster" in file_path.name.lower():
                continue

            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                text = f.read()

            all_rows.append(
                {
                    "id": file_path.stem,
                    "text": clean_text(text),
                    "year": year,
                }
            )

    return pd.DataFrame(all_rows)


def read_csv_with_fallback(path, sep=",", dtype=str):
    """Lecture CSV robuste aux encodages Windows/Excel."""
    last_error = None

    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            return pd.read_csv(
                path,
                sep=sep,
                encoding=encoding,
                dtype=dtype,
                low_memory=False,
            )
        except UnicodeDecodeError as exc:
            last_error = exc

    raise ValueError(
        f"Impossible de lire {path} avec utf-8-sig, utf-8, cp1252 ou latin1."
    ) from last_error


def load_candidates(base_dir, candidate_file="archelect_search.csv"):
    """Charge le fichier candidats."""
    candidate_path = Path(base_dir) / candidate_file

    if not candidate_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {candidate_path}")

    return read_csv_with_fallback(candidate_path, sep=",", dtype=str)


def normalize_series_for_mapping(series):
    """Normalisation légère pour faciliter les jointures de mapping."""
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


def normalize_value_for_mapping(value):
    """Normalise une valeur scalaire pour chercher dans un dictionnaire."""
    if pd.isna(value):
        return ""

    return re.sub(r"\s+", " ", str(value).strip())


def read_mapping_csv(mapping_csv_path):
    """Charge un CSV de correspondance séparé par ';' avec encodage robuste."""
    mapping_df = read_csv_with_fallback(mapping_csv_path, sep=";", dtype=str)

    # Nettoie les colonnes vides créées par un séparateur final, ex. 'Unnamed: 2'
    unnamed_cols = [
        c for c in mapping_df.columns
        if str(c).startswith("Unnamed:")
    ]

    if unnamed_cols:
        mapping_df = mapping_df.drop(columns=unnamed_cols)

    mapping_df.columns = [str(c).strip() for c in mapping_df.columns]

    return mapping_df


def apply_mapping_from_csv(
    df,
    source_col,
    mapping_csv_path,
    mapping_source_col,
    mapping_value_col,
    default_value=None,
    keep="all",
):
    """
    Applique un mapping à partir d'un CSV.

    keep:
    - "all" : garde toutes les catégories trouvées, sans doublons
    - "first" : garde seulement la première catégorie trouvée
    - "max" : garde la catégorie maximale selon un ordre défini
    """
    mapping_df = read_mapping_csv(mapping_csv_path)

    source_aliases = {
        "titulaire-soutien": ["titulaire-soutien", "parti"],
        "mandat": ["mandat"],
    }

    value_aliases = {
        "titulaire-soutien-categorie": [
            "titulaire-soutien-categorie",
            "groupe",
        ],
        "mandat-categorie": ["mandat-categorie"],
    }

    possible_source_cols = source_aliases.get(
        mapping_source_col,
        [mapping_source_col],
    )

    possible_value_cols = value_aliases.get(
        mapping_value_col,
        [mapping_value_col],
    )

    actual_source_col = next(
        (c for c in possible_source_cols if c in mapping_df.columns),
        None,
    )

    actual_value_col = next(
        (c for c in possible_value_cols if c in mapping_df.columns),
        None,
    )

    missing = []

    if actual_source_col is None:
        missing.append(mapping_source_col)

    if actual_value_col is None:
        missing.append(mapping_value_col)

    if missing:
        raise ValueError(
            f"Colonnes manquantes dans {mapping_csv_path}: {missing}. "
            f"Colonnes disponibles: {list(mapping_df.columns)}"
        )

    mapping_df = mapping_df[[actual_source_col, actual_value_col]].copy()

    mapping_df[actual_source_col] = normalize_series_for_mapping(
        mapping_df[actual_source_col]
    )

    mapping_df[actual_value_col] = normalize_series_for_mapping(
        mapping_df[actual_value_col]
    )

    mapping_df = mapping_df.dropna(subset=[actual_source_col])
    mapping_df = mapping_df[mapping_df[actual_source_col] != ""]
    mapping_df = mapping_df.drop_duplicates(subset=[actual_source_col])

    mapping_dict = dict(
        zip(mapping_df[actual_source_col], mapping_df[actual_value_col])
    )

    # Ordre de priorité des catégories de mandat.
    # Adapte ces libellés si ton fichier mandats_correspondance.csv utilise d'autres noms.
    mandat_priority = {
        "Aucun mandat": 0,
        "Local": 1,
        "Départemental": 2,
        "Régional": 3,
        "National": 4,
        "Européen": 5,
        "Gouvernemental": 6,
    }

    def category_rank(category):
        if pd.isna(category):
            return -1

        return mandat_priority.get(str(category).strip(), -1)

    def map_cell(value):
        normalized_value = normalize_value_for_mapping(value)

        if not normalized_value:
            return default_value if default_value is not None else pd.NA

        parts = [
            normalize_value_for_mapping(part)
            for part in normalized_value.split(";")
            if normalize_value_for_mapping(part)
        ]

        if not parts:
            return default_value if default_value is not None else pd.NA

        if keep == "first":
            first_part = parts[0]
            mapped_value = mapping_dict.get(first_part)

            if pd.notna(mapped_value) and str(mapped_value).strip():
                return str(mapped_value).strip()

            return default_value if default_value is not None else pd.NA

        mapped_values = []

        for part in parts:
            mapped_value = mapping_dict.get(part)

            if pd.notna(mapped_value) and str(mapped_value).strip():
                mapped_values.append(str(mapped_value).strip())

        # Supprime les doublons en conservant l'ordre
        mapped_values = list(dict.fromkeys(mapped_values))

        if not mapped_values:
            return default_value if default_value is not None else pd.NA

        if keep == "max":
            return max(mapped_values, key=category_rank)

        return "; ".join(mapped_values)

    return df[source_col].apply(map_cell)


def parse_columns_argument(columns_arg):
    """
    Accepte soit une liste argparse, soit une chaîne CSV.

    Exemples :
    --columns id text date
    ou
    --columns "id,text,date"
    """
    if columns_arg is None:
        return DEFAULT_TEXT_COLUMNS

    if isinstance(columns_arg, list):
        if len(columns_arg) == 1 and "," in columns_arg[0]:
            return [
                c.strip()
                for c in columns_arg[0].split(",")
                if c.strip()
            ]

        return [
            c.strip()
            for c in columns_arg
            if c.strip()
        ]

    return [
        c.strip()
        for c in str(columns_arg).split(",")
        if c.strip()
    ]


def build_processed_dataframe(
    base_dir="data",
    candidate_file="archelect_search.csv",
    selected_columns=None,
    soutien_mapping_csv=None,
    mandat_mapping_csv=None,
):
    """
    Construit le DataFrame final.
    """
    texts_df = load_texts(base_dir)
    candidates_df = load_candidates(base_dir, candidate_file=candidate_file)

    if "id" not in candidates_df.columns:
        raise ValueError("La colonne 'id' est absente du fichier candidats")

    candidates_df = candidates_df.copy()

    candidates_df["id"] = (
        candidates_df["id"]
        .astype(str)
        .str.strip()
    )

    texts_df["id"] = (
        texts_df["id"]
        .astype(str)
        .str.strip()
    )

    df = texts_df.merge(candidates_df, how="inner", on="id")

    selected_columns = parse_columns_argument(selected_columns)

    missing_cols = [
        col for col in selected_columns
        if col not in df.columns
    ]

    if missing_cols:
        raise ValueError(
            "Colonnes demandées absentes après fusion : "
            + ", ".join(missing_cols)
        )

    df = df[selected_columns].copy()

    if "titulaire-mandat-en-cours" in df.columns:
        if mandat_mapping_csv is not None:
            df["titulaire-mandat-en-cours-categorie"] = apply_mapping_from_csv(
                df=df,
                source_col="titulaire-mandat-en-cours",
                mapping_csv_path=mandat_mapping_csv,
                mapping_source_col="mandat",
                mapping_value_col="mandat-categorie",
                default_value=pd.NA,
                keep="max",
            )
        else:
            df["titulaire-mandat-en-cours-categorie"] = pd.NA

    if "titulaire-mandat-passe" in df.columns:
        if mandat_mapping_csv is not None:
            df["titulaire-mandat-passe-categorie"] = apply_mapping_from_csv(
                df=df,
                source_col="titulaire-mandat-passe",
                mapping_csv_path=mandat_mapping_csv,
                mapping_source_col="mandat",
                mapping_value_col="mandat-categorie",
                default_value=pd.NA,
                keep="max",
            )
        else:
            df["titulaire-mandat-passe-categorie"] = pd.NA

    if "titulaire-soutien" in df.columns:
        if soutien_mapping_csv is not None:
            df["titulaire-soutien-categorie"] = apply_mapping_from_csv(
                df=df,
                source_col="titulaire-soutien",
                mapping_csv_path=soutien_mapping_csv,
                mapping_source_col="titulaire-soutien",
                mapping_value_col="titulaire-soutien-categorie",
                default_value="Sans étiquette / autres",
                keep="first",
            )
        else:
            df["titulaire-soutien-categorie"] = pd.NA

    return df


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--candidate-file", default="archelect_search.csv")

    parser.add_argument(
        "--soutien-mapping-csv",
        default="data/partis_correspondance.csv",
    )

    parser.add_argument(
        "--mandat-mapping-csv",
        default="data/mandats_correspondance.csv",
    )

    parser.add_argument(
        "--output",
        default="data/texts_processed.csv",
    )

    parser.add_argument(
        "--columns",
        nargs="+",
        default=DEFAULT_TEXT_COLUMNS,
        help=(
            "Colonnes à garder. "
            "Exemple: --columns id text date titulaire-soutien "
            'ou --columns "id,text,date,titulaire-soutien"'
        ),
    )

    args = parser.parse_args()

    soutien_mapping = (
        args.soutien_mapping_csv
        if os.path.exists(args.soutien_mapping_csv)
        else None
    )

    mandat_mapping = (
        args.mandat_mapping_csv
        if os.path.exists(args.mandat_mapping_csv)
        else None
    )

    df = build_processed_dataframe(
        base_dir=args.base_dir,
        candidate_file=args.candidate_file,
        selected_columns=args.columns,
        soutien_mapping_csv=soutien_mapping,
        mandat_mapping_csv=mandat_mapping,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(
        output_path,
        index=False,
        sep=";",
        encoding="utf-8-sig",
    )


if __name__ == "__main__":
    main()