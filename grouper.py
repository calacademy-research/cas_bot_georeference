"""grouper: a script for normalizing and grouping similar locality strings to allow for more effecient geolocation.
This is modified version of a script by Craig Meyer at the Fort Worth botanical garden"""
import pandas as pd
import re
import os
import sys
import warnings
from rapidfuzz import fuzz, process
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import argparse

warnings.filterwarnings(
    "ignore",
    message="The parameter 'token_pattern' will not be used since 'tokenizer' is not None'"
)

IMPORTANT_PHRASES = [
    'north', 'south', 'east', 'west',
    'northeast', 'northwest', 'southeast', 'southwest'
]
CUSTOM_STOP_WORDS = [
    'the', 'a', 'an', 'in', 'at', 'on', 'for', 'by', 'with', 'and',
    'or', 'but', 'from', 'between', 'along'
]
FUZZY_THRESHOLD = 90
SIMILARITY_THRESHOLD = 0.90
GROUPING_FIELD = "bels_location_id"
NULL_LOCALITY_STRINGS = {
    'unknown', 'no locality', '[no locality]',
    '[no additional data]', '[no additional locality data on sheet]',
    '[locality not indicated]', '[unspecified]',
    '[No location data on label.]', '[ Not readable ]',
    '[none]', 'none listed'
}


def parse_args():
    """Parse command-line arguments for the input file path."""
    parser = argparse.ArgumentParser(
        description="Grouper: normalize and group similar locality strings."
    )
    parser.add_argument(
        "input_path",
        type=str,
        help="Path to the input .csv or .tsv file"
    )
    args = parser.parse_args()

    path = args.input_path
    if not os.path.isfile(path):
        sys.exit(f"File not found: {path}")
    ext = os.path.splitext(path)[1].lower()
    if ext == '.tsv':
        sep = '\t'
    elif ext == '.csv':
        sep = ','
    else:
        sys.exit("Unsupported file type. Use .csv or .tsv")

    return path, sep


def index_rows(df: pd.DataFrame):
    """Assign a unique bels_location_id to all rows with non-null bels_decimallatitude,
    grouped by distinct combinations of location-related fields."""
    location_fields = [
        'bels_matchwithcoords',
        'bels_matchsanscoords',
        'bels_decimallatitude',
        'bels_decimallongitude',
        'bels_geodeticdatum',
        'bels_coordinateuncertaintyinmeters'
    ]

    valid_rows = df.copy()

    unique_locs = (
        valid_rows[location_fields]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(bels_location_id=lambda d: range(1, len(d) + 1))
    )

    df = df.merge(unique_locs, on=location_fields, how='left')

    df['bels_location_id'] = df['bels_location_id'].fillna(0).astype(int)

    return df

def preprocess(text: str) -> str:
    """
    Lowercase, normalize punctuation, compass directions, units,
    fractions, and common abbreviations in a locality string.
    """
    if pd.isnull(text):
        return ""
    text = text.lower()

    # remove repeated prefix before semicolon
    if ";" in text:
        prefix, rest = text.split(";", 1)
        prefix = prefix.strip()
        if prefix and prefix in rest:
            text = rest.strip()

    # --- Normalize possessives ---
    text = re.sub(r"\b(\w+)'s\b", r"\1s", text)

    # --- Normalize compound compass directions ---
    text = re.sub(r'(?<!\w)[nN][\.\s]?[eE](?!\w)', 'northeast', text)
    text = re.sub(r'(?<!\w)[nN][\.\s]?[wW](?!\w)', 'northwest', text)
    text = re.sub(r'(?<!\w)[sS][\.\s]?[eE](?!\w)', 'southeast', text)
    text = re.sub(r'(?<!\w)[sS][\.\s]?[wW](?!\w)', 'southwest', text)

    # --- Normalize single-letter compass directions ---
    text = re.sub(r'(?<![\w\'])\bn[\.\s]*(?=\W|$)', 'north ', text, flags=re.IGNORECASE)
    text = re.sub(r'(?<![\w\'])\bs[\.\s]*(?=\W|$)', 'south ', text, flags=re.IGNORECASE)
    text = re.sub(r'(?<![\w\'])\be[\.\s]*(?=\W|$)', 'east ', text, flags=re.IGNORECASE)
    text = re.sub(r'(?<![\w\'])\bw[\.\s]*(?=\W|$)', 'west ', text, flags=re.IGNORECASE)

    # Join separated compass directions with optional periods
    text = re.sub(r'\bnorth[\.\s]+east\b', 'northeast', text, flags=re.IGNORECASE)
    text = re.sub(r'\bnorth[\.\s]+west\b', 'northwest', text, flags=re.IGNORECASE)
    text = re.sub(r'\bsouth[\.\s]+east\b', 'southeast', text, flags=re.IGNORECASE)
    text = re.sub(r'\bsouth[\.\s]+west\b', 'southwest', text, flags=re.IGNORECASE)

    # --- Common abbreviation replacements ---
    text = re.sub(r'\bjct\b', 'junction', text, flags=re.IGNORECASE)
    text = re.sub(r'\bcir\.?\b', 'circle', text, flags=re.IGNORECASE)
    text = re.sub(r'\bave\.?\b', 'avenue', text, flags=re.IGNORECASE)
    text = re.sub(r'\bmt\.?\b', 'mountain', text, flags=re.IGNORECASE)
    text = re.sub(r'\bmtn\.?\b', 'mountain', text, flags=re.IGNORECASE)
    text = re.sub(r'\bmts\.?\b', 'mountains', text, flags=re.IGNORECASE)
    text = re.sub(r'\bmtns\.?\b', 'mountains', text, flags=re.IGNORECASE)
    text = re.sub(r'\bst\.?\s*hwy\b', 'highway', text, flags=re.IGNORECASE)
    text = re.sub(r'\bhwy\b', 'highway', text, flags=re.IGNORECASE)
    text = re.sub(r'\bhw\b', 'highway', text, flags=re.IGNORECASE)
    text = re.sub(r'\bsh\b', 'highway', text, flags=re.IGNORECASE)
    text = re.sub(r'\bmi\b', 'miles', text, flags=re.IGNORECASE)
    text = re.sub(r'\bft\.?\b', 'fort', text, flags=re.IGNORECASE)
    text = re.sub(r'\bst\.?\s+(?=[a-z])', 'street ', text)
    text = re.sub(r'\bState\s+(\d+)\b', r'highway \1', text, flags=re.IGNORECASE)
    text = re.sub(r'\bUS\s+(\d+)\b', r'highway \1', text, flags=re.IGNORECASE)
    text = re.sub(r'&', 'and', text)
    text = re.sub(r'\+', 'and', text)
    text = re.sub(r'\bok\b', 'oklahoma', text, flags=re.IGNORECASE)
    text = re.sub(r'\bokla\b', 'oklahoma', text, flags=re.IGNORECASE)
    text = re.sub(r'\btx\b', 'texas', text, flags=re.IGNORECASE)
    text = re.sub(r'\bTex\b', 'texas', text, flags=re.IGNORECASE)
    text = re.sub(r'\bCA\b', 'california', text, flags=re.IGNORECASE)
    text = re.sub(r'\bcoll\.?\b', 'collected', text, flags=re.IGNORECASE)
    text = re.sub(r'\btiburoun\.?\b', 'tiburon', text, flags=re.IGNORECASE)
    text = re.sub(r'\btamlpais\.?\b', 'tamalpais', text, flags=re.IGNORECASE)
    text = re.sub(r'\bpipline trail\.?\b', 'pipeline trail', text, flags=re.IGNORECASE)
    text = re.sub(r'\bfranciso\.?\b', 'francisco', text, flags=re.IGNORECASE)
    text = re.sub(r'\bfrancisc\.?\b', 'francisco', text, flags=re.IGNORECASE)

    text = re.sub(r'\bpipline trail\.?\b', 'pipeline trail', text, flags=re.IGNORECASE)
    text = re.sub(r'\bmiels\.?\b', 'miles', text, flags=re.IGNORECASE)
    text = re.sub(r'\bcañon\.?\b', 'canyon', text, flags=re.IGNORECASE)



    # Normalize leading decimals with zeros (".5" to "0.5") if preceded by whitespace or line start
    text = re.sub(r'(^|\s)\.(\d+)', r'\g<1>0.\2', text)

    # --- Normalize cases like "1mi", "3mi.", "2 mi", "2 mi." ---
    text = re.sub(r'(\d+(\.\d+)?)(\s*)mi\.?\b', r'\1 miles', text, flags=re.IGNORECASE)

    # --- Normalize km to kilometers ---
    text = re.sub(r'\bkm\b', 'kilometers', text, flags=re.IGNORECASE)

    # normalize patterns like "8.5kmE" to "8.5 kilometers east"
    text = re.sub(
        r'(\d+(\.\d+)?)(?:\s*)km\.?\s*([nsew])\b',
        lambda
            m: f"{m.group(1)} kilometers { {'n': 'north', 's': 'south', 'e': 'east', 'w': 'west'}[m.group(3).lower()]}",
        text,
        flags=re.IGNORECASE
    )

    # convert patterns like "3 km", "3km." to "3 kilometers"
    text = re.sub(r'(\d+(\.\d+)?)(\s*)km\.?\b', r'\1 kilometers', text, flags=re.IGNORECASE)

    # --- Normalize patterns like "8.5miW" to "8.5 miles west" ---
    text = re.sub(
        r'(\d+(\.\d+)?)(?:\s*)mi\.?\s*([nsew])\b',
        lambda
            m: f"{m.group(1)} miles { {'n': 'north', 's': 'south', 'e': 'east', 'w': 'west'}[m.group(3).lower()]}",
        text,
        flags=re.IGNORECASE
    )

    # Strip .0 from numbers like 5.0 miles to 5 miles
    text = re.sub(r'(\d+)\.0\b', r'\1', text)

    # Normalize numbers directly before compass directions with no unit to miles
    text = re.sub(
        r'(\d+(?:\.\d+)?)\s*(north|south|east|west|northeast|northwest|southeast|southwest)\b',
        r'\1 miles \2',
        text,
        flags=re.IGNORECASE
    )

    # --- Force singular "mile" to plural "miles" ---
    text = re.sub(r'\bmile\b', 'miles', text, flags=re.IGNORECASE)

    # --- Convert spelled-out numbers before miles to digits ---
    number_words = {
        'one': '1',
        'two': '2',
        'three': '3',
        'four': '4',
        'five': '5',
        'six': '6',
        'seven': '7',
        'eight': '8',
        'nine': '9',
        'ten': '10',
        'eleven': '11',
        'twelve': '12',
        'thirteen': '13',
        'fourteen': '14',
        'fifteen': '15',
        'sixteen': '16',
        'seventeen': '17',
        'eighteen': '18',
        'nineteen': '19',
        'twenty': '20'
    }

    directions = [
        'north', 'south', 'east', 'west',
        'northeast', 'northwest', 'southeast', 'southwest'
    ]
    dir_pattern = '|'.join(directions)

    for word, digit in number_words.items():
        text = re.sub(
            rf'\b{word}\b(?=\s*(miles?|{dir_pattern})\b)',
            digit,
            text
        )

    # --- Normalize spelled-out fractions like "one-half" ---
    fraction_words = {
        r'\bone[\s-]+half\b': '0.5',
        r'\bone[\s-]+third\b': '0.33',
        r'\btwo[\s-]+thirds\b': '0.66',
        r'\bone[\s-]+fourth\b': '0.25',
        r'\bthree[\s-]+fourths\b': '0.75',
        r'\bone[\s-]+quarter\b': '0.25',
        r'\bthree[\s-]+quarters\b': '0.75'
    }
    for pattern, replacement in fraction_words.items():
        text = re.sub(pattern, replacement, text)

    # --- Mixed ASCII fractions ---
    text = re.sub(r'(\d+)\s+1/2\b', lambda m: str(float(m.group(1)) + 0.5), text)
    text = re.sub(r'(\d+)\s+1/4\b', lambda m: str(float(m.group(1)) + 0.25), text)
    text = re.sub(r'(\d+)\s+3/4\b', lambda m: str(float(m.group(1)) + 0.75), text)
    text = re.sub(r'(\d+)\s+1/3\b', lambda m: str(float(m.group(1)) + 0.33), text)
    text = re.sub(r'(\d+)\s+2/3\b', lambda m: str(float(m.group(1)) + 0.66), text)

    # --- Mixed Unicode fractions ---
    text = re.sub(r'(\d+)½', lambda m: str(float(m.group(1)) + 0.5), text)
    text = re.sub(r'(\d+)¼', lambda m: str(float(m.group(1)) + 0.25), text)
    text = re.sub(r'(\d+)¾', lambda m: str(float(m.group(1)) + 0.75), text)
    text = re.sub(r'(\d+)⅓', lambda m: str(float(m.group(1)) + 0.33), text)
    text = re.sub(r'(\d+)⅔', lambda m: str(float(m.group(1)) + 0.66), text)

    # --- Standalone fractions ---
    text = re.sub(r'\b1/2\b', '0.5', text)
    text = re.sub(r'\b1/4\b', '0.25', text)
    text = re.sub(r'\b3/4\b', '0.75', text)
    text = re.sub(r'\b1/3\b', '0.33', text)
    text = re.sub(r'\b2/3\b', '0.66', text)
    text = re.sub(r'\b½\b', '0.5', text)
    text = re.sub(r'\b¼\b', '0.25', text)
    text = re.sub(r'\b¾\b', '0.75', text)
    text = re.sub(r'\b⅓\b', '0.33', text)
    text = re.sub(r'\b⅔\b', '0.66', text)

    # --- Remove approximate qualifiers like "ca", "ca.", or "about" with punctuation ---
    text = re.sub(r'\b(?:about|ca\.?)\s+', '', text, flags=re.IGNORECASE)

    # --- Remove trailing periods from known words ---
    text = re.sub(r'\b(miles|north|south|east|west|northeast|northwest|southeast|southwest)\.', r'\1', text)

    return re.sub(r'\s+', ' ', text).strip()


def extract_distance_direction(text: str) -> list[tuple[str, str]]:
    """
    From a normalized locality string extract all (distance, direction)
    pairs. E.g. '3 miles south and 2 miles east' -> [('2', 'east'), ('3', 'south')].
    """
    if pd.isnull(text):
        return []
    pattern = re.compile(
        r'(\d+(?:\.\d+)?)\s*(?:miles?|mi|kilometers|km)?\s*'
        r'(north|south|east|west|northeast|northwest|southeast|southwest)',
        flags=re.IGNORECASE
    )
    matches = pattern.findall(text)
    results = []
    for dist, dir_ in matches:
        num = float(dist)
        results.append((str(int(num) if num.is_integer() else num), dir_.lower()))
    return sorted(results, key=lambda x: (x[1], float(x[0])))


def build_tfidf_matrix(texts: pd.Series) -> tuple:
    """
    Build a TF‑IDF matrix with custom tokenization, stop words,
    boost important phrases/numbers, and return (group_matrix, vectorizer.vocabulary_).
    """
    def custom_tokenizer(t):
        return re.findall(r'\b[\d.]+|\b\w+\b', t)
    vec = TfidfVectorizer(
        tokenizer=custom_tokenizer, lowercase=False, stop_words=CUSTOM_STOP_WORDS
    )
    group_matrix = vec.fit_transform(texts)
    vocab = vec.vocabulary_
    for token, idx in vocab.items():
        if token in IMPORTANT_PHRASES or re.fullmatch(r'\d+(\.\d+)?', token):
            group_matrix[:, idx] *= 1.10
    return group_matrix, vocab


def alias_similar_tokens(group_matrix, vocab: dict, threshold: int = FUZZY_THRESHOLD) -> dict:
    """
    Perform fuzzy‑matching on tokens to alias less frequent ones to
    more frequent ones. Returns a map of merged_token -> canonical_token.
    """
    protected = set(IMPORTANT_PHRASES)
    token_list = [t for t in vocab if t not in protected and not t.replace(".", "").isdigit()]
    freq = {t: group_matrix[:, vocab[t]].nnz for t in token_list}

    merged = {}
    seen = set()
    # Compute all pairwise fuzz ratios using process.cdist
    sim_matrix = process.cdist(token_list, token_list, scorer=fuzz.ratio, dtype=int)

    for i, ti in enumerate(token_list):
        if ti in merged or ti in seen:
            continue
        for j in range(i + 1, len(token_list)):
            tj = token_list[j]
            if tj in merged or tj in seen:
                continue
            score = sim_matrix[i][j]
            if score >= threshold:
                if freq[ti] >= freq[tj]:
                    canon, other = ti, tj
                else:
                    canon, other = tj, ti
                group_matrix[:, vocab[canon]] += group_matrix[:, vocab[other]]
                group_matrix[:, vocab[other]] = 0
                merged[other] = canon
                seen.add(other)
                print(f"Aliasing '{other}' → '{canon}'")

    return merged


def cluster_localities(group_matrix, similarity_threshold: float = SIMILARITY_THRESHOLD) -> tuple[list[int], list[list[int]]]:
    """
    Compute pairwise cosine similarity on group_matrix, then cluster items with
    greedy labeling. Returns (suggested_ids, group_members).
    """
    sim = cosine_similarity(group_matrix)
    n = sim.shape[0]
    ids = [-1] * n
    group_members = []
    gid = 1
    for i in range(n):
        if ids[i] != -1:
            continue
        ids[i] = gid
        members = [i]
        for j in range(i+1, n):
            if ids[j] == -1 and sim[i, j] >= similarity_threshold:
                ids[j] = gid
                members.append(j)
        group_members.append(members)
        gid += 1
    return ids, group_members


def compute_confidence(ids: list[int], group_members: list[list[int]], sim_matrix) -> list[float]:
    """
    For each item, compute confidence as 100×average similarity with its group.
    Singletons get 100%. Returns list of percentages.
    """
    confidences = []
    for idx, gid in enumerate(ids):
        members = group_members[gid-1]
        if len(members) == 1:
            confidences.append(100.0)
        else:
            pairs = [
                sim_matrix[i, j]
                for i in members for j in members if i < j
            ]
            avg = sum(pairs) / len(pairs) if pairs else 0
            confidences.append(round(avg * 100, 1))
    return confidences


def validate_groups_by_distance(df: pd.DataFrame) -> pd.DataFrame:
    """
    If members of a group have differing distance_direction lists,
    split them by appending 'a','b',… to the ID; otherwise leave unchanged.
    """
    df['Final_Suggested_ID'] = df['Suggested_ID'].astype(str)
    for gid in df['Suggested_ID'].unique():
        sub = df[df['Suggested_ID'] == gid]
        sigs = sub['distance_direction'].apply(tuple).unique()
        if len(sigs) <= 1 or all(len(s) == 0 for s in sigs):
            continue
        for idx, sig in enumerate(sigs):
            suffix = chr(97 + idx)
            mask = sub['distance_direction'].apply(tuple) == sig
            df.loc[sub[mask].index, 'Final_Suggested_ID'] = f"{gid}_{suffix}"
    return df


def handle_null_localities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Any blank/null or placeholder locality gets Final_Suggested_ID = '0'.
    """
    mask = (
        df['locality'].isnull()
        | df['locality'].str.strip().str.lower().isin(NULL_LOCALITY_STRINGS)
    )
    df.loc[mask, 'Final_Suggested_ID'] = '0'
    return df

def propagate_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each Final_Suggested_ID group, if any row has non-null coordinates,
    fill those values across the group.
    """
    coordinate_fields = [
        'bels_decimallatitude',
        'bels_decimallongitude',
        'bels_geodeticdatum',
        'bels_coordinateuncertaintyinmeters'
    ]

    df = df.copy()
    for gid, group in df.groupby('Final_Suggested_ID'):
        if gid == '0':  # skip null locality group
            continue
        for field in coordinate_fields:
            values = group[field].dropna().unique()
            if len(values) == 1:
                df.loc[group.index, field] = values[0]
    return df


def merge_and_export(original: pd.DataFrame, grouped: pd.DataFrame, grouping_field: str = GROUPING_FIELD):
    """
    Merge Final_Suggested_ID and normalized_locality back to original,
    then export key CSV.
    """
    merged = original.merge(
        grouped[[grouping_field, 'Final_Suggested_ID', 'normalized_locality', 'Confidence']],
        on=grouping_field, how='left'
    )
    cols = [
        'catalogNumber', 'scientificName', 'institutionCode',
        'collectionCode', 'country', 'stateProvince', 'county',
        'locality', GROUPING_FIELD, 'Final_Suggested_ID',
        'normalized_locality', 'Confidence'
    ]
    cols = [c for c in cols if c in grouped.columns]
    export = grouped[cols].drop_duplicates()
    # out_file = os.path.splitext(path)[0] + '-key.csv'
    # export.to_csv(out_file, index=False)
    # output_path = os.path.join(os.getcwd(), "geo_csvs", "output_csv", "grouper_df.csv")
    # merged.to_csv(output_path, index=False)
    # print(f"Exported with suggested groups to: {out_file}")
    print(merged['Final_Suggested_ID'].value_counts())
    return merged, export


def grouper_main(geo_df):
    """The entire pipeline from loading to exporting results."""

    df = index_rows(geo_df)

    # require necessary columns
    if 'locality' not in df.columns or GROUPING_FIELD not in df.columns:
        sys.exit(f"CSV must contain 'locality' and '{GROUPING_FIELD}' columns.")

    # build grouped DataFrame
    grouped = (
        df.drop_duplicates(subset=GROUPING_FIELD)
          .assign(
              normalized_locality=lambda d: d['locality'].apply(preprocess),
              distance_direction=lambda d: d['normalized_locality'].apply(extract_distance_direction)
          )
    )

    # vectorize & boost
    group_matrix, vocab = build_tfidf_matrix(grouped['normalized_locality'])

    # alias tokens
    _ = alias_similar_tokens(group_matrix, vocab)

    # clustering
    suggested_ids, members = cluster_localities(group_matrix)
    grouped['Suggested_ID'] = suggested_ids

    # confidence
    sim_mat = cosine_similarity(group_matrix)
    grouped['Confidence'] = compute_confidence(suggested_ids, members, sim_mat)

    # finalize group IDs
    grouped = validate_groups_by_distance(grouped)
    grouped = handle_null_localities(grouped)

    # merge back and export
    merged, keyfile = merge_and_export(original=df, grouped=grouped)

    merged = propagate_coordinates(merged)

    return merged, keyfile
