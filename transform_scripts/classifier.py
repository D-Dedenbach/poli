from transformers import pipeline
from datetime import datetime
from tqdm import tqdm

import duckdb
import pandas as pd
import logging
import argparse
import random

from backend.configs.database import DB_PATH
from utils.categorization_utils import (load_categories, 
                                        load_slug_category_names,
                                        get_categories_version)


# --------------------------- Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Classifier specs - current version is for internal tracking of changes
classifier = pipeline("zero-shot-classification", model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli")
current_version = 2

# Fetch uncategorized cases and insert categorizations
FETCH_UNCATEGORIZED_CASES = """
SELECT id
    , case_title
    , case_reasoning 
FROM dev.stg_case C 
WHERE NOT EXISTS (
    SELECT 1 FROM dev.stg_case_cat_zeroshot CAT 
    WHERE CAT.case_id = C.id
      AND CAT.cat_version = {cat_version}
      AND CAT.classifier_version = {classifier_version}
)
ORDER BY C.updated_at DESC 
LIMIT {size} ;
"""

INSERT_CATEGORIZED_CASES = """
INSERT INTO dev.stg_case_cat_zeroshot (case_id, cat_name, confidence_score, cat_version, created_at, classifier_version)
SELECT case_id, cat_name, confidence_score, cat_version, created_at, classifier_version FROM scores_temp;
"""

INSERT_RAW_SCORES = """
INSERT INTO dev.stg_case_scores_raw (case_index, cat_name, confidence_score, batch_timestamp)
SELECT case_index, cat_name, confidence_score, {batch_timestamp} FROM scores_raw;
"""


# -------------------------- Scripting

def get_classification_scores(texts: list[str]) -> pd.DataFrame:
    """
    Classify a batch of texts.
    Returns DataFrame with 1 column for sequence and 1 column per label with scores.
    """

    categories = load_categories()

    category_names = [cat['name'] + ":" + cat['description'] for cat in categories]
    shuffled_names = category_names.copy()
    random.shuffle(shuffled_names)

    results = classifier(texts, shuffled_names, multi_label=True)

    output_data = []
    for result in results:
        row = {'sequence': result['sequence']}
        for label, score in zip(result['labels'], result['scores']):
            row[label] = score
        output_data.append(row)

    return pd.DataFrame(output_data)


def transform_scores_to_results(
    scores_df: pd.DataFrame,
    cases_df: pd.DataFrame,
    cat_version: int,
    classifier_version: int,
    batch_timestamp: str
) -> pd.DataFrame:
    """
    Transform classification scores into database-ready format.
    Returns DataFrame with columns: case_id, cat_name, confidence_score,
    cat_version, created_at, classifier_version
    """

    result_df = pd.merge(
        scores_df,
        cases_df[['id']].reset_index(),
        left_on='case_index',
        right_on='index'
    )

    return result_df.rename(columns={'id': 'case_id'}).assign(
        cat_version=cat_version,
        classifier_version=classifier_version,
        created_at=batch_timestamp
    )[[ 'case_id', 'cat_name', 'confidence_score', 'cat_version', 'created_at', 'classifier_version']]



def classify_case_batch(size: int = 100):
    """
    Function Queries uncategorized cases from stg_case and runs a classifier on them.
    Uses batch processing with size 32 for optimal GPU utilization.
    Finally loads the classified cases into stg_case_cat_zeroshot.
    
    Returns:
        dict: {
            'success': bool,
            'cases_fetched': int,
            'scores_generated': int,
            'rows_inserted': int,
            'error': str or None
        }
    """
    metrics = {
        'success': False,
        'cases_fetched': 0,
        'scores_generated': 0,
        'rows_inserted': 0,
        'error': None
    }
    
    try:
        cat_version = get_categories_version()
        classifier_version = current_version
        batch_timestamp = datetime.now().isoformat()

        logger.info(f"Starting classification batch | cat_version={cat_version}, classifier_version={classifier_version}, size={size}")

        fetch_qry = FETCH_UNCATEGORIZED_CASES.format(
            cat_version=cat_version,
            classifier_version=classifier_version,
            size=size
        )

        conn = duckdb.connect(database=DB_PATH)
        df = conn.execute(fetch_qry).fetchdf()
        metrics['cases_fetched'] = len(df)
        logger.info(f"Fetched {metrics['cases_fetched']} uncategorized cases")

        if df.empty:
            logger.info("No cases to process - exiting")
            metrics['success'] = True
            conn.close()
            return metrics

        df['categorization_text'] = df['case_title'].fillna('') + ': ' + df['case_reasoning'].fillna('')
        texts = df['categorization_text'].tolist()

        # Classify in batches of 32, using pipeline's native parallelism
        batch_size = 32
        all_scores = []

        for i in tqdm(range(0, len(texts), batch_size), desc='Classifying'):
            batch_texts = texts[i:i + batch_size]
            batch_scores = get_classification_scores(batch_texts)
            batch_scores['case_index'] += i
            all_scores.append(batch_scores)
            print(batch_scores)

        scores_df = pd.concat(all_scores, ignore_index=True)
        
        # Insert raw scores
        insert_raw_qry = INSERT_RAW_SCORES.format(batch_timestamp=f"'{batch_timestamp}'")
        conn.register('scores_raw', scores_df)
        conn.execute(insert_raw_qry)
        conn.unregister('scores_raw')
        
        final_data = transform_scores_to_results(
            scores_df, df, cat_version, classifier_version, batch_timestamp
        )

        logger.info(f"Generated {len(final_data)} scores for {df['id'].nunique()} cases")

        conn.register('scores_temp', final_data)
        conn.execute(INSERT_CATEGORIZED_CASES)
        metrics['scores_generated'] = len(final_data)
        metrics['success'] = True
        
        logger.info(f"Inserted {metrics['scores_generated']} rows into dev.stg_case_cat_zeroshot")
        
        conn.unregister('scores_temp')
        
    except Exception as e:
        logger.error(f"Classification batch failed: {str(e)}", exc_info=True)
        metrics['error'] = str(e)
        
    finally:
        if 'conn' in locals():
            conn.close()
    
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Classify batches of cases and store results in stg_case_cat_zeroshot'
    )
    parser.add_argument(
        '--size', type=int, default=100,
        help='Number of cases to process (default: 100)'
    )
    parser.add_argument(
        '--verbosity', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO', help='Set logging verbosity'
    )

    args = parser.parse_args()

    # Set logging level
    logger.setLevel(getattr(logging, args.verbosity.upper()))

    logger.info(f"Starting batch classification with size={args.size}")
    metrics = classify_case_batch(size=args.size)

    if metrics['success']:
        logger.info(
            f"Batch completed: {metrics['cases_fetched']} cases, "
            f"{metrics['scores_generated']} scores, "
            f"{metrics['rows_inserted']} rows inserted"
        )
    else:
        logger.error(f"Batch failed: {metrics['error']}")
        exit(1)
