from transformers import pipeline
from datetime import datetime
from tqdm import tqdm
from pathlib import Path

import duckdb
import pandas as pd
import logging
import argparse
import sys

from backend.configs.database import DB_PATH
from utils.categorization_utils import (load_categories, 
                                        load_slug_category_names,
                                        get_categories_version)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Classifier specs - current version is for internal tracking of changes
classifier = pipeline("zero-shot-classification", model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli")
current_version = 1


def classify_text(text):
    """
    Classify the input text into one or more of the candidate labels using zero-shot classification.
    For each label a confidence score between 0 an 1 is returned.

    Parameters:
    - text (str): The input text to classify.
    - candidate_labels (list of str): The list of candidate labels to classify the text into
    """
    
    # Categories are fixed
    cat = load_categories()
    cat_slugs = load_slug_category_names()

    result = classifier(text, cat, multi_label=True)
    labelled_scores = list(zip(cat_slugs, result['scores']))

    df_scores = pd.DataFrame(labelled_scores, columns=['category', 'score'])
    # Turn scores into dict and label with slug names in order to load into df
    return df_scores



def classify_case_batch(size: int = 100):
    """
    Function Queries uncategorized cases from stg_case and runs a classifier on them.
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
        # Get versions of categories and classifier that the compute will work on
        cat_version = get_categories_version()
        classifier_version = current_version
        batch_timestamp = datetime.now().isoformat()

        logger.info(f"Starting classification batch | cat_version={cat_version}, classifier_version={classifier_version}, size={size}")

        # Case data will be in the stg_case table. Usually text is in the title and sometimes in the reasoning column.
        fetch_qry = f"""
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

        # Fetch case data from database
        conn = duckdb.connect(database=DB_PATH)
        df = conn.execute(fetch_qry).fetchdf()
        metrics['cases_fetched'] = len(df)
        logger.info(f"Fetched {metrics['cases_fetched']} uncategorized cases")

        if df.empty:
            logger.info("No cases to process - exiting")
            metrics['success'] = True
            return metrics

        # Create input for the classifier model and apply classifier. 
        df['categorization_text'] = df['case_title'].fillna('') + ': ' + df['case_reasoning'].fillna('')

        # Apply classification and extract scores to df
        scores_data = []  # List of dicts
        for _, row in tqdm(df.iterrows(), total=len(df), desc='Classifying cases'):
            df_row_scores = classify_text(row['categorization_text'])
            scores_data.extend({
                'case_id': row['id'],
                'cat_name': score_row['category'],
                'confidence_score': score_row['score'],
                'cat_version': cat_version,
                'classifier_version': classifier_version,
                'created_at': batch_timestamp
            } for _, score_row in df_row_scores.iterrows())

        # This is the final product of data 
        df_scores = pd.DataFrame(scores_data)
        metrics['scores_generated'] = len(df_scores)
        logger.info(f"Generated {metrics['scores_generated']} scores for {df['id'].nunique()} cases")

        # Insert. Column names must be specified in insert statement due to auto-increment ID column not populated explicitly
        # Column names must also be specified in SELECT statement to ensure correct matching of df to table columns
        insert_qry = """
        INSERT INTO dev.stg_case_cat_zeroshot (case_id, cat_name, confidence_score, cat_version, created_at, classifier_version)
        SELECT case_id, cat_name, confidence_score, cat_version, created_at, classifier_version FROM scores_temp;
        """
        
        # In order for duckdb to be able to use the table, it needs to register
        conn.register('scores_temp', df_scores)
        result = conn.execute(insert_qry)
        metrics['rows_inserted'] = len(df_scores)
        logger.info(f"Inserted {metrics['rows_inserted']} rows created at {batch_timestamp} into dev.stg_case_cat_zeroshot")
        
        conn.unregister('scores_temp')
        metrics['success'] = True
        
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
