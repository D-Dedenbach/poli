import yaml
from pathlib import Path

config_path = Path(__file__).parent.parent / "backend" / "configs" / "categories.yml"

def load_categories() -> list[str]:
    """
    Helper function to load categories from a .yml file.
    Intended for use with backend/configs/categories.yml.

    Returns a list of category names and descriptions in the format "name: description".
    """

    with config_path.open() as f:
        categories = yaml.safe_load(f)["categories"]

    return categories

def load_slug_category_names() -> list[str]:
    """
    Each category has a slug name to get around Nordic characters an upper/lower chars. 
    This function returns a list of them
    """

    with config_path.open() as f:
        categories = yaml.safe_load(f)["categories"]

    return [cat['slug'] for cat in categories]


def get_categories_version() -> int:
    """
    Helper function to get the version from categories.yml.
    """
    with config_path.open() as f:
        config = yaml.safe_load(f)
    return config.get("version", 1)

    
    