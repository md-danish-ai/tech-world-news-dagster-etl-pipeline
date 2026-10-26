from dagster import Definitions, load_assets_from_modules
from src.assets import bronze, silver, gold
from src.resources.hn_api import HackerNewsResource

all_assets = [
    *load_assets_from_modules([bronze, silver, gold]),
]

defs = Definitions(
    assets=all_assets,
    resources={
        "hn_api": HackerNewsResource(),
    },
)
