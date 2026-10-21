from dagster import Definitions, load_assets_from_modules
from src.assets import bronze
from src.resources.hn_api import HackerNewsResource

all_assets = [
    *load_assets_from_modules([bronze]),
]

defs = Definitions(
    assets=all_assets,
    resources={
        "hn_api": HackerNewsResource(),
    },
)
