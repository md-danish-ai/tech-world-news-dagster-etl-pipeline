import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dagster import ConfigurableResource

# Defines the HackerNewsResource class as a Dagster ConfigurableResource.

class HackerNewsResource(ConfigurableResource):
    """Resource to interact with the Hacker News Firebase REST API."""
    base_url: str = "https://hacker-news.firebaseio.com/v0"
    timeout: int = 15

    def _get_session(self) -> requests.Session:
        """Create a requests session with automatic retries and exponential backoff."""
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,  # waits 1s, 2s, 4s...
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get_top_story_ids(self, limit: int = 50) -> list[int]:
        """Fetch the top story IDs from Hacker News front page."""
        session = self._get_session()
        url = f"{self.base_url}/topstories.json"
        response = session.get(url, timeout=self.timeout)
        response.raise_for_status()
        ids = response.json() or []
        return ids[:limit]

    def get_item(self, item_id: int) -> dict | None:
        """Fetch raw JSON payload for a single item (story/post)."""
        session = self._get_session()
        url = f"{self.base_url}/item/{item_id}.json"
        try:
            response = session.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
        except requests.RequestException:
            return None
        return None

        
    def get_top_stories(self, limit: int = 50) -> list[dict]:
        """Fetch top story IDs and retrieve their full raw JSON dictionaries."""
        story_ids = self.get_top_story_ids(limit=limit)
        stories = []
        for sid in story_ids:
            item = self.get_item(sid)
            if item:
                stories.append(item)
        return stories