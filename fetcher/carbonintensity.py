import httpx
from datetime import datetime


class CarbonIntensity:
    """UK carbon intensity API: https://api.carbonintensity.org.uk/"""

    def __init__(self):
        self.client = httpx.AsyncClient()

    async def fetch(self, path: str):
        base_url = "https://api.carbonintensity.org.uk/"
        res = await self.client.get(base_url + path)
        if res.status_code != 200:
            raise Exception(f"Carbon intensity fetch failed: {res.text}")
        return res.json()

    async def get_intensity(self, from_date: datetime, to_date: datetime):
        data = await self.fetch(
            f"intensity/{from_date.isoformat()}/{to_date.isoformat()}"
        )

        return data["data"]
