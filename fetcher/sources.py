import logging
import httpx
from datetime import date
from settlement_periods import sp_to_datetime


class NGESO:
    ENDPOINT = "https://data.nationalgrideso.com/api/3/action/datastore_search_sql"

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.client = httpx.AsyncClient()

    async def fetch_sql(self, query: str):
        query = query.replace("\n", "").strip()
        self.log.info(f"Running NGESO query: {query}")
        res = await self.client.get(self.ENDPOINT, params={"sql": query})
        data = res.json()
        if data["success"] != True:
            raise Exception("NGESO query error")

        return data["result"]

    async def get_demand_data_update(self, forecast=False):
        if forecast:
            forecast_indicator = "F"
            order = "ASC"
        else:
            forecast_indicator = "A"
            order = "DESC"

        data = await self.fetch_sql(
            f"""SELECT * FROM "177f6fa4-ae49-4182-81ea-0c6b35f26ca6"
                WHERE "FORECAST_ACTUAL_INDICATOR" = '{forecast_indicator}'
                ORDER BY "SETTLEMENT_DATE" {order}
                LIMIT 250"""
        )

        for record in data["records"]:
            record["time"] = sp_to_datetime(
                date.fromisoformat(record["SETTLEMENT_DATE"]),
                int(record["SETTLEMENT_PERIOD"]),
            )

        return data["records"]
