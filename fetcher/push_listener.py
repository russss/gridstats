import asyncio
import logging
from bmrs_client import BMRSClient
import xml.etree.ElementTree as ET
from dateutil.parser import parse as parse_date

from config import config
from database import DB


def parse_elexon_timestamp(ts: str):
    return parse_date(ts.replace(":GMT", "Z"))


class PushListener:
    def __init__(self):
        self.db = DB()
        self.log = logging.getLogger(__name__)

        self.client = BMRSClient(config["ELEXON_API_KEY"])
        self.client.register_handler("FREQ", self.handle_freq)
        self.client.register_handler("FUELINST", self.handle_fuelinst)

    async def run(self):
        await self.db.connect()
        await self.client.run()

    async def handle_freq(self, doc):
        self.log.info("Handling FREQ message")
        await self.db.db.execute_many(
            query="""INSERT INTO frequency(time, frequency)
                        VALUES (:time, :frequency)
                        ON CONFLICT (time) DO UPDATE SET frequency=:frequency""",
            values=[
                {
                    "time": parse_elexon_timestamp(row.find("TS").text),
                    "frequency": float(row.find("SF").text),
                }
                for row in doc.iter("row")
            ],
        )

    async def handle_fuelinst(self, doc):
        self.log.info("Handling FUELINST message")
        await self.db.db.execute_many(
            query="""INSERT INTO generation_by_fuel_type_inst
                        (time, fuel_type, generation)
                        VALUES (:time, :type, :generation)
                        ON CONFLICT (time, fuel_type) DO UPDATE
                            SET generation=:generation
            """,
            values=[
                {
                    "time": parse_elexon_timestamp(row.find("TS").text),
                    "type": await self.db.get_fuel_type(row.find("FT").text),
                    "generation": int(row.find("FG").text),
                }
                for row in doc.iter("row")
            ],
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(PushListener().run())
