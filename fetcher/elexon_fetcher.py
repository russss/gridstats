from typing import Optional
import httpx
import logging
import asyncio
from dateutil.parser import parse as parse_date
from datetime import date, datetime, timedelta
import time

from sources import NGESO
from database import DB


def elexon_params(params: dict):
    if params is None or len(params) == 0:
        return None

    for key in params.keys():
        if key in ("from", "to", "settlementDateFrom", "settlementDateTo"):
            params[key] = params[key].isoformat("T")

    return params


class FetchJob:
    def __init__(self, func, frequency: int):
        self.func = func
        self.frequency = frequency
        self.last_run: Optional[datetime] = None

    def __str__(self) -> str:
        return f"Job<{self.func.__name__} ({self.frequency}s)>"


class ElexonFetcher:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.client = httpx.AsyncClient()
        self.ngeso = NGESO()
        self.db = DB()
        self.fetch_jobs = []

    async def elexon_fetch(self, path: str, **params):
        base_url = "https://data.elexon.co.uk/bmrs/api/v1/"
        res = await self.client.get(base_url + path, params=elexon_params(params))
        if res.status_code != 200:
            raise Exception(f"Elexon fetch failed: {res.text}")
        return res.json()

    def register_fetcher(self, fetch_func, frequency):
        self.fetch_jobs.append(FetchJob(fetch_func, frequency))

    async def run(self):
        self.log.info("Initialising...")
        await self.db.connect()

        self.register_fetcher(self.fetch_fuel_types, 3600)

        self.register_fetcher(self.fetch_rolling_system_demand, 30)
        # self.register_fetcher(self.fetch_generation_inst, 30)

        self.register_fetcher(self.fetch_demand_outturn, 120)
        self.register_fetcher(self.fetch_generation_hh, 120)
        self.register_fetcher(self.fetch_demand_data_update, 120)
        self.register_fetcher(self.fetch_demand_forecast, 120)
        self.register_fetcher(self.fetch_pv_live, 120)

        while True:
            for job in self.fetch_jobs:
                now = datetime.utcnow()
                if (
                    job.last_run is None
                    or (now - job.last_run).total_seconds() > job.frequency
                ):

                    start = time.perf_counter()
                    try:
                        await job.func()
                    except Exception:
                        self.log.exception(f"Error running {job}")

                    duration = time.perf_counter() - start
                    self.log.info(f"Running {job} took {duration:.2f}s")
                    job.last_run = now

            await asyncio.sleep(2)

    async def fetch_demand_data_update(self):
        data = await self.ngeso.get_demand_data_update()

        await self.db.execute_many(
            query="""INSERT INTO embedded_generation (time, solar_generation, solar_capacity, wind_generation, wind_capacity)
                        VALUES (:time, :solar_gen, :solar_cap, :wind_gen, :wind_cap)
                        ON CONFLICT (time) DO UPDATE
                            SET solar_generation = :solar_gen, solar_capacity = :solar_cap,
                                wind_generation = :wind_gen, wind_capacity = :wind_cap""",
            values=[
                {
                    "time": row["time"],
                    "solar_cap": int(row["EMBEDDED_SOLAR_CAPACITY"]),
                    "solar_gen": int(row["EMBEDDED_SOLAR_GENERATION"]),
                    "wind_cap": int(row["EMBEDDED_WIND_CAPACITY"]),
                    "wind_gen": int(row["EMBEDDED_WIND_GENERATION"]),
                }
                for row in data
            ],
        )

        forecast_data = await self.ngeso.get_demand_data_update(forecast=True)
        await self.db.execute_many(
            query="""INSERT INTO embedded_generation_forecast (time, solar_generation, solar_capacity, wind_generation, wind_capacity)
                        VALUES (:time, :solar_gen, :solar_cap, :wind_gen, :wind_cap)
                        ON CONFLICT (time) DO UPDATE
                            SET solar_generation = :solar_gen, solar_capacity = :solar_cap,
                                wind_generation = :wind_gen, wind_capacity = :wind_cap""",
            values=[
                {
                    "time": row["time"],
                    "solar_cap": int(row["EMBEDDED_SOLAR_CAPACITY"]),
                    "solar_gen": int(row["EMBEDDED_SOLAR_GENERATION"]),
                    "wind_cap": int(row["EMBEDDED_WIND_CAPACITY"]),
                    "wind_gen": int(row["EMBEDDED_WIND_GENERATION"]),
                }
                for row in forecast_data
            ],
        )

    async def fetch_pv_live(self):
        # https://www.solar.sheffield.ac.uk/pvlive/api/

        from_date = datetime.utcnow() - timedelta(hours=24)

        res = await self.client.get(
            f"https://api0.solar.sheffield.ac.uk/pvlive/api/v4/gsp/0",
            params={"start": from_date.isoformat("T"), "period": 5},
        )
        data = res.json()
        await self.db.execute_many(
            query="""INSERT INTO pv_live(time, pv_generation)
                        VALUES (:time, :generation)
                        ON CONFLICT (time) DO UPDATE SET pv_generation = :generation
                        """,
            values=[
                {
                    "time": parse_date(row[1]),
                    "generation": row[2],
                }
                for row in data["data"]
            ],
        )

    async def fetch_generation_hh(self, **params):
        # Fetch half-hourly generation numbers
        # Note: the Elexon generation/outturn/summary endpoint provides similar data but
        # does not report negative interconnector flows.

        data = await self.elexon_fetch("datasets/FUELHH", **params)
        await self.db.execute_many(
            query="""INSERT INTO generation_by_fuel_type_hh
                        (time, settlement_period, fuel_type, generation)
                        VALUES (:time, :settlement_period, :type, :generation)\
                        ON CONFLICT (time, fuel_type) DO UPDATE
                            SET settlement_period=:settlement_period, generation=:generation
            """,
            values=[
                {
                    "time": parse_date(row["startTime"]),
                    "type": await self.db.get_fuel_type(row["fuelType"]),
                    "settlement_period": row["settlementPeriod"],
                    "generation": row["generation"],
                }
                for row in data["data"]
            ],
        )

    async def fetch_generation_inst(self, **params):
        # Fetch instantaneous (5-minute) generation numbers

        if "settlementDateFrom" not in params:
            params["settlementDateFrom"] = datetime.utcnow() - timedelta(minutes=30)
            params["settlementDateTo"] = datetime.utcnow()

        data = await self.elexon_fetch("datasets/FUELINST", **params)
        await self.db.execute_many(
            query="""INSERT INTO generation_by_fuel_type_inst
                        (time, fuel_type, generation)
                        VALUES (:time, :type, :generation)\
                        ON CONFLICT (time, fuel_type) DO UPDATE
                            SET generation=:generation
            """,
            values=[
                {
                    "time": parse_date(row["startTime"]),
                    "type": await self.db.get_fuel_type(row["fuelType"]),
                    "generation": row["generation"],
                }
                for row in data["data"]
            ],
        )

    async def fetch_fuel_types(self):
        data = await self.elexon_fetch("reference/fueltypes/all")
        await self.db.execute_many(
            query="INSERT INTO fuel_type (ref) VALUES (:type) ON CONFLICT DO NOTHING",
            values=[{"type": fuel_type} for fuel_type in data],
        )

        interconnectors = await self.elexon_fetch("reference/interconnectors/all")
        await self.db.execute_many(
            query="UPDATE fuel_type SET name = :name, interconnector = TRUE WHERE ref = :ref AND interconnector = FALSE",
            values=[
                {"ref": row["interconnectorId"], "name": row["interconnectorName"]}
                for row in interconnectors
            ],
        )

    async def fetch_rolling_system_demand(self, **params):
        data = await self.elexon_fetch("demand/rollingSystemDemand", **params)
        query = "INSERT INTO system_demand (time, demand) VALUES (:time, :demand) ON CONFLICT (time) DO UPDATE SET demand=:demand"

        await self.db.execute_many(
            query=query,
            values=[
                {"time": parse_date(row["startTime"]), "demand": row["demand"]}
                for row in data["data"]
            ],
        )

    async def fetch_demand_outturn(self, **params):
        data = await self.elexon_fetch("demand/stream", **params)
        query = """
            INSERT INTO initial_demand_outturn (time, settlement_date, settlement_period, demand_outturn, transmission_demand_outturn)
                VALUES (:time, :date, :period, :demand_outturn, :transmission_demand_outturn)
                ON CONFLICT (time) DO UPDATE
                    SET demand_outturn=:demand_outturn, transmission_demand_outturn=:transmission_demand_outturn
        """

        await self.db.execute_many(
            query=query,
            values=[
                {
                    "time": parse_date(row["startTime"]),
                    "date": date.fromisoformat(row["settlementDate"]),
                    "period": row["settlementPeriod"],
                    "demand_outturn": row["initialDemandOutturn"],
                    "transmission_demand_outturn": row[
                        "initialTransmissionSystemDemandOutturn"
                    ],
                }
                for row in data
            ],
        )

    async def fetch_demand_forecast(self, **params):
        data = await self.elexon_fetch("forecast/demand/day-ahead", **params)
        await self.db.execute_many(
            query="""INSERT INTO demand_forecast (time, settlement_period, transmission_demand, national_demand)
                        VALUES (:time, :settlement_period, :transmission_demand, :national_demand)
                        ON CONFLICT (time) DO UPDATE
                            SET transmission_demand=:transmission_demand, national_demand=:national_demand
                    """,
            values=[
                {
                    "time": parse_date(row["startTime"]),
                    "settlement_period": row["settlementPeriod"],
                    "transmission_demand": row["transmissionSystemDemand"],
                    "national_demand": row["nationalDemand"],
                }
                for row in data["data"]
            ],
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(ElexonFetcher().run())
