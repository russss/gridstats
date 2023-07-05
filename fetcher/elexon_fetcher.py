from collections import defaultdict
from typing import Optional
import httpx
import logging
import asyncio
from dateutil.parser import parse as parse_date
from datetime import date, datetime, timedelta
import time
import csv

from carbonintensity import CarbonIntensity
from sources import NGESO
from database import DB
from wikidata import Wikidata
from config import config


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
    def __init__(self, api_key):
        self.log = logging.getLogger(__name__)
        self.client = httpx.AsyncClient()
        self.ngeso = NGESO()
        self.intensity = CarbonIntensity()
        self.wikidata = Wikidata()
        self.db = DB()
        self.api_key = api_key
        self.fetch_jobs = []

    async def elexon_fetch(self, path: str, **params):
        base_url = "https://data.elexon.co.uk/bmrs/api/v1/"
        res = await self.client.get(base_url + path, params=elexon_params(params))
        if res.status_code != 200:
            raise Exception(f"Elexon fetch failed: {res.text}")
        return res.json()

    async def portal_fetch(self, file: str):
        url = "https://downloads.elexonportal.co.uk/file/download/"
        url += file
        url += "?key=" + self.api_key
        res = await self.client.get(url)
        rows = [r for r in res.text.split("\n") if "," in r]
        return csv.DictReader(rows)

    def register_fetcher(self, fetch_func, frequency: int):
        self.fetch_jobs.append(FetchJob(fetch_func, frequency))

    async def run(self):
        self.log.info("Initialising...")
        await self.db.connect()

        self.register_fetcher(self.fetch_parties, 43200)
        self.register_fetcher(self.fetch_units, 3600)
        self.register_fetcher(self.fetch_units_detail, 43200)
        self.register_fetcher(self.fetch_fuel_types, 3600)
        self.register_fetcher(self.fetch_wikidata_plants, 900)

        self.register_fetcher(self.fetch_rolling_system_demand, 30)

        self.register_fetcher(self.fetch_carbon_intensity, 120)
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
                        async with self.db.db.transaction():
                            await job.func()
                    except Exception:
                        self.log.exception(f"Error running {job}")

                    duration = time.perf_counter() - start
                    self.log.info(f"Running {job} took {duration:.2f}s")
                    job.last_run = now

            await asyncio.sleep(2)

    async def fetch_carbon_intensity(self):
        data = await self.intensity.get_intensity(
            datetime.now() - timedelta(days=14), datetime.now() + timedelta(days=14)
        )

        await self.db.execute_many(
            query="""INSERT INTO carbon_intensity_national (time, intensity) VALUES (:time, :intensity)
                        ON CONFLICT (time) DO UPDATE SET intensity=:intensity""",
            values=[
                {
                    "time": parse_date(row["from"]),
                    "intensity": row["intensity"]["actual"],
                }
                for row in data
                if row["intensity"]["actual"] is not None
            ],
        )

        await self.db.execute_many(
            query="""INSERT INTO carbon_intensity_national_forecast (time, intensity) VALUES (:time, :intensity)
                        ON CONFLICT (time) DO UPDATE SET intensity=:intensity""",
            values=[
                {
                    "time": parse_date(row["from"]),
                    "intensity": row["intensity"]["forecast"],
                }
                for row in data
                if row["intensity"]["forecast"] is not None
            ],
        )

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

    async def fetch_units(self):
        data = await self.elexon_fetch("reference/bmunits/all")
        fuel_types = {
            row["ref"]: row["id"]
            for row in await self.db.db.fetch_all("SELECT id, ref FROM fuel_type")
        }

        for row in data:
            # Check if the row exists by National Grid or Elexon reference
            res = await self.db.db.fetch_one(
                "SELECT id FROM bm_unit WHERE ng_ref = :ng_ref OR elexon_ref = :elexon_ref",
                {
                    "ng_ref": row["nationalGridBmUnit"],
                    "elexon_ref": row["elexonBmUnit"],
                },
            )

            params = {
                "elexon_ref": row["elexonBmUnit"],
                "ng_ref": row["nationalGridBmUnit"],
                "fuel": fuel_types.get(row["fuelType"]),
                "party_name": row["leadPartyName"],
                "type": row["bmUnitType"],
                "fpn": row["fpnFlag"],
            }

            if res:
                await self.db.execute(
                    """UPDATE bm_unit SET party_name = :party_name, elexon_ref = :elexon_ref,
                                ng_ref = :ng_ref,
                                fuel = :fuel, fpn = :fpn, type=:type, last_seen = now()""",
                    params,
                )
            else:
                await self.db.execute(
                    """INSERT INTO bm_unit (ng_ref, elexon_ref, fuel, party_name, type, fpn)
                                    VALUES (:ng_ref, :elexon_ref, :fuel, :party_name, :type, :fpn)""",
                    params,
                )

    async def fetch_units_detail(self):
        data = await self.portal_fetch("REGISTERED_BMUNITS_FILE")

        gsp_region = {
            row["gsp_group"]: row["id"]
            for row in await self.db.db.fetch_all("SELECT id, gsp_group FROM region")
        }

        pc = {
            "P": "producer",
            "C": "consumer",
        }

        await self.db.execute_many(
            query="""INSERT INTO bm_unit (ng_ref, elexon_ref,
                        name, region, participant, prod_cons)
                    VALUES (:ng_ref, :elexon_ref,
                        :name, :region, :participant, :prod_cons)
                    ON CONFLICT (elexon_ref) DO UPDATE
                        SET name = :name, region = :region, participant = :participant,
                        prod_cons = :prod_cons, last_seen = now()
            """,
            values=[
                {
                    "ng_ref": row["NGC BMU Name"]
                    if row["NGC BMU Name"] != ""
                    else None,
                    "elexon_ref": row["BM Unit ID"],
                    "name": row["BMU Name"]
                    if row["BMU Name"] != row["BM Unit ID"]
                    else None,
                    "region": gsp_region.get(row["GSP Group Id"]),
                    "participant": row["Party ID"],
                    "prod_cons": pc.get(row["Prod/Cons Flag"]),
                }
                for row in data
            ],
        )

    async def fetch_parties(self):
        data = await self.portal_fetch("REGISTERED_PARTICIPANTS_FILE")
        await self.db.execute_many(
            query="""INSERT INTO participant (ref, name) VALUES (:ref, :name)
                        ON CONFLICT (ref) DO UPDATE SET name = :name, last_seen = now()
                    """,
            values=[
                {"ref": row["Trading Party ID"], "name": row["Trading Party Name"]}
                for row in data
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

    async def fetch_wikidata_plants(self):
        plant_names = {}
        bmrs_ids = defaultdict(list)
        for item in self.wikidata.get_plants():
            plant_names[item["id"]] = item["name"]
            bmrs_ids[item["id"]].append(item["bmrs_id"])

        await self.db.execute_many(
            """INSERT INTO wikidata_plants (wd_id, name) VALUES (:wd_id, :name) ON CONFLICT (wd_id) DO UPDATE SET name = :name""",
            values=[{"wd_id": k, "name": v} for k, v in plant_names.items()],
        )

        for wd_id, bm_units in bmrs_ids.items():
            unit_ids = []
            for bm_unit in bm_units:
                res = await self.db.db.fetch_one(
                    "SELECT id FROM bm_unit WHERE ng_ref = :bm_unit",
                    values={"bm_unit": bm_unit},
                )
                if res is None:
                    self.log.info(f"Missing bm_unit from Wikidata: {bm_unit} ({wd_id})")
                    continue
                unit_ids.append(res["id"])

            await self.db.execute_many(
                """INSERT INTO wd_bm_unit (wd_id, bm_unit) VALUES (:wd_id, :bm_unit)
                                        ON CONFLICT (bm_unit) DO UPDATE SET wd_id = :wd_id""",
                values=[{"wd_id": wd_id, "bm_unit": e} for e in unit_ids],
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(ElexonFetcher(config["ELEXON_API_KEY"]).run())
