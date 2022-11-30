import asyncio
import logging

from bmrs_client import BMRSClient
from dateutil.parser import parse as parse_date

from config import config
from settlement_periods import sp_to_datetime
from database import DB


def parse_elexon_timestamp(ts: str):
    return parse_date(ts.replace(":GMT", "Z"))


def unit_from_subject(subject: str):
    parts = subject.split(".")
    if parts[1] in ("DYNAMIC", "BM", "RR"):
        return parts[2]
    return None


class PushListener:
    def __init__(self):
        self.db = DB()
        self.log = logging.getLogger(__name__)

        self.client = BMRSClient(config["ELEXON_API_KEY"])
        self.client.register_handler("LOLPDM", self.handle_lolp_dm)
        self.client.register_handler("FREQ", self.handle_freq)
        self.client.register_handler("FUELINST", self.handle_fuelinst)
        self.client.register_handler("SYS_WARN", self.handle_sys_warn)
        self.client.register_handler("SEL", self.handle_sel)
        self.client.register_handler("MEL", self.handle_mel)
        self.client.register_handler("MELS", self.handle_mel)
        self.client.register_handler("MIL", self.handle_mil)
        self.client.register_handler("MILS", self.handle_mil)
        self.client.register_handler("PN", self.handle_pn)
        self.client.register_handler("FPN", self.handle_pn)

    async def run(self):
        await self.db.connect()
        await self.client.run()

    async def handle_freq(self, doc):
        self.log.info("Handling FREQ message")
        await self.db.execute_many(
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
        await self.db.execute_many(
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

    async def handle_sys_warn(self, doc):
        self.log.info("Handling SYS_WARN message")
        await self.db.execute_many(
            query="""INSERT INTO system_warning (time, warning) VALUES (:time, :warning)
                    ON CONFLICT (time) DO UPDATE SET warning = :warning""",
            values=[
                {
                    "time": parse_elexon_timestamp(row.find("TP").text),
                    "warning": row.find("SW").text,
                }
                for row in doc.iter("row")
            ],
        )

    async def handle_sel(self, doc):
        subject = doc.find("subject").text
        unit = unit_from_subject(subject)
        self.log.info(f"Handling SEL ({unit})")
        await self.db.execute_many(
            query="""INSERT INTO stable_export_limit (time, unit, export_limit)
                        VALUES (:time, :unit, :export_limit)
                    ON CONFLICT (time, unit) DO UPDATE SET export_limit = :export_limit""",
            values=[
                {
                    "time": parse_elexon_timestamp(row.find("TE").text),
                    "unit": unit,
                    "export_limit": float(row.find("SE").text),
                }
                for row in doc.iter("row")
            ],
        )

    async def handle_mel(self, doc):
        subject = doc.find("subject").text
        unit = unit_from_subject(subject)
        self.log.info(f"Handling MEL ({unit})")
        await self.db.execute_many(
            query="""INSERT INTO maximum_export_limit (time, unit, export_limit)
                        VALUES (:time, :unit, :export_limit)
                    ON CONFLICT (time, unit) DO UPDATE SET export_limit = :export_limit""",
            values=[
                {
                    "time": parse_elexon_timestamp(row.find("TS").text),
                    "unit": unit,
                    "export_limit": float(row.find("VE").text),
                }
                for row in doc.iter("row")
            ],
        )

    async def handle_mil(self, doc):
        subject = doc.find("subject").text
        unit = unit_from_subject(subject)
        self.log.info(f"Handling MIL ({unit})")
        await self.db.execute_many(
            query="""INSERT INTO maximum_import_limit (time, unit, import_limit)
                        VALUES (:time, :unit, :import_limit)
                    ON CONFLICT (time, unit) DO UPDATE SET import_limit = :import_limit""",
            values=[
                {
                    "time": parse_elexon_timestamp(row.find("TS").text),
                    "unit": unit,
                    "import_limit": float(row.find("VF").text),
                }
                for row in doc.iter("row")
            ],
        )

    async def handle_pn(self, doc):
        subject = doc.find("subject").text
        unit = unit_from_subject(subject)
        self.log.info(f"Handling PN ({unit})")
        await self.db.execute_many(
            query="""INSERT INTO physical_notification (time, unit, level)
                        VALUES (:time, :unit, :level)
                    ON CONFLICT (time, unit) DO UPDATE SET level = :level""",
            values=[
                {
                    "time": parse_elexon_timestamp(row.find("TS").text),
                    "unit": unit,
                    "level": float(row.find("VP").text),
                }
                for row in doc.iter("row")
            ],
        )

    async def handle_lolp_dm(self, doc):
        self.log.info("Handling LOLPDM")
        await self.db.execute_many(
            query="""INSERT INTO lolp_dm (time, loss_of_load_probability, derated_margin)
                        VALUES (:time, :lolp, :dm)
                        ON CONFLICT (time) DO UPDATE SET loss_of_load_probability = :lolp,
                            derated_margin = :dm
                """,
            values=[
                {
                    "time": sp_to_datetime(
                        parse_date(row.find("SD").text), int(row.find("SP").text)
                    ),
                    "lolp": float(row.find("LP").text),
                    "dm": float(row.find("DR").text),
                }
                for row in doc.iter("row")
            ],
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(PushListener().run())
