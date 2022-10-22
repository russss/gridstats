from databases import Database
from asyncstdlib.functools import lru_cache
from config import config


class DB:
    def __init__(self):
        self.db = Database(config["DATABASE_URL"])

    @lru_cache()
    async def get_fuel_type(self, ref: str) -> int:
        res = await self.db.execute(
            query="SELECT id FROM fuel_type WHERE ref = :ref", values={"ref": ref}
        )
        if res is None:
            raise Exception(f"Missing fuel type: {ref}")
        return res

    async def connect(self):
        return await self.db.connect()

    async def execute(self, *args, **kwargs):
        return await self.db.execute(*args, **kwargs)

    async def execute_many(self, *args, **kwargs):
        return await self.db.execute_many(*args, **kwargs)
