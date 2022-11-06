import logging
import asyncio
import ssl

from aiostomp import AioStomp
import xml.etree.ElementTree as ET


class BMRSClient:
    HOST = "api.bmreports.com"
    PORT = 61613

    def __init__(self, api_key):
        self.log = logging.getLogger(__name__)
        self.api_key = api_key
        self.handlers = {}

    def register_handler(self, message_type, handler):
        self.handlers[message_type] = handler

    async def run(self):
        client = AioStomp(
            self.HOST,
            self.PORT,
            error_handler=self.report_error,
            ssl_context=ssl.create_default_context(),
            heartbeat_interval_cx=0,
            heartbeat_interval_cy=10000,
        )
        client.subscribe("/topic/bmrsTopic", handler=self.on_message)

        self.log.info("Connecting")

        await client.connect(self.api_key, self.api_key)

        while True:
            await asyncio.sleep(1)

    async def report_error(self, error):
        self.log.error("Stomp error: %s", error)

    async def on_message(self, frame, message):
        handler = self.handlers.get(frame.headers["type"])
        if handler:
            try:
                doc = ET.fromstring(message.decode("utf-8"))
                if doc.tag == "msgGrp":
                    for msg in doc.iter("msg"):
                        await handler(msg)
                else:
                    await handler(doc)

            except Exception:
                self.log.exception(f"Exception in handler for message {message}")
                self.log.error("Message was: %s", message.decode("utf-8"))
        return True
