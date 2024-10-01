
import asyncio

import aiohttp
import random
import re

from logger import log

from dtypes.order import Order

from config import USER_AGENTS, MOMOX_KEY_REGEX, MOMOX_VERSION_REGEX, REQUESTS_LIMIT


semaphore = asyncio.Semaphore(REQUESTS_LIMIT)


class Parser:
    def __init__(self):
        self.log = log.bind(classname=f"{self.__class__.__name__}")
        self.sessions = {}

    async def create_session(self, proxy: str) -> aiohttp.ClientSession:
        try:
            session = aiohttp.ClientSession()

            agent = random.choice(USER_AGENTS)
            session.user_agent = {
                "User-Agent": agent[0],
                "Sec-Ch-Ua": f'"Chromium";v="{agent[2]}", "Not;A=Brand";v="24", "Google Chrome";v="{agent[2]}"',
                "Sec-Ch-Ua-Platform": f'"{agent[1]}"',
                "Sec-Ch-Ua-Mobile": "?0"
            }

            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-GB,en;q=0.9",
                "Cache-Control": "max-age=0",
                "Priority": "u=0, i",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "sec-fetch-user": "?1",
                "Upgrade-Insecure-Requests": "1",
                **session.user_agent
            }

            async with semaphore:
                try:
                    async with session.get(
                        url="https://www.momox.de/",
                        headers=headers,
                        proxy=proxy,
                    ) as resp:
                        if not resp.status == 200:
                            self.log.warning(f"Can't create session -> {proxy} with agent -> {agent}")
                            self.log.debug(resp)
                            await session.close()
                            return

                        self.log.info(f"Session {proxy} -> {resp.status}")
                except Exception as err:
                    await session.close()
                    self.log.exception(err)
                    return

            self.log.info(f"Created session -> {proxy} with agent -> {agent}")
            return session

        except Exception as err:
            self.log.exception(err)

    async def __update_order(self, session, order: Order, proxy: str) -> Order:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-GB,en;q=0.9",
            "Cache-Control": "max-age=0",
            "Priority": "u=0, i",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "sec-fetch-user": "?1",
            "Upgrade-Insecure-Requests": "1",
            **session.user_agent
        }

        async with semaphore:
            try:
                async with session.get(
                    url=f"https://www.momox.de/offer/{order.get_ean()}",
                    headers=headers,
                    proxy=proxy,
                ) as resp:
                    if resp.status != 200:
                        self.log.warning(f"Can't connect to momox -> {proxy} with order -> {order}")
                        self.log.debug(resp)
                        await session.close()
                        del self.sessions[proxy]
                        return

                    data = await resp.text()
                    client_key = re.search(MOMOX_KEY_REGEX, data)
                    version = re.search(MOMOX_VERSION_REGEX, data)

                    api_key = client_key.group(1) if client_key else None
                    api_version = version.group(1).split("-", 1)[-1] if version else None

                    if None in [api_key, api_version]:
                        self.log.warning(f"Can't find api_key and api_version -> {proxy} with order -> {order}")
                        self.log.debug(resp)
                        await session.close()
                        del self.sessions[proxy]
                        return

            except Exception as err:
                await session.close()
                self.log.exception(err)
                return

        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7",
            "Content-type": "application/json",
            "Origin": "https://www.momox.de",
            "Priority": "u=1, i",
            "Referer": "https://www.momox.de/",
            **session.user_agent,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "X-Api-Token": api_key,
            "X-Client-Version": api_version,
            "X-Marketplace-Id": "momox_de"
        }

        async with semaphore:
            try:
                async with session.get(
                    url=f"https://api.momox.de/api/v4/media/offer/?ean={order.get_ean()}",
                    headers=headers,
                    proxy=proxy
                ) as resp:
                    if resp.status != 200:
                        self.log.warning(f"Can't connect to momox api -> {proxy} for order -> {order}")
                        self.log.debug(resp)
                        await session.close()
                        del self.sessions[proxy]
                        return

                    data = await resp.json()
                    status = data.get("status")

                    if status == "no_offer":
                        cost = 0.0

                    else:
                        try:
                            cost = float(data["price"]) if data["price"] else 0.0

                        except Exception as err:
                            self.log.warning(f"Can't get order cost -> {order} with proxy -> {proxy}")
                            self.log.debug(data)
                            self.log.debug(resp)
                            await session.close()
                            del self.sessions[proxy]
                            return

                    order.cost = cost
                    self.log.debug(f"Updated order -> {order} with proxy -> {proxy}")

            except Exception as err:
                self.log.exception(err)
                await session.close()
                return

        return order

    async def update_order(self, order: Order, proxy: str) -> Order:
        session = self.sessions.get(proxy)
        if not session:
            session = await self.create_session(proxy=proxy)

            if not session:
                return

            self.sessions.update({proxy: session})

        try:
            return await self.__update_order(session, order, proxy)

        except Exception as err:
            self.log.exception(err)

    async def stop_sessions(self):
        for key in list(self.sessions):
            try:
                session = self.sessions[key]
                await session.close()
                del self.sessions[key]

                self.log.debug(f"Stopped session -> {key}")

            except Exception as err:
                self.log.exception(err)
