
import os
import asyncio
import time

import aiohttp
import base64
import math
import numpy as np
from bs4 import BeautifulSoup
from loguru import logger as log
import undetected_chromedriver as uc
from selenium import webdriver

from listing import Listing
from config import RETAIL_COEFFICIENT, CONCURRENT_REQUESTS, CHROME_PATH, CHROME_VERSION
from config import PERCENTILE_FIRST, PERCENTILE_SECOND


semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


async def get_phpsession_id() -> str:
    def f():
        if CHROME_PATH:
            os.system('taskkill /IM "chrome.exe" /F')
            os.system('killall "Google Chrome"')
            os.system('pkill chrome')

            parts = CHROME_PATH.split("/")
            path = '/'.join(parts[:-1])
            profile = parts[-1]

        else:
            path = "resources/chrome"
            profile = "Default"

        options = webdriver.ChromeOptions()
        options.add_argument(f'--user-data-dir={path}')
        options.add_argument(f'--profile-directory={profile}')

        driver = uc.Chrome(version_main=CHROME_VERSION, options=options, headless=True)
        driver.get('https://ecooparts.com/en/used-auto-parts/?pag=pro&busval=fDY5NDI1MDEwNHxuaW5ndW5vfHByb2R1Y3RvfC0xfDB8MHwwfDB8fDB8MHwwfDA=&filval=&panu=MQ==&tebu=Njk0MjUwMTA0&ord=bmluZ3Vubw==&valo=LTE=&ubic=&toen=eXBtMzJ2aDlhM2xkOGVwbnBjcXhr&veid=MA==&qregx=MzA=&tmin=MQ==&ttseu=&txbu=Njk0MjUwMTA0&ivevh=&ivevhmat=&ivevhsel=&ivevhcsver=&ivevhse=&oem=&vin=')

        phpsessionid = None
        while not phpsessionid:
            all_cookies = driver.get_cookies()

            cookies_dict = {}

            for cookie in all_cookies:
                cookies_dict[cookie['name']] = cookie['value']

            phpsessionid = cookies_dict['PHPSESSID']
            time.sleep(1)

        driver.close()

        return phpsessionid

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, f)


async def __parse_listing(session: aiohttp.ClientSession, listing: Listing) -> Listing:
    busval = base64.b64encode("|".join((
        f"|{listing.oem.replace(' ', '*')}",
        "ninguno", "producto",
        "-1", "0", "0", "0", "0", "", "0", "0", "0", "0"
    )).encode()).decode()

    response = await session.get(
        url=f"https://ecooparts.com/ajax/ajax_filtros.php",
        params={
            "busval": busval,
        }
    )

    soup = BeautifulSoup(''.join((
        "<div>", await response.text(), "</div>"
    )), "html.parser")

    listing.amount = int(soup.select_one("#qregbusc").get("value"))
    if not listing.amount:
        return listing

    prices = []
    for page in range(math.ceil(listing.amount/180)):
        params = {
            "busval": busval,
            "qregx": "MTgw",
            "panu": page+1
        }

        response = await session.get(
            url=f"https://ecooparts.com/ajax/ajax_buscador.php",
            params=params
        )

        soup = BeautifulSoup(''.join((
            "<div>", await response.text(), "</div>"
        )), "html.parser")

        for price in soup.select("div.product-card__price--current") + soup.select("div.product-card__price--old"):
            try:
                prices.append(float(price.get_text(strip=True).split(" ")[0].replace(".", "").replace(",", ".")))

            except Exception as err:
                raise err

    retail_prices = list(map(lambda x: round(x / RETAIL_COEFFICIENT, 2), prices))

    Q1 = np.percentile(retail_prices, PERCENTILE_FIRST)
    Q3 = np.percentile(retail_prices, PERCENTILE_SECOND)

    filtered_prices = list(filter(lambda x: Q1 <= x <= Q3, retail_prices))

    if not len(filtered_prices):
        return listing

    listing.upper_bound = max(filtered_prices)
    listing.lower_bound = min(filtered_prices)

    return listing


async def parse_listing(listing: Listing, progress: dict, phpsessionid: str) -> Listing:
    async with semaphore:
        t1 = time.time()
        session = aiohttp.ClientSession()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        })
        session.cookie_jar.update_cookies({"PHPSESSID": phpsessionid})

        try:
            listing = await __parse_listing(session, listing)

        except Exception as err:
            log.exception(err)

        await session.close()

        t2 = time.time() - t1
        start_delta = time.time() - progress['start']

        progress["collected"] += 1
        print(f"\rCollecting {round(progress['collected']/progress['total']*100)}%, speed = {round(3600/(start_delta/progress['collected']), 2)} oems/hour", end="")
        log.info(f"Parsed {listing} in {t2} seconds")
        return listing
