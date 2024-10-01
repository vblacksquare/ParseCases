
import asyncio
import aiohttp

from config import VALUESARP_SEARCH_LINK, VALUESARP_TOKEN, CONCURRENT_REQUESTS, GOOGLE_CREDS


semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
progress = 0


async def get_ads_result(keyword, location, domain, country):
    global progress

    async with semaphore:
        async with aiohttp.ClientSession(loop=asyncio.get_running_loop()) as session:
            async with session.get(
                VALUESARP_SEARCH_LINK,
                params={
                    "api_key": VALUESARP_TOKEN,
                    "q": keyword,
                    "location": location,
                    "google_domain": domain,
                    "gl": country,
                    "hl": "en",
                    "device": "desktop",
                }
            ) as response:
                progress -= 1
                print(f"\rCollecting data, remaining: {progress}", end="")

                ads = []
                shop = []

                if not response.status == 200:
                    print(f"\nGot bad status while requesting resource -> {response.status}\n")
                    return [keyword, location, domain], ads, shop

                try:
                    data = await response.json()

                except Exception as err:
                    print(f"\nGot err while parsing json -> {err}\n")
                    return [keyword, location, domain], ads, shop

                if "ads" in data:
                    for row in data["ads"]:
                        ads.append([
                            keyword,
                            location,
                            row.get("title"),
                            row.get("description"),
                            row.get("link"),
                            row.get("domain"),
                        ])

                if "inline_shopping" in data:
                    for row in data["inline_shopping"]:
                        shop.append([
                            keyword,
                            location,
                            row.get("link"),
                        ])

                return [keyword, location, domain], ads, shop


async def get_ads_results(keywords, locations, domains, country, size):
    global progress

    tasks = []

    for i in range(size):
        tasks.append(get_ads_result(
            keyword=keywords[i],
            location=locations[i],
            domain=domains[0],
            country=country
        ))

    progress = len(tasks)

    return await asyncio.gather(*tasks)
