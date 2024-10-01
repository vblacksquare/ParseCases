
import asyncio
import aiohttp
from aiosocksy import Socks5Auth
from aiosocksy.connector import ProxyConnector, ProxyClientRequest

import time
import base64
import json
import re
import urllib.parse
from bs4 import BeautifulSoup

from dtypes import SearchResult, Movie
from utils import string_to_uuid, get_max_quality, get_best_translator
from config import HOST_HDREZKA
from db import movies_collection


class Parser:
    def __init__(self):
        self.is_proxy = False
        self.__proxy_auth = Socks5Auth(login='qxlyraux', password='e4y9dv996mbo')
        self.__proxy = "socks5://p.webshare.io:80"

        self.http = aiohttp.ClientSession(loop=asyncio.get_running_loop(), connector=ProxyConnector(), request_class=ProxyClientRequest)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        }

    @property
    def proxy(self):
        return self.__proxy if self.is_proxy else None

    @property
    def proxy_auth(self):
        return self.__proxy_auth if self.is_proxy else None

    async def get_movie_from_db(self, movie_id):
        movie_response = await movies_collection.find_one({"id": movie_id}, {"_id": 0})

        if not movie_response:
            return None

        movie = Movie(**movie_response)
        return movie

    async def search(self, query):

        search_link = f"https://{HOST_HDREZKA}/search/?do=search&subaction=search&q={query}"

        async with self.http.get(search_link, headers=self.headers, proxy=self.proxy, proxy_auth=self.proxy_auth) as response:
            if response.status != 200:
                return SearchResult(
                    query=query,
                    movies=[]
                )

            search_response = await response.text()

            soup = BeautifulSoup(search_response, "html.parser")
            movies = []

            for movie_card in soup.select("div.b-content__inline_item"):
                movie_link = movie_card.get("data-url")
                poster = movie_card.select_one("img").get("src")

                title_block = movie_card.select_one("div.b-content__inline_item-link")
                title = title_block.select_one("a").get_text(strip=True)
                subtitle = title_block.select_one("div").get_text(strip=True)

                movie = Movie(
                    id=string_to_uuid(movie_link),
                    link=movie_link,
                    title=title,
                    subtitle=subtitle,
                    seasons=[],
                    is_series="series" in movie_link,
                    translators=[],
                    source=None,
                    poster=poster
                )

                movies.append(movie)

                if not await movies_collection.find_one({"id": movie.id}):
                    await movies_collection.insert_one(movie.to_dict())

            return SearchResult(
                query=query,
                movies=movies
            )

    async def decode_streams(self, string):
        def b1(s):
            encoded = urllib.parse.quote(s)
            encoded = encoded.replace('%', '')
            decoded_bytes = bytes.fromhex(encoded)
            return base64.b64encode(decoded_bytes).decode('utf-8')

        def b2(s):
            decoded_bytes = base64.b64decode(s)
            decoded_string = decoded_bytes.decode('utf-8')
            return urllib.parse.unquote(decoded_string)

        v = {
            "bk4": "$$!!@$$@^!@#$$@",
            "bk3": "@@@@@!##!^^^",
            "bk2": "####^!!##!@@",
            "bk1": "^^^!@##!!##",
            "bk0": "$$#!!@#!@##"
        }

        file3_separator = r'//_//'

        a = string[2:].replace(r'\/\/_\/\/', "//_//")

        for i in "43210":
            i = f"bk{i}"
            if i in v and v[i] != "":
                a = a.replace(file3_separator + b1(v[i]), "")

        return b2(a)

    async def find_streams(self, string, is_found=False):
        streams = {}

        if not is_found:
            pattern = r'"streams":\s*"(.*?)",'

            match = re.search(pattern, string, re.DOTALL)

            if not match:
                return streams

            streams_bytes = match.group(1)

        else:
            streams_bytes = string

        streams_raw = await self.decode_streams(streams_bytes)
        qualities = streams_raw.split(",")

        for quality_raw in qualities:
            parts = quality_raw.split("]")
            quality = parts[0][1:]
            streams.update({
                quality: parts[1].split(" or ")[0]
            })

        return streams

    async def get_movie(self, movie_id):
        movie_response = await movies_collection.find_one({"id": movie_id}, {"_id": 0})

        if not movie_response:
            return None

        movie = Movie(**movie_response)

        async with self.http.get(movie.link, headers=self.headers, proxy=self.proxy, proxy_auth=self.proxy_auth) as response:
            if response.status != 200:
                return None

            movie_response = await response.text()

        soup = BeautifulSoup(movie_response, "html.parser")

        pattern = r"initCDNSeriesEvents\(\d+,\s*(\d+)"
        match = re.search(pattern, movie_response)
        translators = [
            {
                "id": translator["data-translator_id"],
                "title": translator.get_text(strip=True)
            }
            for translator in soup.select("#translators-list li")
        ]

        movie.translators = [{"id": match.group(1), "title": "default"}] if len(translators) == 0 and match else translators
        movie.is_series = not soup.select_one("#simple-seasons-tabs") is None

        if movie.is_series:
            movie.seasons = {str(i): {} for i in range(len(soup.select("#simple-seasons-tabs li")))}

        else:
            movie = await self.get_film(movie)

        await movies_collection.update_one({"id": movie.id}, {"$set": movie.to_dict()})

        return movie

    async def get_movie_season(self, movie_id, season_id):
        movie_response = await movies_collection.find_one({"id": movie_id}, {"_id": 0})

        if not movie_response:
            return None

        movie = Movie(**movie_response)

        if season_id == 1:
            season_link = movie.link

        elif len(movie.translators) == 0:
            season_link = movie.link

        else:
            season_link = f"{movie.link}#t:{movie.translators[0]['id']}-s:{int(season_id)+1}-e:1"

        async with self.http.get(season_link, headers=self.headers, proxy=self.proxy, proxy_auth=self.proxy_auth) as response:
            if response.status != 200:
                return None

            season_response = await response.text()

        soup = BeautifulSoup(season_response, "html.parser")
        series = len(soup.select("#simple-episodes-list-1 li"))
        movie.seasons[season_id] = {str(i): None for i in range(series)}

        await movies_collection.update_one({"id": movie.id}, {"$set": movie.to_dict()})

        return movie

    async def get_film(self, movie: Movie):
        movie_id = int(movie.link.split("/")[-1].split("-")[0])

        best_translator = get_best_translator(movie)

        streams_link = f"https://{HOST_HDREZKA}/ajax/get_cdn_series/?t={round(time.time() * 1000)}"
        async with self.http.post(streams_link, headers=self.headers, proxy=self.proxy, proxy_auth=self.proxy_auth, data={
            "id": movie_id,
            "translator_id": int(best_translator["id"]) if best_translator else best_translator,
            "is_camrip": 0,
            "is_ads": 0,
            "is_director": 0,
            "action": "get_movie"
        }) as response:
            if response.status != 200:
                return None

            episode_response = json.loads(await response.text())

        if "url" not in episode_response:
            return None

        streams = await self.find_streams(episode_response['url'], True)

        movie.source = get_max_quality(streams)

        await movies_collection.update_one({"id": movie.id}, {"$set": movie.to_dict()})

        return movie

    async def get_movie_episode(self, movie_id, season_id, episode_id):
        movie_response = await movies_collection.find_one({"id": movie_id}, {"_id": 0})

        if not movie_response:
            return None

        movie = Movie(**movie_response)

        movie_id = int(movie.link.split("/")[-1].split("-")[0])

        streams_link = f"https://{HOST_HDREZKA}/ajax/get_cdn_series/?t={round(time.time()*1000)}"
        async with self.http.post(streams_link, headers=self.headers, proxy=self.proxy, proxy_auth=self.proxy_auth, data={
            "id": movie_id,
            "translator_id": int(get_best_translator(movie)["id"]),
            "season": int(season_id)+1,
            "episode": int(episode_id)+1,
            "action": "get_stream"
        }) as response:
            if response.status != 200:
                return None

            episode_response = json.loads(await response.text())

        if "url" not in episode_response:
            return None

        streams = await self.find_streams(episode_response['url'], True)

        movie.seasons[season_id][episode_id] = get_max_quality(streams)

        await movies_collection.update_one({"id": movie.id}, {"$set": movie.to_dict()})

        return movie

    async def stop(self):
        await self.http.close()
