
from loguru import logger

import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime

from dtypes.face import Face
from dtypes.category import Category, CategoryGroup
from dtypes.freelancer import FreelancerPreview, Freelancer, Social, Link

from utils.singleton import SingletonMeta


FREELANCERS_QUERY = "\n      query GetUserSearchResults(\n        $query: query\n        $filter: SearchResultFilter\n        $first: Int! = 48\n        $after: String\n        $shouldPrefetchReviews: Boolean!\n      ) {\n        search(query: $query, type: USER, filter: $filter, first: $first, after: $after, alwaysHasNext: true) {\n          pageInfo {\n            hasNextPage\n            endCursor\n          }\n          nodes {\n            ... on User {\n              __typename\n              id\n              firstName\n              username\n              url\n              isProfileOwner\n              isFeaturedFreelancer\n              isResponsiveToHiring\n              country\n              images {\n                size_50 {\n                  ...ImageFields\n                }\n                size_100 {\n                  ...ImageFields\n                }\n                size_115 {\n                  ...ImageFields\n                }\n                size_230 {\n                  ...ImageFields\n                }\n                size_138 {\n                  ...ImageFields\n                }\n                size_276 {\n                  ...ImageFields\n                }\n              }\n              displayName\n              location\n              creativeFields {\n                name\n                id\n                url\n              }\n              isFollowing\n              allowsContactFromAnyone\n              subscriptionProduct {\n                unitAmount\n                currency\n              }\n              isMessageButtonVisible\n              availabilityInfo {\n                isAvailableFullTime\n                isOpenToRelocation\n                isLookingForRemote\n                isAvailableFreelance\n                compensationMin\n                currency\n                availabilityTimeline\n                buttonCTAType\n                hiringTimeline {\n                  key\n                  label\n                }\n              }\n              stats {\n                appreciations\n                views\n                followers\n              }\n              projects(first: 5) {\n                nodes {\n                  url\n                  slug\n                  covers {\n                    size_202 {\n                      url\n                    }\n                    size_404 {\n                      url\n                    }\n                    size_808 {\n                      url\n                    }\n\n                    size_202_webp {\n                      url\n                    }\n                    size_404_webp {\n                      url\n                    }\n                    size_max_808_webp {\n                      url\n                    }\n                  }\n                }\n              }\n              freelanceProjectUserInfo {\n                ...reviewsFields\n              }\n              ...creatorProBadgeFields\n            }\n          }\n          metaContent {\n            totalEntityCount\n            csam {\n              isCSAMViolation\n              description\n              helpResource\n              reportingOption\n            }\n          }\n        }\n      }\n      fragment ImageFields on ImageRendition {\n        url\n        width\n        height\n      }\n      \n  fragment reviewsFields on FreelanceProjectUserInfo {\n    completedProjectCount\n    completedProjects @include(if: $shouldPrefetchReviews) {\n      id\n      modifiedOn\n      status\n      hirer {\n        username\n        displayName\n        id\n        images {\n          size_230 {\n            url\n          }\n        }\n        url\n      }\n      creator {\n        username\n        displayName\n        id\n        images {\n          size_230 {\n            url\n          }\n        }\n        url\n      }\n    }\n    reviews @include(if: $shouldPrefetchReviews) {\n      review\n      id\n      freelanceProject {\n        id\n        status\n        modifiedOn\n      }\n      reviewer {\n        id\n        username\n        displayName\n        id\n        images {\n          size_230 {\n            url\n          }\n        }\n        url\n      }\n    }\n  }\n\n      \n  fragment creatorProBadgeFields on User {\n    creatorPro {\n      initialSubscriptionDate\n      isActive\n    }\n  }\n\n    "


class Behance(metaclass=SingletonMeta):
    def __init__(self):
        self.log = logger.bind(classname=self.__class__.__name__)

    async def create_session(self, face: Face) -> aiohttp.ClientSession:
        try:
            session = aiohttp.ClientSession()

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
                **face.headers
            }

            async with session.get(
                    url="https://www.behance.net/search/users",
                    allow_redirects=False,
                    headers=headers
            ) as resp:
                session.bcp = resp.headers.get("x-trace-id")

            session.cookie_jar.update_cookies({
                "gki": "test_cross_auth: false, feature_search_gk_debug: false, list_keyboard_nav: false",
                "ilo0": "true",
                "originalReferrer": "https://www.behance.net/search/users",
                "bcp": session.bcp,
                "gpv": "behance.net:hire:browse; sign_up_prompt=true"
            })

            self.log.info(f"Created session -> {face}")
            return session

        except Exception as err:
            self.log.exception(err)

    async def get_categories(self, face: Face, in_session: aiohttp.ClientSession = None) -> list[CategoryGroup]:
        try:
            session = in_session if in_session else await self.create_session(face)

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
                **face.headers
            }

            async with session.get(
                url="https://www.behance.net/search/users",
                allow_redirects=False,
                headers=headers,
                proxy=face.proxies
            ) as resp:
                data = await resp.text()

            bs4 = BeautifulSoup(data, "html.parser")

            categories = []

            category_group = bs4.select_one("div[role='group'][aria-labelledby*='Popular']")
            raw_categories = category_group.select("div[class*='CollapsibleSection-fieldLabel']")
            categories.append(
                CategoryGroup(
                    title="Popular",
                    categories=list(map(
                        lambda x: Category(title=x.get_text(strip=True), index=-1),
                        raw_categories
                    ))
                )
            )

            category_groups = bs4.select("div[role='group'][aria-labelledby*='Alphabetical'] fieldset[class*='CollapsibleSection-fieldset']")
            for category_group in category_groups:
                category_header = category_group.select_one("legend").get_text(strip=True)
                raw_categories = category_group.select("div")

                categories.append(
                    CategoryGroup(
                        title=category_header,
                        categories=list(map(
                            lambda x: Category(title=x.get_text(strip=True), index=-1),
                            raw_categories
                        ))
                    )
                )

            if not in_session:
                await session.close()

            self.log.info(f"Got categories -> {categories}")
            return categories

        except Exception as err:
            self.log.exception(err)

    async def get_freelancers(
        self,
        face: Face,
        category: Category,
        pointer: str = None,
        in_session: aiohttp.ClientSession = None,
        limit=24,
        parsed=None
    ) -> list[FreelancerPreview]:
        try:
            session = in_session if in_session else await self.create_session(face)

            parsed = parsed if parsed else []
            if len(parsed) >= limit:
                if not in_session:
                    await session.close()

                self.log.info(f"Got freelancers_preview -> {parsed[:limit]}")
                return parsed[:limit]

            headers = {
                'accept': '*/*',
                'accept-encoding': 'gzip, deflate, br, zstd',
                'accept-language': 'en-GB,en;q=0.9',
                'origin': 'https://www.behance.net',
                "reffer": "https://www.behance.net/search/users",
                'priority': 'u=1, i',
                'sec-ch-ua-mobile': '?0',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                "x-bcp": session.bcp,
                "x-requested-with": "XMLHttpRequest",
                **face.headers
            }

            async with session.post(
                url="https://www.behance.net/v3/graphql",
                json={
                    "query": FREELANCERS_QUERY,
                    "variables": {
                          "filter": {
                            "field": category.key
                          },
                          "first": 24,
                          "after": pointer,
                          "shouldPrefetchReviews": True
                        }
                    },
                headers=headers,
                proxy=face.proxies
            ) as response:
                data = (await response.json()).get("data")

                if not data:
                    if not in_session:
                        await session.close()

                    return []

                for node in data["search"]["nodes"]:
                    parsed.append(FreelancerPreview(
                        id=node['id'],
                        link=node['url'],
                        username=node['username'],
                        display_name=node['displayName']
                    ))

                pointer = data["search"]["pageInfo"]["endCursor"]

                if not in_session:
                    await session.close()

                self.log.info(f"Got freelancers_preview -> {parsed[:limit]}")
                return await self.get_freelancers(
                    face=face,
                    category=category,
                    pointer=pointer,
                    limit=limit,
                    parsed=parsed
                )

        except Exception as err:
            self.log.exception(err)

    async def get_freelancer(
        self,
        face: Face,
        freelancer_preview: FreelancerPreview,
        in_session: aiohttp.ClientSession = None
    ) -> Freelancer:
        try:
            session = in_session if in_session else await self.create_session(face)

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
                **face.headers
            }

            async with session.get(
                url='/'.join((freelancer_preview.link, "info")),
                headers=headers,
                proxy=face.proxies
            ) as resp:

                soup = BeautifulSoup(await resp.text(), "html.parser")

                avatar = soup.select_one("img.AvatarImage-avatarImage-PUL.Avatar-root-sWV")
                if avatar:
                    avatar = avatar.get("src")
                    avatar = None if "/img/profile/no-image-" in avatar else avatar

                views = soup.select_one("a[aria-label='Project Views']")
                views = int(views.get_text(strip=True).replace(",", "")) if views else 0

                appreciations = soup.select_one("a[aria-label='Appreciations']")
                appreciations = int(appreciations.get_text(strip=True).replace(",", "")) if appreciations else 0

                followers = soup.select_one("a[aria-label='Followers']")
                followers = int(followers.get_text(strip=True).replace(",", "")) if followers else 0

                following = soup.select_one("a[aria-label='Following']")
                following = int(following.get_text(strip=True).replace(",", "")) if following else 0

                location = soup.select_one("span.e2e-Profile-location")
                location = location.get_text(strip=True) if location else None

                website = soup.select_one("a[class*='ProfileCard-anchor']")
                website = website['href'] if website else None

                raw_socials = soup.select("#profile-contents a[class*='VerifiedSocial-accountContent']")
                socials = []
                for i in raw_socials:
                    socials.append(Social(
                        title=i.get_text().split(" ")[1],
                        link=i['href'],
                        is_verified=not i.select_one("div[class*='VerifiedSocial-activator']") is None
                    ))

                raw_links = soup.select("#profile-contents a[class*='UserInfo-linkTitle']")
                links = []
                for i in raw_links:
                    links.append(Link(
                        title=i.select_one("div[class*='WebReference-webReferenceLink']").get_text(strip=True)[1],
                        link=i['href'],
                    ))

                raw_additional = soup.select("#profile-contents div[class*='UserInfo-bio']")
                additional = []
                for i in raw_additional:
                    additional.append(i.get_text(strip=True))

                created_date = soup.select_one("p[class*='UserInfo-memberSince']")
                if created_date:
                    created_date = round(datetime.strptime(created_date.get_text(strip=True).split(": ")[-1], "%B %d, %Y").timestamp())

                freelancer = Freelancer(
                    id=freelancer_preview.id,
                    link=freelancer_preview.link,
                    username=freelancer_preview.username,
                    display_name=freelancer_preview.display_name,
                    avatar=avatar,
                    views=views,
                    appreciations=appreciations,
                    followers=followers,
                    following=following,
                    location=location,
                    website=website,
                    socials=socials,
                    links=links,
                    additional=additional,
                    created_date=created_date
                )

            if not in_session:
                await session.close()

            self.log.info(f"Got freelancer -> {freelancer}")
            return freelancer

        except Exception as err:
            self.log.exception(err)
