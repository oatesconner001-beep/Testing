from __future__ import annotations

import importlib
import importlib.util
import json
import urllib.request
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Optional

from rockauto_buyersguide_scraper.cache import CacheStore, deserialize_json, serialize_json

BUYER_GUIDE_CACHE_KIND = "buyer_guide_api"
INFO_HTML_CACHE_KIND = "info_page_html"
INFO_DESC_CACHE_KIND = "info_page_description"


@dataclass(frozen=True)
class BuyerGuideResult:
    payload: dict


@dataclass(frozen=True)
class InfoPageResult:
    html: str
    description: str


class DescriptionParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_meta_description = False
        self._description: Optional[str] = None

    @property
    def description(self) -> str:
        return self._description or ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag != "meta":
            return
        attrs_dict = {key.lower(): value for key, value in attrs}
        if attrs_dict.get("name", "").lower() == "description":
            self._description = attrs_dict.get("content") or ""


def fetch_buyer_guide(
    cache: CacheStore,
    part_number: str,
    part_type: str,
    api_url: str,
) -> BuyerGuideResult:
    cached = cache.get(part_number, part_type, BUYER_GUIDE_CACHE_KIND)
    if cached is not None:
        return BuyerGuideResult(payload=deserialize_json(cached.value))

    with urllib.request.urlopen(api_url) as response:
        payload = json.loads(response.read().decode("utf-8"))
    cache.set(part_number, part_type, BUYER_GUIDE_CACHE_KIND, serialize_json(payload))
    return BuyerGuideResult(payload=payload)


def fetch_info_page_python(
    cache: CacheStore,
    part_number: str,
    part_type: str,
    info_url: str,
) -> InfoPageResult:
    cached_html = cache.get(part_number, part_type, INFO_HTML_CACHE_KIND)
    cached_description = cache.get(part_number, part_type, INFO_DESC_CACHE_KIND)
    if cached_html is not None and cached_description is not None:
        return InfoPageResult(html=cached_html.value, description=cached_description.value)

    with urllib.request.urlopen(info_url) as response:
        html = response.read().decode("utf-8")
    description = _parse_description(html)
    cache.set(part_number, part_type, INFO_HTML_CACHE_KIND, html)
    cache.set(part_number, part_type, INFO_DESC_CACHE_KIND, description)
    return InfoPageResult(html=html, description=description)


def fetch_info_page_playwright(
    cache: CacheStore,
    part_number: str,
    part_type: str,
    info_url: str,
) -> InfoPageResult:
    cached_html = cache.get(part_number, part_type, INFO_HTML_CACHE_KIND)
    cached_description = cache.get(part_number, part_type, INFO_DESC_CACHE_KIND)
    if cached_html is not None and cached_description is not None:
        return InfoPageResult(html=cached_html.value, description=cached_description.value)

    playwright_spec = importlib.util.find_spec("playwright.sync_api")
    if playwright_spec is None:
        raise RuntimeError("Playwright is not installed. Install playwright to use this path.")

    sync_api = importlib.import_module("playwright.sync_api")
    with sync_api.sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.goto(info_url)
        html = page.content()
        browser.close()

    description = _parse_description(html)
    cache.set(part_number, part_type, INFO_HTML_CACHE_KIND, html)
    cache.set(part_number, part_type, INFO_DESC_CACHE_KIND, description)
    return InfoPageResult(html=html, description=description)


def _parse_description(html: str) -> str:
    parser = DescriptionParser()
    parser.feed(html)
    return parser.description
