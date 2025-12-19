from __future__ import annotations

import argparse
from pathlib import Path

from rockauto_buyersguide_scraper.cache import CacheStore
from .fetchers import fetch_buyer_guide, fetch_info_page_playwright, fetch_info_page_python

DEFAULT_TTL_SECONDS = 60 * 60 * 24


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch part data with caching.")
    parser.add_argument("part_number", help="Part number to fetch.")
    parser.add_argument("part_type", help="Part type to fetch.")
    parser.add_argument("buyer_guide_url", help="Buyer guide API URL.")
    parser.add_argument("info_page_url", help="Info page URL.")
    parser.add_argument(
        "--use-playwright",
        action="store_true",
        help="Use Playwright to fetch the info page instead of urllib.",
    )
    parser.add_argument(
        "--cache-dir",
        default=".cache",
        help="Directory used to store cache files.",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        default=DEFAULT_TTL_SECONDS,
        help="Cache TTL in seconds before entries are refreshed.",
    )
    parser.add_argument(
        "--cache-clear",
        action="store_true",
        help="Clear all cached entries before fetching.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    cache = CacheStore(Path(args.cache_dir), ttl_seconds=args.cache_ttl)
    if args.cache_clear:
        cache.clear()
    cache.prune_expired()

    buyer_guide = fetch_buyer_guide(
        cache=cache,
        part_number=args.part_number,
        part_type=args.part_type,
        api_url=args.buyer_guide_url,
    )
    if args.use_playwright:
        info_page = fetch_info_page_playwright(
            cache=cache,
            part_number=args.part_number,
            part_type=args.part_type,
            info_url=args.info_page_url,
        )
    else:
        info_page = fetch_info_page_python(
            cache=cache,
            part_number=args.part_number,
            part_type=args.part_type,
            info_url=args.info_page_url,
        )

    print("Buyer guide payload keys:", ", ".join(sorted(buyer_guide.payload.keys())))
    print("Info page description:", info_page.description)


if __name__ == "__main__":
    main()
