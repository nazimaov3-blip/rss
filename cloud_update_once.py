#!/usr/bin/env python3
"""
Single-run RSS updater for cloud schedulers (GitHub Actions, cron, etc.).

Behavior:
- loads sources from sources_state.json
- refreshes each source
- writes XML only when items changed
- saves state back to sources_state.json
"""

import logging
import sys
import traceback
from datetime import datetime, timezone
from email.utils import format_datetime
from xml.etree import ElementTree as ET

from main import RSSBuilder, SiteAnalyzer, SourceRecord, StateStorage


LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("cloud-update")


def items_signature(xml_text: str):
    """
    Stable signature based on item guid/link/title.
    Returns tuple or None if XML cannot be parsed.
    """
    try:
        root = ET.fromstring(xml_text or "")
        channel = root.find("channel")
        if channel is None:
            return None
        signature = []
        for item in channel.findall("item"):
            guid = (item.findtext("guid") or "").strip()
            link = (item.findtext("link") or "").strip()
            title = (item.findtext("title") or "").strip()
            signature.append((guid, link, title))
        return tuple(signature)
    except Exception:
        return None


class CloudUpdater:
    def __init__(self, state_path: str = "sources_state.json"):
        self.analyzer = SiteAnalyzer()
        self.builder = RSSBuilder()
        self.storage = StateStorage(state_path)

    def _build_xml(self, source: SourceRecord, page_url: str, articles):
        channel = {
            "title": source.name,
            "link": page_url,
            "description": f"RSS feed for {source.name}",
            "language": "ru-RU",
            "lastBuildDate": format_datetime(datetime.now(timezone.utc)),
            "generator": "SiteToRSS Cloud Updater",
        }
        return self.builder.build(channel, articles)

    def update_source(self, source: SourceRecord):
        logger.info("Updating: %s", source.name)
        try:
            page_url, soup = self.analyzer.load_html(source.url)
            if soup is None:
                return False, 0, "page load failed", False

            feed_info = self.analyzer.discover_feed_info(page_url, soup)
            feed_url = feed_info.get("best_url")

            articles = None
            if feed_url:
                try:
                    parsed = self.analyzer.parse_feed(feed_url)
                    articles = parsed["articles"]
                    source.source_type = "feed"
                    logger.info("  using feed: %s", feed_url[:80])
                except Exception as exc:
                    logger.warning("  feed parse failed: %s", str(exc)[:120])

            if not articles:
                parsed = self.analyzer.extract_articles_from_html(page_url, soup)
                articles = parsed["articles"]
                source.source_type = "html"
                logger.info("  using html parsing fallback")

            if not articles:
                return False, 0, "no articles", False

            new_xml = self._build_xml(source, page_url, articles)
            old_sig = items_signature(source.generated_xml)
            new_sig = items_signature(new_xml)
            changed = (old_sig != new_sig) or not source.generated_xml

            if not changed:
                source.status = "No changes"
                logger.info("  no changes detected")
                return True, len(articles), "", False

            source.generated_xml = new_xml
            source.status = "Updated"
            source.last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if source.update_history is None:
                source.update_history = []
            source.update_history.append(
                {
                    "timestamp": source.last_updated,
                    "articles_count": len(articles),
                    "feed_type": source.source_type,
                }
            )

            with open(source.file_name, "w", encoding="utf-8") as f:
                f.write(source.generated_xml)
            logger.info("  updated (%d items)", len(articles))
            return True, len(articles), "", True
        except Exception as exc:
            source.status = f"Error: {str(exc)[:80]}"
            logger.error("  update failed: %s", exc)
            logger.debug(traceback.format_exc())
            return False, 0, str(exc)[:120], False

    def run_once(self):
        state = self.storage.load()
        github_settings = state.get("github", {})
        raw_sources = state.get("sources", [])
        sources = []
        for raw in raw_sources:
            try:
                sources.append(SourceRecord(**raw))
            except Exception as exc:
                logger.warning("Skipping broken source entry: %s", exc)

        if not sources:
            logger.warning("No sources found in sources_state.json")
            return 0, 0, 0, 0

        ok = 0
        errors = 0
        total_articles = 0
        changed_sources = 0

        for source in sources:
            success, count, err, changed = self.update_source(source)
            if success:
                ok += 1
                total_articles += count
                if changed:
                    changed_sources += 1
            else:
                errors += 1
                if err:
                    logger.error("  %s", err)

        self.storage.save(github_settings, sources)
        return ok, errors, total_articles, changed_sources


def main():
    updater = CloudUpdater()
    ok, errors, total_articles, changed_sources = updater.run_once()
    logger.info("Summary: ok=%d errors=%d articles=%d changed=%d", ok, errors, total_articles, changed_sources)
    if ok == 0 and errors > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
