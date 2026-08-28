"""The headline rail that replaced the quick-asset switcher.

Phil, 2026-08-28: "Remove this strip and add USDINR value here and important
news rolling up aside of that ... the one liner news has to roll everything
related to Crypto".

The feeds are third-party documents fetched over the network, so most of what
is pinned here is what happens when one misbehaves: entities, CDATA, markup
inside a headline, a javascript: link, a dead provider.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402

FEED = """<?xml version="1.0"?>
<rss><channel>
  <title>Some Feed</title>
  <item>
    <title>Bitcoin tops &amp;#36;77k as ETFs buy</title>
    <link>https://example.com/a</link>
    <pubDate>Fri, 28 Aug 2026 11:00:00 +0000</pubDate>
  </item>
  <item>
    <title><![CDATA[Britain&#39;s crypto millionaires]]></title>
    <link>https://example.com/b</link>
    <pubDate>Fri, 28 Aug 2026 12:00:00 +0000</pubDate>
  </item>
</channel></rss>"""


def _item(title, ts=1.0, link="", source="A"):
    return {"title": title, "link": link, "source": source, "ts": ts}


class RssParsingTests(unittest.TestCase):
    def test_it_reads_title_link_and_time(self):
        items = app_module._parse_rss_headlines(FEED, "Feed")
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["title"], "Bitcoin tops $77k as ETFs buy")
        self.assertEqual(items[0]["link"], "https://example.com/a")
        self.assertEqual(items[0]["source"], "Feed")
        self.assertGreater(items[0]["ts"], 0)

    def test_entities_inside_cdata_are_decoded(self):
        """The double-encoded case: CDATA carrying &#39; must end an apostrophe.

        Decrypt really does ship "Britain&#39;s" this way, and one unescape
        pass leaves the raw entity sitting in the headline.
        """
        items = app_module._parse_rss_headlines(FEED, "Feed")
        self.assertEqual(items[1]["title"], "Britain's crypto millionaires")

    def test_markup_in_a_headline_never_survives_as_markup(self):
        feed = (
            "<rss><item><title>Hack &lt;script&gt;alert(1)&lt;/script&gt; here</title>"
            "<link>https://e.com/x</link></item></rss>"
        )
        title = app_module._parse_rss_headlines(feed, "F")[0]["title"]
        self.assertNotIn("<script", title.lower())
        self.assertIn("alert(1)", title)

    def test_a_javascript_link_is_dropped_not_carried(self):
        feed = "<rss><item><title>Click me</title><link>javascript:alert(1)</link></item></rss>"
        self.assertEqual(app_module._parse_rss_headlines(feed, "F")[0]["link"], "")

    def test_a_very_long_headline_is_cut_with_an_ellipsis(self):
        feed = "<rss><item><title>{}</title><link>https://e.com/y</link></item></rss>".format("x" * 400)
        title = app_module._parse_rss_headlines(feed, "F")[0]["title"]
        self.assertLessEqual(len(title), app_module._CRYPTO_NEWS_TITLE_MAX)
        self.assertTrue(title.endswith("…"))

    def test_an_item_with_no_title_is_skipped_not_rendered_blank(self):
        feed = "<rss><item><link>https://e.com/z</link></item><item><title>Real</title></item></rss>"
        self.assertEqual([i["title"] for i in app_module._parse_rss_headlines(feed, "F")], ["Real"])

    def test_an_atom_feed_reads_too(self):
        feed = (
            "<feed><entry><title>Atom headline</title>"
            '<link rel="alternate" href="https://example.com/atom"/>'
            "<published>2026-08-28T11:00:00Z</published></entry></feed>"
        )
        items = app_module._parse_rss_headlines(feed, "A")
        self.assertEqual(items[0]["title"], "Atom headline")
        self.assertEqual(items[0]["link"], "https://example.com/atom")
        self.assertGreater(items[0]["ts"], 0)


class NewsCacheTests(unittest.TestCase):
    """_crypto_news around a stubbed _fetch_crypto_news."""

    def setUp(self):
        self._real = app_module._fetch_crypto_news
        app_module._CRYPTO_NEWS_CACHE.clear()

    def tearDown(self):
        app_module._fetch_crypto_news = self._real
        app_module._CRYPTO_NEWS_CACHE.clear()

    def test_a_second_read_is_served_from_cache(self):
        calls = []

        def counting():
            calls.append(1)
            return {"items": [_item("One")], "sources": ["A"], "error": ""}

        app_module._fetch_crypto_news = counting
        app_module._crypto_news()
        app_module._crypto_news()
        self.assertEqual(len(calls), 1)

    def test_when_every_feed_dies_the_last_headlines_are_kept_and_marked_stale(self):
        """A blank rail is worse than an old one — but it must SAY it is old."""
        app_module._fetch_crypto_news = lambda: {"items": [_item("Yesterday")], "sources": ["A"], "error": ""}
        self.assertTrue(app_module._crypto_news()["live"])

        def dead():
            raise RuntimeError("all providers down")

        app_module._fetch_crypto_news = dead
        app_module._CRYPTO_NEWS_CACHE["expires_at"] = 0
        second = app_module._crypto_news()
        self.assertEqual([i["title"] for i in second["items"]], ["Yesterday"])
        self.assertFalse(second["live"])
        self.assertTrue(second["stale"])

    def test_a_stale_read_is_retried_sooner_than_a_good_one(self):
        """One blip must not freeze the rail for the whole TTL."""
        import time as _time

        app_module._fetch_crypto_news = lambda: {"items": [_item("Y")], "sources": ["A"], "error": ""}
        app_module._crypto_news()
        good_expiry = float(app_module._CRYPTO_NEWS_CACHE["expires_at"])

        def dead():
            raise RuntimeError("down")

        app_module._fetch_crypto_news = dead
        app_module._CRYPTO_NEWS_CACHE["expires_at"] = 0
        app_module._crypto_news()
        stale_expiry = float(app_module._CRYPTO_NEWS_CACHE["expires_at"])
        self.assertLess(stale_expiry - _time.time(), good_expiry - _time.time())

    def test_a_cold_start_with_no_feeds_returns_empty_rather_than_raising(self):
        def dead():
            raise RuntimeError("down")

        app_module._fetch_crypto_news = dead
        news = app_module._crypto_news()
        self.assertEqual(news["items"], [])
        self.assertTrue(news["stale"])


class FetchMergeTests(unittest.TestCase):
    """_fetch_crypto_news around a stubbed network."""

    def setUp(self):
        self._urlopen = app_module.urlopen
        self._feeds = app_module._crypto_news_feeds
        app_module._CRYPTO_NEWS_CACHE.clear()

    def tearDown(self):
        app_module.urlopen = self._urlopen
        app_module._crypto_news_feeds = self._feeds
        app_module._CRYPTO_NEWS_CACHE.clear()

    def _serve(self, docs):
        """docs: {url: document-or-Exception}"""

        class Response:
            def __init__(self, body):
                self._body = body.encode("utf-8")

            def read(self, _n=None):
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def fake_urlopen(req, timeout=None):
            url = req.full_url if hasattr(req, "full_url") else str(req)
            doc = docs[url]
            if isinstance(doc, Exception):
                raise doc
            return Response(doc)

        app_module.urlopen = fake_urlopen
        app_module._crypto_news_feeds = lambda: [(f"F{i}", u) for i, u in enumerate(docs)]

    def test_newest_first_across_outlets(self):
        older = "<rss><item><title>Older</title><pubDate>Fri, 28 Aug 2026 09:00:00 +0000</pubDate></item></rss>"
        newer = "<rss><item><title>Newer</title><pubDate>Fri, 28 Aug 2026 15:00:00 +0000</pubDate></item></rss>"
        self._serve({"https://a.example/f": older, "https://b.example/f": newer})
        self.assertEqual([i["title"] for i in app_module._fetch_crypto_news()["items"]], ["Newer", "Older"])

    def test_the_same_story_from_two_outlets_appears_once(self):
        same = "<rss><item><title>Same Story</title><pubDate>Fri, 28 Aug 2026 09:00:00 +0000</pubDate></item></rss>"
        self._serve({"https://a.example/f": same, "https://b.example/f": same})
        self.assertEqual(len(app_module._fetch_crypto_news()["items"]), 1)

    def test_one_dead_feed_does_not_empty_the_rail(self):
        alive = "<rss><item><title>Still here</title><pubDate>Fri, 28 Aug 2026 09:00:00 +0000</pubDate></item></rss>"
        self._serve({"https://a.example/f": OSError("timed out"), "https://b.example/f": alive})
        news = app_module._fetch_crypto_news()
        self.assertEqual([i["title"] for i in news["items"]], ["Still here"])
        self.assertEqual(news["sources"], ["F1"])
        self.assertIn("timed out", news["error"])

    def test_every_feed_dead_raises_so_the_caller_can_fall_back(self):
        self._serve({"https://a.example/f": OSError("boom")})
        with self.assertRaises(RuntimeError):
            app_module._fetch_crypto_news()

    def test_the_rail_is_capped_so_one_feed_cannot_flood_it(self):
        many = (
            "<rss>"
            + "".join(
                f"<item><title>Story {n}</title><pubDate>Fri, 28 Aug 2026 09:00:00 +0000</pubDate></item>"
                for n in range(200)
            )
            + "</rss>"
        )
        self._serve({"https://a.example/f": many})
        self.assertEqual(len(app_module._fetch_crypto_news()["items"]), app_module._CRYPTO_NEWS_LIMIT)


class FeedConfigTests(unittest.TestCase):
    def test_the_defaults_are_https_and_named(self):
        feeds = app_module._crypto_news_feeds()
        self.assertGreaterEqual(len(feeds), 3)
        for label, url in feeds:
            self.assertTrue(url.startswith("https://"), url)
            self.assertTrue(label)

    def test_coindesk_is_listed_without_the_trailing_slash(self):
        """With it the feed answers 308, and urlopen does not follow for us."""
        urls = dict(app_module._crypto_news_feeds())
        self.assertEqual(urls["CoinDesk"], "https://www.coindesk.com/arc/outboundfeeds/rss")

    def test_an_env_override_wins(self):
        os.environ["CRYPTOFORGE_NEWS_RSS_URLS"] = "Mine=https://example.com/feed"
        try:
            self.assertEqual(app_module._crypto_news_feeds(), [("Mine", "https://example.com/feed")])
        finally:
            del os.environ["CRYPTOFORGE_NEWS_RSS_URLS"]

    def test_a_plain_http_feed_is_refused(self):
        os.environ["CRYPTOFORGE_NEWS_RSS_URLS"] = "Bad=http://example.com/feed"
        app_module._CRYPTO_NEWS_CACHE.clear()
        try:
            with self.assertRaises(RuntimeError) as caught:
                app_module._fetch_crypto_news()
            self.assertIn("HTTPS", str(caught.exception))
        finally:
            del os.environ["CRYPTOFORGE_NEWS_RSS_URLS"]
            app_module._CRYPTO_NEWS_CACHE.clear()
