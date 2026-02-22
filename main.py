import random
import sys
import time
import json
import threading

import logging
from logging.handlers import RotatingFileHandler

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote_plus, urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

print("START parser_service.py")

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/120.0"}
onliner_url = 'https://baraholka.onliner.by/'
kufar_api = 'https://api.kufar.by/search-api/v2/search/rendered-paginated'

BASE_KUFAR_PARAMS = {
    "lang": "ru",
    "size": 50,
    "sort": "lst.d",
    "query": "NONE",
}

SESSION = requests.session()
SESSION.headers.update(HEADERS)

SEMAPHORE_ONLINER = threading.Semaphore(5)
SEMAPHORE_KUFAR = threading.Semaphore(5)
CRAWL_SEMAPHORE = threading.Semaphore(5)

LOG_FILE = 'parser.log'
logger = logging.getLogger('parser.log')
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(name)s, %(message)s')
handler.setFormatter(formatter)

console = logging.StreamHandler(stream=sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(console)
    logger.addHandler(handler)


class Page:

    def __init__(self):
        self.page_url = None

class Utilities:

    @classmethod
    def normalize_price(cls, price):
        numerical_price = round((int(price) / 100), 2)
        normalized_price = str(numerical_price).replace('.', ',') + ' р.'
        return normalized_price

    @classmethod
    def gap_check(cls, price, min_price, max_price):
        clean = price.replace(' ', '').replace(',', '.').replace('р.', '').strip()
        value = int(float(clean))
        if min_price is not None and max_price is not None:
            return min_price <= value <= max_price
        if min_price is not None:
            return min_price <= value
        if max_price is not None:
            return value <= max_price
        return True


# ---------------HTTPServer---------------
class ParserHandler(BaseHTTPRequestHandler):

    def json_response(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header('content-type', 'application/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        client_ip = self.client_address[0]
        logger.info("Incoming request from %s path=%s", client_ip, self.path)
        parsed = urlparse(self.path)

        if parsed.path == "/favicon.ico":
            logger.debug("Ignored favicon from %s", client_ip)
            self.send_response(204)  # no content
            self.end_headers()
            return None

        if parsed.path != "/cost_parser/api/v1/search":
            logger.warning("Not found path=%s from %s", parsed.path, client_ip)
            self.send_response(404)  # page not found
            self.end_headers()
            return None

        qs = parse_qs(parsed.query)
        q_list = qs.get('q')
        if not q_list or not q_list[0].strip():
            logger.warning("q is missing")
            return self.json_response(400, {"error": "q обязательно"})  # bad request
        q = q_list[0].strip()

        if 'min_price' in qs:
            min_price = int(qs.get('min_price')[0])
        else:
            min_price = None
        if 'max_price' in qs:
            max_price = int(qs.get('max_price')[0])
        else:
            max_price = None

        excluded_raw = qs.get('exclude', [])
        excluded_list = []
        for er in excluded_raw:
            part = [w.strip() for part in er.split(",") for w in part.split()]
            excluded_list.extend(part)

        acquired = CRAWL_SEMAPHORE.acquire()
        if not acquired:
            logger.warning("Too many requests, you need to wait", client_ip)
            return self.json_response(429, {"error": "Too many current requests"})

        try:
            crawler = Crawler()
            results = crawler.crawl(q, excluded_list, min_price, max_price)
        except Exception as e:
            logger.exception("Error while handling request q=%s", q)
            return self.json_response(500, {"error": "error", "detail": str(e)})  # server error
        finally:
            CRAWL_SEMAPHORE.release()

        logger.info("Request handled q=%s items=%d", q, len(results))
        return self.json_response(200, results)  # ok


class Onliner(Page):

    def __init__(self):
        super().__init__()
        self.results = []

    @staticmethod
    def get_soup(url):
        delay = random.uniform(0.5, 1.2)
        time.sleep(delay)
        acquired = SEMAPHORE_KUFAR.acquire(timeout=5)
        if not acquired:
            logger.warning("Too many requests, please wait", url)
            return None
        try:
            response_o = SESSION.get(url, headers=HEADERS)
            # response_o = SESSION.get(url, headers=HEADERS, proxies=PROXIES)
            logger.info("HTTP GET %s status=%s ", url, response_o.status_code)
            return BeautifulSoup(response_o.text, "lxml")
        except requests.RequestException:
            logger.exception("Onliner request failed for %s", url)
            return None
        finally:
            SEMAPHORE_ONLINER.release()

    @staticmethod
    def search_onliner(search_word, soup):
        search_form = soup.find("form", class_="b-searchsubj")
        if not search_form:
            return None
        script_url = search_form.get("action")
        input_n = search_form.find("input")
        if not input_n:
            return None
        name = input_n.get("name")
        if not name:
            return None
        encode = quote_plus(search_word)
        return urljoin(onliner_url, script_url) + "?" + name + "=" + encode

    @staticmethod
    def parse_onliner(soup, excluded_words, min_price, max_price):
        items = []
        tb_tags = soup.find_all("table", class_="ba-tbl-list__table")
        if not tb_tags:
            logging.warning("Ошибка в разметке страницы Онлайнер.")
            return items
        for tb in tb_tags:
            for tdph in tb.find_all("td", class_="frst ph colspan"):

                tr = tdph.find_parent("tr")
                if tr.has_attr("class") and "m-imp" in tr["class"]:
                    continue

                name_container = tr.find("td", class_="txt")
                price_container = tr.find("td", class_="cost")

                name = ""
                if name_container:
                    name_tag = name_container.find("h2", class_="wraptxt")
                    if name_tag:
                        name = name_tag.text
                if not name:
                    continue
                name_l = name.lower()
                if any(ex_word.lower() in name_l for ex_word in excluded_words):
                    continue

                price = ""
                if price_container:
                    price_tag = price_container.find("div", class_="price-primary")
                    if price_tag:
                        price = price_tag.text

                if not price or price == '0':
                    continue

                if Utilities.gap_check(price, min_price, max_price) is False:
                    continue

                item_url = ""
                if name_container:
                    url_tag = name_container.find("a")
                    if url_tag:
                        href = url_tag.get("href")
                        item_url = urljoin(onliner_url, href)

                items.append({"name": name, "price": price, "item_url": item_url, "source": "Барахолка онлайнер"})

        return items

    @staticmethod
    def find_next_onliner_page(soup, current_url):
        li_tags = soup.select("ul.pages-fastnav li")
        for li in reversed(li_tags):
            next_page = li.find("a")
            if next_page and next_page.get("href"):
                return urljoin(current_url, next_page["href"])
        return None

    def crawl_onliner(self, search_word, excluded_words, min_price, max_price, max_pages=10):
        pages = 0
        visited = set()

        base_soup = self.get_soup(onliner_url)
        if base_soup is None:
            return self.results

        self.page_url = self.search_onliner(search_word, base_soup)

        while self.page_url and pages < max_pages:
            if self.page_url in visited:
                break
            soup = self.get_soup(self.page_url)
            visited.add(self.page_url)

            self.results.extend(self.parse_onliner(soup, excluded_words, min_price, max_price))

            next_url = self.find_next_onliner_page(soup,  self.page_url)
            self.page_url = next_url
            pages += 1
        return self.results


class Kufar(Page):

    def __init__(self):
        super().__init__()
        self.results = []

    @staticmethod
    def get_page(params):
        delay = random.uniform(0.5, 1.2)
        time.sleep(delay)
        acquired = SEMAPHORE_KUFAR.acquire(timeout=5)
        if not acquired:
            logger.warning("Too many requests, please wait", params)
            return None
        try:
            response_k = SESSION.get(kufar_api, headers=HEADERS, params=params)
            logger.info("HTTP GET %s status=%s ", kufar_api, response_k.status_code)
            return response_k.json()
        except requests.RequestException:
            logger.exception("Kufar request failed for %s", kufar_api)
            return None
        finally:
            SEMAPHORE_KUFAR.release()

    @staticmethod
    def parse_kufar(data, excluded_words, min_price, max_price):
        items = []
        ads = data.get("ads", [])
        for ad in ads:

            name = ad.get("subject")
            if not name:
                continue
            name_l = name.lower()
            if any(ex_word.lower() in name_l for ex_word in excluded_words):
                continue

            price = ad.get("price_byn")
            if not price or price == '0':
                continue
            price = Utilities.normalize_price(price)
            if Utilities.gap_check(price, min_price, max_price) is False:
                continue

            item_url = ad.get("ad_link")

            items.append({"name": name, "price": price, "item_url": item_url, "source": "Куфар"})

        return items

    @staticmethod
    def find_next_kufar_token(data, tparams):
        pages = (data.get("pagination")).get("pages", [])
        if pages:
            next_page = next((p for p in pages if p.get("label") == "next"), None)
            if next_page and next_page.get("token"):
                tparams["cursor"] = next_page["token"]
                return tparams
        return None

    def crawl_kufar(self, search_word, excluded_words, min_price, max_price, max_pages=10):
        params = BASE_KUFAR_PARAMS.copy()
        params['query'] = search_word
        visited_tokens = set()
        pages = 0

        while pages < max_pages:
            data = self.get_page(params)
            if data is None:
                return self.results
            self.results.extend(self.parse_kufar(data, excluded_words, min_price, max_price))

            new_params = self.find_next_kufar_token(data, params)
            if not new_params:
                break

            cursor = new_params.get("cursor")
            if cursor:
                if cursor in visited_tokens:
                    break
                visited_tokens.add(cursor)

            params = new_params
            pages += 1
        return self.results

class Crawler:

    def __init__(self):
        self.onliner = Onliner()
        self.kufar = Kufar()

    def crawl(self, search_word, excluded_words, min_price, max_price):
        onliner_results = self.onliner.crawl_onliner(search_word, excluded_words, min_price, max_price, max_pages=10)
        kufar_results = self.kufar.crawl_kufar(search_word, excluded_words, min_price, max_price, max_pages=10)
        merged_results = onliner_results + kufar_results
        return merged_results


# ---------------MAIN---------------
if __name__ == "__main__":
    host = "0.0.0.0"
    port = 8000
    server = ThreadingHTTPServer((host, port), ParserHandler)
    try:
        logger.info("Start parser on %s:%s", host, port)
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
        exit()

