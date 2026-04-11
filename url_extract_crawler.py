import cloudscraper
from bs4 import BeautifulSoup
import json
import os
import random
import unicodedata
import html
scraper = cloudscraper.create_scraper()

"""
Module này dùng để gửi yêu cầu trích xuất urls từ một đường dẫn trang web cụ thể, sau đó trả về content và các urls liên quan trong content hiện tại.
 + Tham số lớp `source_url` chính là url gốc của trang web bạn muốn crawl ví dụ "https://vnexpress.net/".
 + Tham số lớp `metadata_write_delay` là delay cho việc crawled bao nhiêu lần thì sẽ lưu metadata, tránh spam write gây stress disk.
 + Hàm `requests_and_extract_urls_incontent()` dùng để crawl, tự trích xuất urls, và trả về danh sách urls và content dạng source code, dành cho việc crawl.
     - Hàm trả về False, có nghĩa là url không thuộc url gốc, thường là bỏ qua.
     - Hàm trẻ về chuỗi "ERROR" tức là có lỗi trong quá trình crawl và xử lý.
"""

def normalize_text(text):
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text)
    table = str.maketrans({
        "“": '"',
        "”": '"',
        "‘": "'",
        "’": "'",
        "–": "-",
        "—": "-",
        "…": "...",
    })
    return text.translate(table)

def get_text(html_content: str, min_length: int = 300):
    soup = BeautifulSoup(html_content, "html.parser")
    tags = soup.find_all(["h1","h2","h3","h4","h5","h6","p"])
    total_text = str()
    for tag in tags:
        text = tag.get_text(strip=True)
        if text == "":
            continue
        if tag.get_text(strip=False)[-1] == "\n":
            text = text + "\n"
        if tag.name == "h1":
            text = "# " + text
        elif tag.name == "h2":
            text = "## " + text
        elif tag.name == "h3":
            text = "### " + text
        elif tag.name == "h4":
            text = "#### " + text
        elif tag.name == "h5":
            text = "##### " + text
        elif tag.name == "h6":
            text = "###### " + text
        total_text += text + "\n"
    total_text = total_text.strip()
    total_text = normalize_text(total_text)
    # input(len(total_text.split(" ")))
    if len(total_text.split(" ")) < min_length:
        return False
    return total_text



class UrlExtractorCrawler:
    def __init__(self, source_url: str, folder_path: str = "./url_extract_crawler_metadata", metadata_path: str = "metadata.json", metadata_write_delay=64):
        self.source_url = source_url
        self.folder_path = folder_path
        self.metadata_path = folder_path + "/" + metadata_path
        self.metadata_write_delay = metadata_write_delay
        self.crawling_times = 0
        self.headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        }
        self.metadata = {
            "previous_url": [source_url]
        }
        self.init_folder_files()

    def init_folder_files(self):
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
        if not os.path.exists(self.metadata_path):
            with open(self.metadata_path, mode="w", encoding="utf-8") as file:
                json.dump(self.metadata, file, indent=4, ensure_ascii=False)

    def auto_write_metadata(self):
        self.crawling_times += 1
        if self.crawling_times >= self.metadata_write_delay:
            self.crawling_times = 0
            with open(self.metadata_path, mode="w", encoding="utf-8") as file:
                json.dump(self.metadata, file, indent=4, ensure_ascii=False)

    def requests_and_extract_urls_incontent(self):
        url = random.choice(self.metadata['previous_url'])

        url = url.strip().split("?")[0].split("#")[0]
        if url[-1] == "/":
            while url[-1] == "/":
                url = url[:-1]
            url = url + "/"

        if not url.startswith(self.source_url):
            return False
        
        try:
            urls_extracted = set()
            response = scraper.get(url=url, headers=self.headers, timeout=10)
            website_content = response.text
            soup = BeautifulSoup(website_content, "html.parser")
            urls_content = soup.find_all("a")
            for u in urls_content:
                u = u.get_attribute_list("href")
                if str(u) == "[]":
                    continue
                u = u[0]
                if u.startswith(self.source_url):
                    urls_extracted.add(u)
                elif "/" in u and not u.startswith("https://"):
                    if self.source_url.endswith("/") and u.startswith("/"):
                        u = self.source_url[:-1] + u
                        urls_extracted.add(u)
                    elif not self.source_url.endswith("/") and not u.startswith("/"):
                        u = self.source_url[:-1] + "/" + u
                        urls_extracted.add(u)
            urls_extracted = list(urls_extracted)
            self.auto_write_metadata()
            random.shuffle(self.metadata["previous_url"])
            self.metadata['previous_url'] = urls_extracted + self.metadata["previous_url"][:5]
            return urls_extracted, url, website_content
        except:
            return "ERROR"

if __name__ == "__main__":
    import time
    u = UrlExtractorCrawler("https://vnexpress.net/", metadata_write_delay=2)
    while True:
        pack = u.requests_and_extract_urls_incontent()
        input(get_text(pack[1]))
        os.system("cls")