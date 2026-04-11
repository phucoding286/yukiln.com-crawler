from json_writer_for_big_dataset import JsonTextDataWriter
from duplicate_filter import UrlDublicateFilter
from data_buffer import DataBuffer
from url_extract_crawler import get_text, UrlExtractorCrawler
import os, sys, time

datasets_folder = "./datasets"
if not os.path.exists(datasets_folder):
    os.makedirs(datasets_folder)

root_url = "https://yukiln.com/"
limit_batch_num = 1_000_000
current_file_crawlpart = f"{datasets_folder}/crawlpart1.json"
writer_metadata_path = f"{datasets_folder}/metadata.json"
buffer_size = 128
cooldown = 1
# loop text là phần text thường xuất hiện nhiều nhất trong nội dung và tần suất crawl, buộc phải bỏ qua nếu muốn dữ liệu sạch.
loop_text = "#### Thông tin\n#### Liên hệ\n• Email: yukiln.trans@gmail.com\n©  Yukiln.com - All rights reserved\n©  Yukiln.com - All rights reserved\n### ĐĂNG NHẬP\nTên đăng nhập hoặc Email\n\nMật khẩu\n\nGhi nhớ\nQuên mật khẩu ?\nQuay về Yukiln - Đọc Light Novel, Truyện Chữ\n### ĐĂNG KÝ\nVui lòng nhập đầy đủ thông tin bên dưới\nTên tài khoản\n\nĐịa chỉ Email\n\nMật khẩu\n\nĐăng nhập|Quên mật khẩu ?\nQuay về Yukiln - Đọc Light Novel, Truyện Chữ\n### Quên mật khẩu ?\nPlease enter your username or email address. You will receive a link to create a new password via email.\nUsername or Email Address\n\n← Back to  Yukiln - Đọc Light Novel, Truyện Chữ"

writer = JsonTextDataWriter(
    file=current_file_crawlpart,
    encoding="utf-8",
    metadata=writer_metadata_path
)
dup_filter = UrlDublicateFilter(
    limit_lines_on_file_chunk=10000,
    folder_path="./url_duplicate_working_data",
    metadata_path="metadata.json"
)
buffer = DataBuffer(buffer_size=buffer_size)
crawler = UrlExtractorCrawler(
    source_url=root_url,
    folder_path="./url_extract_crawler_metadata",
    metadata_path="metadata.json",
    metadata_write_delay=128
)

if sys.platform.startswith("win"):
    os.system('cls')
else:
    os.system("clear")

print("Thông số phiên hiện tại.")
print(f" - Folder chứa datasets: {datasets_folder}")
print(f" - Url gốc: {root_url}")
print(f" - Thời gian cooldown cho mỗi lần crawl: {cooldown}s")
print(f" - Giới hạn crawl trên một filepart: {limit_batch_num}")
print(f" - Filepart hiện tại là: {current_file_crawlpart}")
print("-------------------------------------")
print(f"# Tổng batch dữ liệu đã lưu hiện tại vào file: {writer.total_batches}")
print(f"# Kích cỡ buffer hiện tại: {len(buffer.buffer_data)}/{buffer.buffer_size}")
print(f" - Buffer sẽ lưu vào disk khi {len(buffer.buffer_data)} lớn hơn hoặc bằng {buffer.buffer_size}")

print("-------------------------------------")
if writer.total_batches >= limit_batch_num:
    input(f"CẢNH BÁO: Hiện tại filepart đã đạt full {writer.total_batches}/{limit_batch_num} batches, không thể crawl thêm.\nHãy thay đổi filepart nếu muốn crawl tiếp tục.\nEnter để đóng-> ")
    exit()


while True:
    time.sleep(cooldown)

    if sys.platform.startswith("win"):
        os.system('cls')
    else:
        os.system("clear")

    print("Thông số phiên hiện tại.")
    print(f" - Folder chứa datasets: {datasets_folder}")
    print(f" - Url gốc: {root_url}")
    print(f" - Thời gian cooldown cho mỗi lần crawl: {cooldown}s")
    print(f" - Giới hạn crawl trên một filepart: {limit_batch_num}")
    print(f" - Filepart hiện tại là: {current_file_crawlpart}")
    print("-------------------------------------")
    print(f"# Tổng batch dữ liệu đã lưu hiện tại vào file: {writer.total_batches}")
    print(f"# Kích cỡ buffer hiện tại: {len(buffer.buffer_data)}/{buffer.buffer_size}")
    print(f" - Buffer sẽ lưu vào disk khi {len(buffer.buffer_data)} lớn hơn hoặc bằng {buffer.buffer_size}")

    print("-------------------------------------")

    package = crawler.requests_and_extract_urls_incontent()
    crawl_result = "GOOD" if isinstance(package, tuple) else "BAD"
    print(f"Kết quả crawl: {crawl_result}!")
    if crawl_result == "BAD":
        continue

    urls, url, html_content = package
    print(url)
    text_content = get_text(html_content=html_content, min_length=300)
    extract_text_result = "BAD" if not text_content else "GOOD"
    print(f"Kết quả extract text: {extract_text_result}!")
    if extract_text_result == "BAD":
        continue
    text_content = text_content.replace(loop_text, "")

    buffer_data = buffer.update(url=url, content=text_content)
    if buffer_data is None:
        print(f"Đã update url và text content vào ram buffer, sẽ ghi khi buffer size {len(buffer.buffer_data)} lớn hơn hoặc bằng {buffer.buffer_size}")
    else:
        for pack in buffer_data:
            url, text_content = pack
            filt_result = dup_filter.check_dublicate_nupdate(url)
            if filt_result:
                continue
            writer.write(text_content)
        print("Đã hoàn thành quá trình ghi dữ liệu từ buffer vào disk.")

    if writer.total_batches >= limit_batch_num:
        writer.locked_file()
        input(f"HOÀN THÀNH NHIỆM VỤ: Hiện tại filepart đã đạt full {writer.total_batches}/{limit_batch_num} batches, không thể crawl thêm.\nHãy thay đổi filepart nếu muốn crawl tiếp tục.\nEnter để đóng-> ")
        exit()