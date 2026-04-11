import os
import json

"""
Module này dùng để lọc trùng lặp các đường link, phục vụ cho việc crawl dữ liệu.
Tham số lớp `folder_path` là đường dẫn đến folder chứa các `chunk files` và `metadata`.
Tham số lớp `limit_lines_on_file_chunk` là giới hạn số dòng tối đa ở một chunk file, nếu vượt qua sẽ tạo chunk file mới.
Hàm `check_duplicate_nupdate` dùng để check xem url đã crawl hay chưa, sau đó update url đó vào lịch sử.
 + Hàm này trả về True thì url hiện đã từng crawl.
 + Hàm này trả về False có nghĩa là url hiện là mới.
Module này dùng chiến lược chunk files để giảm mức sử dụng ram và tăng tốc độ hơn.
"""

class UrlDublicateFilter:
    def __init__(self, folder_path="./url_dublicate_working_data", metadata_path="metadata.json", limit_lines_on_file_chunk=10000):
        self.folder_path = folder_path
        self.metadata_path = folder_path + "/" + metadata_path
        self.limit_lines_on_file_chunk = limit_lines_on_file_chunk
        self.init_folder_files()
        self.chkncreate_new_file_infolder()

    def chkncreate_new_file_infolder(self):
        with open(file=self.metadata_path, mode="r", encoding="utf-8") as file:
            metadata = json.load(file)
        
        if len(metadata) == 0:
            path = self.folder_path + "/" + f"{len(metadata)}.txt"
            with open(path, mode="w", encoding="utf-8"):
                pass
            metadata.append(path)
            with open(file=self.metadata_path, mode="w", encoding="utf-8") as file:
                json.dump(obj=metadata, fp=file, indent=4, ensure_ascii=False)

        elif len(metadata) > 0:
            with open(metadata[-1], mode="r", encoding="utf-8") as file:
                urls = file.readlines()
            if len(urls) > self.limit_lines_on_file_chunk:
                path = self.folder_path + "/" + f"{len(metadata)}.txt"
                with open(path, mode="w", encoding="utf-8"):
                    pass
                metadata.append(path)
                with open(file=self.metadata_path, mode="w", encoding="utf-8") as file:
                    json.dump(obj=metadata, fp=file, indent=4, ensure_ascii=False)

    def init_folder_files(self):
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
        if not os.path.exists(self.metadata_path):
            with open(file=self.metadata_path, mode="w", encoding="utf-8") as file:
                json.dump(obj=[], fp=file, indent=4, ensure_ascii=False)

    def check_dublicate_nupdate(self, url: str):
        for filepath in os.listdir(self.folder_path):
            path = self.folder_path + "/" + filepath
            if self.metadata_path.endswith(filepath):
                continue
            with open(file=path, encoding="utf-8", mode="r") as file:
                urls = file.read().splitlines()
            if url in urls:
                return True
        with open(file=self.metadata_path, mode="r", encoding="utf-8") as file:
            metadata = json.load(file)
        with open(file=metadata[-1], encoding="utf-8", mode="a") as file:
            file.write(url + "\n")
        self.chkncreate_new_file_infolder()
        return False

if __name__ == "__main__":
    import random
    u = UrlDublicateFilter(limit_lines_on_file_chunk=1000)
    for _ in range(20000):
        seq = list("qwertyuiopasdfghjklzxcvbnm")
        random.shuffle(seq)
        seq = "".join(seq)
        print(u.check_dublicate_nupdate(f"https://{seq}.com/"))