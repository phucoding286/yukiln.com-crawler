import json
import ijson
import os

"""
Lớp này hổ trợ việc crawl và ghi dữ liệu lớn không tốn ram và tăng tốc độ.
Được phát triển bởi Phú, ngày 8 tháng 4 năm 2026.
Hướng dẫn sử dụng:
+ Tham số `file` của lớp, là đường dẫn đến file dataset bạn muốn ghi khi crawling.
+ Tham số `encoding` của lớp, là loại mã hóa khi ghi hoặc đọc file, ví dụ utf-8.
+ Tham số `metadata` của lớp, là đường dẫn đến metadata của mô đun.
+ Hàm `write()` dùng để ghi batch văn bản vào file dataset json với điều kiện file chưa locked (tức đã đóng ngoặc và trở thành một cấu trúc json list hoàn chỉnh).
+ Hàm `locked_file()` dùng để đóng ngoặc cho data trong file json, tạo ra cấu trúc json list hoàn chỉnh và có thể load.
+ Hàm `ijson_load()` dùng để load file vào danh sách, giúp tiết kiệm ram hơn so với json thường, có tham số giới hạn cho lần load `limit_sentence_loaded`. 

Lưu ý:
Nên xóa metadata đi nếu bạn crawl lại hoặc dùng lại tên file, nói chung là reset dự án.
"""

class JsonTextDataWriter:
    def __init__(self, file: str = "./datasets/dataset.json", encoding: str = None, metadata:str="./datasets/metadata.json"):
        self.file = file
        self.encoding = encoding
        self.metadata = metadata
        self.init_files(file, encoding)
        with open(self.metadata, encoding="utf-8", mode="r") as file:
            self.total_batches = json.load(file)[self.file]["total_batches"]

    def init_files(self, file: str = "dataset.json", encoding: str = None):
        file_write_object = open(file, mode="a", encoding=encoding)
        file_load_object = open(file, mode="r", encoding=encoding)
        if file_load_object.readline(1) != "[":
            file_write_object.write("[")
        file_write_object.close()
        file_load_object.close()
        
        if not os.path.exists(self.metadata):
            with open(self.metadata, encoding="utf-8", mode="w") as file:
                data = {
                    self.file: {
                        "locked_file": False,
                        "total_batches": 0
                    }
                }
                json.dump(data, file, indent=4, ensure_ascii=False)
        
        with open(self.metadata, encoding="utf-8", mode="r") as file:
            data = json.load(file)
        
        if self.file not in data:
            data[self.file] = {
                "locked_file": False,
                "total_batches": 0
            }
            with open(self.metadata, encoding="utf-8", mode="w") as file:
                json.dump(data, file, indent=4, ensure_ascii=False)

    def write(self, text_sentence: str):
        self.total_batches += 1
        with open(self.metadata, encoding="utf-8", mode="r") as file:
            metadata = json.load(file)
        if metadata[self.file]["locked_file"]:
            raise ValueError("File đã được locked, không thể ghi thêm.")
        metadata[self.file]["total_batches"] = self.total_batches
        with open(self.metadata, encoding="utf-8", mode="w") as file:
            json.dump(metadata, file, indent=4, ensure_ascii=False)

        file_write_object = open(self.file, mode="a", encoding=self.encoding)
        file_load_object = open(self.file, mode="r", encoding=self.encoding)
        
        text_sentence = text_sentence.replace("\n", "\\n").replace("\"", "\\\"")
        if self.total_batches > 1:
            file_write_object.write(",\n    " + f'"{text_sentence}"')
        else:
            file_write_object.write("\n    " + f'"{text_sentence}"')

        file_write_object.close()
        file_load_object.close()

    def locked_file(self):
        with open(self.metadata, encoding="utf-8", mode="r") as file:
            metadata = json.load(file)
        if not metadata[self.file]["locked_file"]:
            file_write_object = open(self.file, mode="a", encoding=self.encoding)
            file_write_object.write("\n]")
            file_write_object.close()
        with open(self.metadata, encoding="utf-8", mode="w") as file:
            metadata[self.file]["locked_file"] = True
            json.dump(metadata, file, indent=4, ensure_ascii=False)


    def ijson_load(self, limit_sentence_loaded=-1):
        with open(self.metadata, encoding="utf-8", mode="r") as file:
            if not json.load(file)[self.file]["locked_file"]:
                raise ValueError("Vẫn chưa locked file, hãy gọi hàm này để tiến hành khóa dataset thành cấu trúc danh sách json, để json có thể đọc được\nLưu ý! điều này đồng nghĩa việc không thể ghi thêm.")
        result = []
        i = 0
        with open(self.file, "r", encoding=self.encoding) as f:
            for item in ijson.items(f, "item"):
                result.append(item)
                if limit_sentence_loaded != -1:
                    i += 1
                if limit_sentence_loaded != -1 and i >= limit_sentence_loaded:
                    break
        return result

if __name__ == "__main__":
    j = JsonTextDataWriter(encoding="utf-8")
    print("Tổng câu đã lưu là:", j.total_batches)
    # j.write("I am Algo.")
    # j.locked_file()
    print(j.ijson_load())
