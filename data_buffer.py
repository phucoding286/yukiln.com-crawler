"""
Lớp này được dùng để tạo bộ nhớ buffer trước khi ghi vào disk, điều này giúp bảo vệ tuổi thọ disk khi không spam write liên tục.
nhất là trong môi trường hay mất điện đột ngột, nó cũng giảm tỉ lệ hỏng dữ liệu xuống cực thấp, buffer size càng lớn thì càng an toàn
vì nó sẽ chờ lâu hơn để write vào disk.
"""

class DataBuffer:
    def __init__(self, buffer_size: int = 128):
        self.buffer_size = buffer_size
        self.buffer_data = list()
    
    def update(self, url: str, content: str):
        self.buffer_data.append((url, content))
        if len(self.buffer_data) >= self.buffer_size:
            buffer_data = self.buffer_data.copy()
            self.buffer_data = list()
            return buffer_data
        else:
            return None
        
if __name__ == "__main__":
    d = DataBuffer()
    for _ in range(129):
        r = d.update("test", "test")
        if r is not None:
            print(r)