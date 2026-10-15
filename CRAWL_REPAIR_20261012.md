# Kết quả sửa crawler ngày 12/10/2026

Crawler đã được sửa và khởi động lại chạy nền. Bộ dữ liệu sẵn dùng cho hệ thống nằm tại `D:\R2AI\corpus_ready\20261012T034551Z`.

| Dữ liệu trong bản xuất | Kết quả |
|---|---:|
| Tài liệu có ID duy nhất | 3.306.161 |
| Dòng trùng ID đã loại khỏi bản xuất | 25.543 |
| File Parquet | 133 |
| Dung lượng nén | 2.696.523.216 byte, khoảng 2,70 GB |
| Tổng ID trong danh sách URL gốc tại máy | 4.394.718 |
| ID chưa có nội dung trong bản xuất | 1.088.557 |

Bản xuất lấy từ thời điểm dừng sạch lúc **10:45:51 ngày 12/10/2026, UTC+7**. Crawler chạy tiếp sẽ bổ sung dữ liệu mới vào `corpus_data`; bản Parquet này là một ảnh chụp dữ liệu cố định và chưa bao gồm các bản ghi tải sau thời điểm trên. Trường dữ liệu được giữ nguyên: `doc_id`, `url`, `clean_text`. File gốc được giữ nguyên.

Lỗi chính là `PermissionError: [WinError 5]` khi thay file tiến độ trên Windows. Tiến trình đã chết nhưng màn hình còn hiển thị trạng thái chờ thử lại. Đã sửa cách ghi/đọc file tiến độ, thêm bộ tự khởi động lại tiến trình lỗi, lưu vị trí theo website để không lặp mãi các URL lỗi ở đầu danh sách, giảm tải mỗi website và giới hạn các vòng không có tiến triển. Đã bổ sung đọc nội dung bài báo Tây Ninh bị bộ trích xuất cũ bỏ sót.

Trong lượt kiểm tra sau khi sửa, crawler đã lưu thêm trên 500 bản ghi, gồm báo Tây Ninh và các nguồn `39.net`. Tốc độ thay đổi theo khả năng truy cập của website. Một số nguồn vẫn chặn bằng trang xác minh, trả 403, lặp chuyển hướng hoặc hết thời gian kết nối. Chưa thu thập đủ 100% danh sách URL. Các vòng thử lại kết thúc sẽ ghi rõ số còn thiếu; các URL này không bị đánh dấu giả là đã có nội dung.

Các file để sử dụng và kiểm tra:

- `corpus_ready\20261012T034551Z\manifest.json`: danh sách 133 file Parquet để đưa vào hệ thống, số bản ghi và schema.
- `corpus_data\corpus_audit.json`: kiểm tra toàn bộ 3.331.704 dòng JSONL của bản chụp; mọi dòng có cú pháp và các trường bắt buộc hợp lệ, tương ứng 3.306.161 ID duy nhất.
- `corpus_data\corpus_manifest.json`: thống kê bản chụp gần nhất; trường `complete` hiện là `false`.
- `corpus_data\missing_urls.jsonl`: danh sách ID và URL chưa có nội dung để tiếp tục khôi phục.
- `corpus_data\crawl_status.json`: tiến độ đang chạy, có thể mới hơn bản xuất.
- `crawl_stderr.log`, `crawl_supervisor.log`: lỗi và thông tin khởi động lại.

Đã kiểm thử tiếp tục sau sự cố, không tải trùng ID đã lưu, dừng an toàn, phục hồi lỗi parser, đọc/ghi tiến độ bị khóa trên Windows, chạy qua nhóm URL lỗi đầu website và kết thúc có danh sách còn thiếu. Đã đọc lại toàn bộ cột ID của 133 file Parquet: đủ 3.306.161 ID, không trùng. Schema/số dòng của từng file và nội dung đầu/cuối được kiểm tra sau xuất. Các kiểm tra này xác nhận cấu trúc dữ liệu, không đánh giá độ chính xác y khoa của nội dung.

Nhấp đúp `crawl.bat` để xem hoặc chạy tiếp, `stop_crawl.bat` để dừng và lưu sạch. Đóng cửa sổ theo dõi không dừng crawler. Cấu hình mặc định đã giảm xuống một kết nối mỗi website, cách nhau một giây; các tên miền con `39.net` dùng chung giới hạn khoảng bốn yêu cầu/giây. Mỗi lượt chạy tối đa 10 vòng, kết thúc sớm sau ba vòng liên tiếp không lưu thêm bài; URL còn thiếu vẫn được giữ để thử lại.

Sau một lần dừng sạch mới, chạy `python audit_corpus.py` và `python prepare_corpus.py` để tạo bản Parquet mới có cả dữ liệu bổ sung. Cách đọc Parquet theo từng batch để tiết kiệm RAM nằm trong `README.md`.
