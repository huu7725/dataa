# Tăng tốc crawler ngày 12/10/2026

Đã áp dụng cấu hình theo từng website và cho crawler chạy lại nền, tiếp tục từ các ID đã lưu.

| Khoảng đo | Tốc độ lưu quan sát |
|---|---:|
| Cấu hình một kết nối mỗi website, trước tối ưu | khoảng 0,9 bài/giây, lúc báo Tây Ninh còn dữ liệu |
| Hai kết nối, khoảng cách 0,35 giây | khoảng 1,8–2 bài/giây, chưa tăng lỗi trong khoảng đo |
| Chỉ còn `ask.39.net` dùng desktop | khoảng 0,1–0,5 bài/giây, thường khoảng 0,2 |
| `ask.39.net` dùng bản di động chính thức | khoảng 2,7–3 bài/giây qua nhiều cửa sổ đo 10 giây |
| Bản cuối, sau sửa thời gian chờ bộ lập lịch | khoảng 3,8–4 bài/giây qua các cửa sổ đo 10 giây |

Các khoảng đo dùng các URL khác nhau và trạng thái website có thể thay đổi; đây là tốc độ quan sát thực tế, không phải cam kết tốc độ cố định. Lượt kiểm tra bản di động đã lưu **371 bài** từ `ask.39.net`, nâng tổng số ID lưu tại thời điểm dừng sạch lên **3.308.012**. Crawler chạy nền sau đó tiếp tục bổ sung.

Lượt chạy cuối đã ghi nhận 3,8, 3,9 và 4 bài/giây trong ba cửa sổ liên tiếp. Tiến trình vẫn chạy; không có lỗi trong `crawl_stderr.log` và không có bản ghi sai ID câu hỏi di động. Kết quả đo cùng cấu hình được lưu ở `crawl_speed_metrics.json`.

Các thay đổi đã áp dụng:

- `ask.39.net`: bốn kết nối, khoảng cách 0,25 giây, lấy trang `wapask.39.net` mà website desktop khai báo là bản di động; kiểm tra ID câu hỏi và đường dẫn trước khi lưu. Giữ nguyên ID/URL gốc trong dữ liệu.
- Báo Tây Ninh: bốn kết nối, khoảng cách 0,2 giây.
- Các nguồn thường timeout: thời gian chờ kết nối năm giây, không thử ngay hai lần cùng URL; sau tám lỗi liên tục thì hoãn nguồn sang lượt sau.
- Các nguồn thường trả trang xác minh/403: hoãn sớm sau tám lỗi liên tục, giữ URL ở trạng thái có thể thử lại.
- Sửa lỗi phân lượt khiến các tên miền con ở đầu danh sách chiếm hết lượt của `39.net`; các tên miền phía sau được phục vụ công bằng.
- Bộ lập lịch đợi theo thời điểm được phép gửi yêu cầu tiếp theo để giảm thời gian chờ dư.
- Giữ một tiến trình trích xuất vì RAM trống của máy có lúc chỉ khoảng 567 MB. Giới hạn tổng kết nối là 64; giới hạn dùng chung nhóm `39.net` vẫn khoảng bốn yêu cầu/giây.

Cấu hình lưu trong `crawl_host_settings.json` và `corpus_data\crawl_control.json`; mở lại `crawl.bat` vẫn giữ cấu hình từng nguồn. Tiến độ có thêm `saved_by_host`, `failed_by_host`, `active_requests_by_host` để xem nguồn nào đang thực sự góp dữ liệu.

Đã chạy kiểm thử tiếp tục sau lỗi, chống tải trùng, dừng an toàn, cấu hình timeout/retry theo website, kiểm tra đúng ID bản di động và phân lượt công bằng. Mobile 404 không bị đánh dấu giả là URL desktop mất vĩnh viễn. Các bản ghi không đúng ID và trang xác minh đều bị loại.

Bản Parquet đã chuẩn bị ở lượt trước vẫn là bản chụp cố định. Sau một lần dừng sạch, chạy `python audit_corpus.py` và `python prepare_corpus.py` để xuất thêm bản chụp có các tài liệu mới. Một số nguồn còn chặn hoặc không phản hồi nên chưa thu thập đủ toàn bộ URL.
