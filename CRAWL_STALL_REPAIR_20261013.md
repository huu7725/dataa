# Sửa tình trạng crawl đứng ở 1.039.536 ID

Crawler đang hoạt động nhưng không còn nguồn tải nhanh. PubMed đã lưu 239.355/240.000 bài; 645 PMID còn lại không có abstract hợp lệ trong response. Khoảng 154.933 ID chưa có nội dung, chủ yếu thuộc Long Châu (79.598), Lao Động (31.282), VOV (9.784) và các nguồn trả trang chặn. Các số này là bản kiểm tra lúc bắt đầu sửa, không phải tuyên bố đã tải đủ.

Nguyên nhân màn hình báo 0 bài/giây: VOV trả 403 liên tục. Sau tám lỗi, crawler đặt cooldown 30 giây nhưng phải tới 32 lỗi mới hoãn host, nên cuối mỗi lượt có thể mất thêm khoảng 12 phút chỉ để lặp lỗi. Hàng đợi HTML/output trống, tiến trình vẫn cập nhật và không có dấu hiệu deadlock.

Đã giảm ngưỡng hoãn VOV và QDND xuống tám lỗi, giữ URL còn thiếu ở trạng thái retryable. Các URL binary còn lại ở Thầy Thuốc Việt Nam được kiểm tra với hai kết nối và khoảng cách 0,25 giây. Không đổi nội dung tài liệu, quy tắc ID, loại bỏ trang xác minh hoặc commit JSONL trước SQLite.

Đã thêm `activity=waiting_host_cooldown`, `cooling_hosts` theo tên website/số giây và `pass_number` ngay khi chạy. Cửa sổ theo dõi mới hiển thị rõ lý do chờ. Khởi động lại bằng `crawl.bat` sẽ dùng bản màn hình mới.

Lượt sửa dừng sạch từ 1.039.536 ID và tiếp tục từ checkpoint với một parser, 48 kết nối tổng, tối đa hai lượt thử. Lượt đầu mới hoàn tất trong khoảng 45 giây và lưu thêm 52 bài; trước đó lượt có thể kéo dài hơn mười phút vì cooldown. Sau hai lượt, crawler ghi `finished_with_missing`, manifest và `missing_urls.jsonl` rồi nhường tài nguyên cho pipeline tại `competition\work_vi_en_zh`. Đây là hoàn tất giai đoạn thử nguồn hiện tại, chưa phải thu thập thành công 100% URL.

Kiểm thử đã qua: HTTP 429 tạo cooldown được hiển thị khi active requests bằng 0, stop trong cooldown không gửi thêm URL và thoát nhanh; kiểm thử chống trùng/resume, parser recovery, PubMed batch/đúng PMID và ZIP/schema trước đó vẫn đạt. Các bản sao trước chỉnh trong `repair_20261013`; toàn bộ snapshot dừng sạch cũng được chạy audit.

Kết quả sau hai lượt sửa: 1.039.588 tài liệu đã lưu, 154.881 ID chưa có nội dung. Crawler thoát sạch với `finished_with_missing`/`max_passes`. Không coi ID chưa thu thập là thành công. Pipeline tự chuyển sang `chunking_and_lexical_index` trong `competition\work_vi_en_zh`, xác nhận công việc xây hệ thống tiếp tục sau khi crawl kết thúc. Bản audit đầy đủ trước hai lượt sửa xác nhận 1.039.536 dòng/ID hợp lệ, không trùng; các shard phát sinh trong lượt sửa được kiểm tra riêng theo ID nguồn và nội dung.
