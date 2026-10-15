# Chuyển sang links_corpus_vi_en_zh.parquet

Đã áp dụng thông báo cập nhật nguồn của Ban Tổ chức ngày 12/10/2026.

| Kiểm tra | Kết quả |
|---|---:|
| URL trong tệp mới | 1.194.469 |
| ID duy nhất | 1.194.469 |
| ID giao với danh sách cũ | 954.469 |
| ID mới trong danh sách cập nhật | 240.000 |
| ID cũ bị loại khỏi danh sách cập nhật | 3.440.249 |
| URL thay đổi trên phần giao | 0 |
| URL PubMed bổ sung | 240.000 |
| Bài đã giữ từ crawl cũ | 787.727 |
| URL còn cần thử lúc bắt đầu chuyển đổi | 406.742 |

Tệp mới được tải từ `AIGuruTinix/ViBioMIR` và có SHA256 `9e34cb4b7d6424b80ed2634ada581ea9e2d97da69c245d80bdbb31413a78e8ce`. Kho cũ `D:\R2AI\corpus_data` được giữ nguyên. Kho mới nằm ở `D:\R2AI\corpus_data_vi_en_zh`; chỉ bản ghi có ID và URL trong danh sách mới được sao chép. Không có URL lệch trên bản ghi được giữ.

Crawler mới dùng `links_corpus_vi_en_zh.parquet`, hai parser, 64 kết nối tổng và profile `ask.39.net` dùng bản di động với tối đa 16 kết nối. Crawler hiện đang chạy tiếp phần URL mới; các URL 403, 404, timeout hoặc challenge vẫn giữ trạng thái retryable/deferred.

Đối với 240.000 URL PubMed, NCBI trực tiếp trả trang chặn truy cập. Crawler dùng Europe PMC REST search công khai để lấy bản ghi `source=MED` theo đúng PMID, sau đó giữ URL PubMed và ID gốc trong corpus. Mỗi batch tối đa 50 PMID, response được đọc đủ theo stream, kiểm tra PMID/title/abstract, nén lưu cache và ghi checksum/provenance tại `corpus_data_vi_en_zh\pubmed_api_provenance.jsonl` và `pubmed_api_cache`. Hai PMID không có abstract bị giữ retryable; không sinh nội dung thay thế.

Do ID danh sách mới lên tới 7.466.161, `audit_corpus.py` và `prepare_corpus.py` đã bỏ giới hạn ID 5 triệu và kiểm tra theo `max_id` trong manifest. Bản export mới sẽ nằm trong `corpus_ready_vi_en_zh`.

Pipeline cuộc thi đã chuyển sang file link mới, manifest mới và thư mục `competition\work_vi_en_zh`; nó chỉ lập chỉ mục sau khi crawler mới kết thúc sạch. Checkpoint cũ không được trộn vì fingerprint nguồn đã thay đổi.

```powershell
& 'D:\R2AI\start_crawl_vi_en_zh.ps1'
& 'D:\R2AI\stop_crawl_vi_en_zh.ps1'
& 'D:\R2AI\competition\run_after_crawl.ps1'
```

`crawl.bat` và `stop_crawl.bat` đã trỏ sang corpus mới. `revised_links_audit_20261012.json` ghi audit URL/ID/host; `corpus_data_vi_en_zh\source_links.json` ghi checksum và số bản ghi đã sao chép.
