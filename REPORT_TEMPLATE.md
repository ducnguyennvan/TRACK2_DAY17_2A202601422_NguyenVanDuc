# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Văn Đức  **MSSV:** 2A202601422  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details open>
<summary>Output ba lần chạy của make verify</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 16.1s
  run 2/3 … 17.1s
  run 3/3 … 17.4s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Khi Clear Task trên Airflow hoặc bấm chạy lại pipeline, `gold_training_set` tăng số lượng hàng liên tục (từ 12,480 lên 38,750 hàng), các `ticket_id` bị ghi lặp nhiều lần (12,480 ticket bị lặp). |
| **Nguyên nhân** | Model `gold_training_set.sql` được cấu hình `materialized = 'incremental'` nhưng thiếu khai báo `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. Mặc định dbt dùng chiến lược `append` (INSERT INTO) nên mỗi lần re-run dbt đều chèn thêm bản ghi mới. Nguồn CDC chứa bản ghi `op = 'u'` (update) khiến 1 ticket lọt qua bộ lọc `_ingested_at` ở nhiều lượt chạy. Đồng thời, DAG Airflow để `catchup=True` và thiếu `max_active_runs=1`, khiến việc Clear Task kích hoạt Airflow tự động chạy bù lại toàn bộ lịch sử và ghi đè lặp dữ liệu. |
| **Cách khắc phục** | - File `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào khối `config()`.<br>- File `dags/ai_training_pipeline.py`: Cập nhật `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng (thừa 26,270 hàng) · sau: 12,480 hàng (khớp expected) · checksum 3 lượt: `8dd7c98653` (giống hệt nhau) · `gold_training_set: 1 hàng / 1 ticket` ✓ |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu 455 hàng ở các ngày trong quá khứ (chỉ có 8,645 hàng so với kỳ vọng 9,100 hàng). |
| **P99 độ trễ đo được** | **2.73 ngày** *(bắt buộc)* (P50: 0.13 ngày, P95: 1.81 ngày, P99: 2.73 ngày, Max: 2.94 ngày, tỷ lệ đến muộn > 1 ngày: 5.05%) |
| **Lookback đã chọn** | **3 ngày** — vì P99 độ trễ đo được là 2.73 ngày ($\approx 3$ ngày), đủ để bảo đảm bao phủ 99% các bản ghi đến muộn mà không phải quét dư thừa dữ liệu quá xa. |
| **Nguyên nhân** | Khối `is_incremental()` của model dùng điều kiện `where event_date > (select max(event_date) from {{ this }})`. Khi một bản ghi phát sinh vào ngày `08-12` nhưng tới kho muộn vào ngày `08-15`, tại lượt chạy ngày `08-15` thì `max(event_date)` trong target đã là `08-14`, khiến bản ghi `08-12` bị loại bỏ và không bao giờ được tổng hợp vào `gold_feature_daily`. |
| **Cách khắc phục** | - File `dbt/models/gold/gold_feature_daily.sql`: Nới rộng lookback window trong `is_incremental()` thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`. Đồng thời thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` vào khối `config()` để ghi đè (upsert) chính xác các partition ngày được tính lại mà không làm trùng lặp hàng. |
| **Bằng chứng** | trước: 8,645 hàng (thiếu 455 hàng) · sau: 9,100 hàng (khớp expected) · checksum 3 lượt: `f8d3f591f0` |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> **Trả lời:** Chọn P99 làm căn cứ vì `max` có thể bị kéo xa bởi các giá trị ngoại lệ (outliers) bất thường (ví dụ 1 bản ghi bị kẹt mạng tới muộn hàng tuần/tháng). 
> - Chi phí nếu chọn `max`: Mọi lượt chạy thường nhật đều phải lùi window rất xa, buộc pipeline phải đọc lại và tính toán lại một lượng dữ liệu khổng lồ ở MỌI lượt chạy sau này. Điều này làm tăng chi phí tính toán (compute cost) và thời gian thực thi (latency) vô ích.
> - Chi phí nếu chọn P99: Pipeline thường nhật duy trì lookback ngắn (3 ngày), xử lý tự động và triệt để 99% dữ liệu đến muộn với chi phí tối ưu. 1% cực đoan còn lại có thể xử lý qua batch backfill định kỳ.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Backend đổi kiểu cột `priority` từ số sang chuỗi từ ngày 08-10. Khi ép kiểu đơn thuần bằng `try_cast(priority_raw as integer)`, các giá trị chuỗi hợp lệ (`urgent`, `high`, `medium`, `low`) bị chuyển thành NULL (gây thiếu dữ liệu trong Silver), còn các số sai ngoài khoảng (`0`, `5`, `-1`) lại lọt qua. Model phân loại dự đoán kém hẳn, `quarantine_tickets` có 0 hàng (thiếu 312 hàng). |
| **Nguyên nhân** | Macro `normalize_priority` chỉ dùng `try_cast` nên không chuẩn hóa nhóm nhãn chuỗi về dạng số 1..4, đồng thời không lọc bỏ các số ngoài khoảng 1..4. Model `silver_tickets` chưa loại bỏ các bản ghi không chuẩn hóa được trước khi dùng `row_number()`. `quarantine_tickets` để `where false`, và `schema.yml` để `contract: enforced: false` cùng với các validation test chưa được bật. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ** (`'1'`, `'2'`, `'3'`, `'4'`): Giữ nguyên và cast sang integer (1..4).<br>2. **Nhãn chuỗi hợp lệ** (`'urgent'`, `'high'`, `'medium'`, `'low'`): Schema evolution $\rightarrow$ Map quy đổi về số (`urgent`=1, `high`=2, `medium`=3, `low`=4).<br>3. **Giá trị lỗi** (`'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `NULL`): Dữ liệu hỏng thật $\rightarrow$ Trả về `NULL` để lọc khỏi Silver và đẩy vào Quarantine. |
| **Cách khắc phục** | - File `dbt/macros/normalize_priority.sql`: Thay `try_cast` bằng khối `CASE` xử lý đủ 3 nhóm và trả về `reject_reason` chi tiết.<br>- File `dbt/models/silver/silver_tickets.sql`: Lọc bỏ bản ghi mà `normalize_priority` trả về `NULL` trong CTE trước khi xếp hạng `row_number()` (đảm bảo loại bản ghi hỏng mà không mất ticket, giữ đủ 12,480 ticket).<br>- File `dbt/models/silver/quarantine_tickets.sql`: Sửa điều kiện thành `where {{ normalize_priority('priority_raw') }} is null`.<br>- File `dbt/models/silver/schema.yml`: Đổi `enforced: true` và mở comment khối tests (`not_null`, `accepted_values: values: [1, 2, 3, 4]`). |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (khớp expected) · `dbt test` 11/11 pass · `silver_tickets.priority ∈ 1..4, không NULL` = sạch (0 hàng sai) · `silver_tickets` = 12,480 ticket. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Trả lời:**
> 1. **Chặn ở Bronze hay Silver?** Nên chặn/phân loại ở tầng Silver. Tầng Bronze mang vai trò là lưu trữ dữ liệu thô (Raw Data / Single Source of Truth). Nếu chặn hoặc từ chối dữ liệu ngay tại tầng Bronze, hệ thống sẽ đánh mất toàn bộ vết của bản ghi bị lỗi (data loss), làm cho việc điều tra nguyên nhân sự cố hoặc khôi phục/sửa đổi sau này trở nên bất khả thi.
> 2. **Vì sao KHÔNG để pipeline dừng khi gặp bản ghi lỗi?** Trong môi trường vận hành thực tế, vài trăm bản ghi lỗi không thể và không nên có quyền chặn đứng quá trình xử lý của hàng trăm ngàn bản ghi hợp lệ khác phục vụ cho các ứng dụng downstream (RAG, Dashboard, Routing Agent). Việc đưa bản ghi lỗi vào bảng `quarantine_tickets` giúp đường ống chính tiếp tục hoạt động liên tục (resilient pipeline), đồng thời cách ly bản ghi hỏng vào một hàng đợi riêng để đội vận hành có thể kiểm tra và xử lý sau.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | không làm |
| **Nguyên nhân** | (Không thực hiện theo yêu cầu) |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra tính **Idempotent** của các incremental model (khai báo `unique_key`, `incremental_strategy`) và cấu hình của Airflow DAG (`catchup`, `max_active_runs`) để đảm bảo việc chạy lại (re-run/clear task) không tạo dữ liệu trùng lặp. |
| 2 | Kiểm tra **phân bố độ trễ dữ liệu** (`_ingested_at` so với `event_time`) bằng các chỉ số thống kê (P50, P95, P99) để xác định Lookback Window chính xác cho dữ liệu đến muộn (late-arriving data). |
| 3 | Kiểm tra **Data Contract** (`contract: enforced: true`), các bộ kiểm thử tự động (**dbt test**) và cơ chế **Quarantine** để đảm bảo dữ liệu sai format/schema evolution được xử lý và phân loại thích hợp mà không làm gián đoạn pipeline. |
