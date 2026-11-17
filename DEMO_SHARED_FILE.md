# DEMO: Shared file dùng Ricart–Agrawala (SV3)

**Một máy, 4 Terminal, 3 người lần lượt demo**. Phải thực hiện phần này **trước khi SV2 tắt Node 4**.

## Khởi động (trước khi chạy Node)

```powershell
python reset_counter.py
python reset_board.py
```

Mở 4 Terminal ở thư mục gốc:

```powershell
python main.py --id 1
python main.py --id 2
python main.py --id 3
python main.py --id 4
```

## SV3: Ghi file cộng tác

Ở **Terminal Node 1**, nhập:

```text
write Xin chao ca nhom
```

Ở **Terminal Node 2**, nhập:

```text
write Toi da xem noi dung cua Node 1
```

Ở **Terminal Node 3**, nhập:

```text
write Them mot ghi chu chung
```

Ở **Terminal Node 4**, nhập:

```text
write Xac nhan hoan thanh
```

Tại **Terminal Node 1**, sau khi cả bốn lệnh kết thúc:

```text
board
```

Dự kiến `shared_whiteboard.txt` chứa đúng 4 dòng, mỗi dòng có nhãn Node.
**Thứ tự dòng không nhất thiết là Node 1 → 4**, vì phụ thuộc thời điểm các Node được cấp khóa.

**Lời trình bày:** "Mỗi Node gửi REQUEST đến ba Node còn lại, chỉ vào Critical Section khi nhận đủ REPLY. Toàn bộ thao tác append nội dung được thực hiện bên trong Critical Section. Vì vậy các Node có thể cùng ghi vào một file mà không cần Central Lock Server. Đây là mô phỏng trên một filesystem chung, chưa phải file sao chép giữa nhiều máy."

## SV2: Failure test (thực hiện sau demo SV3)

Dừng Node 4 bằng Ctrl+C. Sau khi Node 3 được bầu Leader, ở Node 2 nhập:

```text
write Khong du quyen nen khong duoc ghi
```

Dự kiến hiển thị `Shared file write NOT performed`, file **không thêm dòng**. Bầu Leader mới không tự làm RA hỗ trợ đổi membership.

## Chạy kiểm thử tự động

**Đóng bốn Terminal Node trước**, sau đó chạy riêng từng script:

```powershell
python tests/test_system.py
python tests/test_shared_file.py
```

Các test tự reset file minh họa nên không chạy đồng thời với demo thủ công.
