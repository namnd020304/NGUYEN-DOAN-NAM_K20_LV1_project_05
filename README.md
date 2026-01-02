Được thực hiện trên VM của GCP, có các đặc điểm sau:
- Sử dụng VM làm server, nơi đặt mongo và dữ liệu source là file hành vi người dùng của Glamira, môt thương hiệu trang sức.
- Dùng sftp đưa code python và một số file quan trong như file bin từ local lên VM để truy cập vào API lấy IP địa điểm lên và chạy trên VM.
- Với code python, sử dụng async để tối ưu hóa hiệu suất crawl dữ liệu, một số url sau 1 khoảng thờ gian sẽ bị thay đổi, để ưu tiên các web dùng ngôn ngữ là tiếng anh, tôi phải:
  + Lấy product_id từ những hành vi của khách hàng có những thao tác như xem sản phẩm, đặt mua...
  + Tạo 1 hàng ưu tiên, với url có domain là .com đẻ chỏ tới trang web quốc tế
  + Nếu như url quốc tế bị lỗi (404) ta chuyển sang url dựa trên url lấy được từ file hành vi người dùng, sử dụng regex để lấy tên miền https://glamira.*/ đưa vào hàng đợi
  + Trong quá trình crawl, sẽ có những lỗi khác như 403..., những product_id này sẽ được lưu lại vào 1 file, sau đó sẽ đc crawl lại thêm 2 lần nữa.
  + Kết quả sẽ được lưu về local.
 
