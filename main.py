import re
import ast
import asyncio
import aiohttp
import csv
import json
from typing import Dict, List
from bs4 import BeautifulSoup
import nest_asyncio


def parse_domain(url):
    domain = re.findall(r'https://www\.glamira\.[^/]+/', url)
    return domain


def get_url(data):
    parent = "catalog/product/view/id/"
    all_urls = {}

    for product in data:
        glob_dom = ["https://www.glamira.com/"]
        private_domain = parse_domain(product['current_url'])
        glob_dom.extend(private_domain)

        result = list(map(lambda x: x + parent + product['product_id'], glob_dom))
        all_urls[product['product_id']] = result

    return all_urls


async def crawl_product_data(session, url, product_id, field):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status != 200:
                return None

            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            # Tìm div column main
            column_main = soup.find('div', class_='column main')
            if not column_main:
                print(f"[{product_id}] Không tìm thấy 'column main'")
                return None

            # Tìm tất cả script type='text/javascript'
            scripts = column_main.find_all('script', type='text/javascript')
            if len(scripts) < 6:
                print(f"[{product_id}] Không đủ script tags (có {len(scripts)})")
                return None

            # Lấy script thứ 6 (index 5)
            script_text = scripts[5].text.strip()

            # Parse JSON
            json_text = re.sub(r'\s*?var\sreact_data\s=\s', '', script_text)
            if json_text.endswith(';'):
                json_text = json_text[:-1]

            data = json.loads(json_text)

            # Lấy các field cần thiết
            p_data = {'crawled_url': url}
            for f in field:
                p_data[f] = data.get(f, None)

            print(f"[{product_id}] ✓ Crawl thành công dữ liệu")
            return p_data

    except asyncio.TimeoutError:
        print(f"[{product_id}] TIMEOUT khi crawl data")
        return None
    except json.JSONDecodeError as e:
        print(f"[{product_id}] Lỗi parse JSON: {e}")
        return None
    except Exception as e:
        print(f"[{product_id}] Lỗi crawl data: {type(e).__name__} - {e}")
        return None


async def check_url(session, url, product_id, semaphore):
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10), allow_redirects=True) as response:
                status_code = response.status
                result = {
                    'product_id': product_id,
                    'url': url,
                    'status_code': status_code
                }

                print(f"[{product_id}] {status_code} | {url}")
                return result

        except asyncio.TimeoutError:
            print(f"[{product_id}] TIMEOUT | {url}")
            return {
                'product_id': product_id,
                'url': url,
                'status_code': 'TIMEOUT'
            }
        except Exception as e:
            print(f"[{product_id}] ERROR | {url} | {type(e).__name__}")
            return {
                'product_id': product_id,
                'url': url,
                'status_code': f'ERROR: {type(e).__name__}'
            }


async def crawl_product(session, product_id, urls, semaphore, field):
    results = []
    working_url = None

    for url in urls:
        result = await check_url(session, url, product_id, semaphore)
        results.append(result)

        # Nếu tìm thấy 200, lưu URL và dừng check
        if result['status_code'] == 200:
            working_url = url
            remaining = len(urls) - urls.index(url) - 1
            if remaining > 0:
                print(f"[{product_id}] Tìm thấy 200! Bỏ qua {remaining} URL còn lại")
            break

    if not working_url:
        print(f"[{product_id}] Không có URL nào trả về 200")
        return results, False, None

    product_data = await crawl_product_data(session, working_url, product_id, field)

    return results, True, product_data


async def crawl_all_products(url_dict: Dict[str, List[str]], field: List[str], max_concurrent: int = 10):
    semaphore = asyncio.Semaphore(max_concurrent)
    all_results = []
    failed_products = []
    product_data_list = []

    async with aiohttp.ClientSession() as session:
        tasks = []

        for product_id, urls in url_dict.items():
            task = crawl_product(session, product_id, urls, semaphore, field)
            tasks.append(task)

        # Chạy tất cả products đồng thời
        print(f"\n{'=' * 60}")
        print(f"Bắt đầu crawl {len(tasks)} products (max {max_concurrent} concurrent)")
        print(f"{'=' * 60}\n")

        results = await asyncio.gather(*tasks)

        for (product_results, success, product_data), product_id in zip(results, url_dict.keys()):
            all_results.extend(product_results)
            if not success:
                failed_products.append(product_id)
            elif product_data:
                product_data_list.append(product_data)

    return all_results, failed_products, product_data_list


def save_failed_to_csv(failed_products, results, filename='failed_products.csv'):
    if not failed_products:
        print("\n Tất cả products đều có ít nhất 1 URL trả về 200")
        return

    failed_data = [r for r in results if r['product_id'] in failed_products]

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['product_id', 'url', 'status_code'])
        writer.writeheader()
        writer.writerows(failed_data)

    print(f"\n Đã lưu products không có URL 200 vào: {filename}")


def save_product_data_to_csv(product_data_list, field, filename='product_data.csv'):
    if not product_data_list:
        print("\n Không có dữ liệu product nào để lưu")
        return

    fieldnames = ['crawled_url'] + field

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(product_data_list)

    print(f"\n Đã lưu {len(product_data_list)} products vào: {filename}")


async def main():
    field = ['product_id', 'name', 'sku', 'attribute_set_id', 'attribute_set',
             'type_id', 'price', 'min_price', 'max_price', 'min_price_format',
             'max_price_format', 'gold_weight', 'none_metal_weight', 'fixed_silver_weight',
             'material_design', 'qty', 'collection', 'collection_id', 'product_type',
             'product_type_value', 'category', 'category_name', 'store_code',
             'platinum_palladium_info_in_alloy', 'bracelet_without_chain', 'show_popup_quantity_eternity',
             'visible_contents', 'gender']

    data = []
    with open('product_id.txt', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                obj = ast.literal_eval(line)
                data.append(obj)

    print(f"Đã load {len(data)} products từ file")

    url_dict = get_url(data)

    total_urls = sum(len(urls) for urls in url_dict.values())
    print(f"Tổng số URLs tối đa: {total_urls}")

    results, failed_products, product_data_list = await crawl_all_products(
        url_dict,
        field,
        max_concurrent=20
    )

    save_failed_to_csv(failed_products, results, 'failed_products.csv')
    save_product_data_to_csv(product_data_list, field, 'product_data.csv')

    print(f"\n{'=' * 60}")
    print(f"TỔNG KẾT:")
    print(f"{'=' * 60}")
    print(f"Tổng số products: {len(url_dict)}")
    print(f"Tổng số requests check URL: {len(results)}")
    print(f"Số products không có URL 200: {len(failed_products)}")
    print(f"Số products crawl thành công: {len(product_data_list)}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(main())