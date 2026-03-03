import json
import pathlib
from dataclasses import dataclass
from datetime import date
from typing import Iterable

import pandas as pd
import requests


QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
STATIC_PDF_HOST = "https://static.cninfo.com.cn"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36"
    )
}

CATEGORY_MAP = {
    "年报": "category_ndbg_szsh",
    "半年报": "category_bndbg_szsh",
    "一季报": "category_yjdbg_szsh",
    "三季报": "category_sjdbg_szsh",
    "业绩预告": "category_yjygjxz_szsh",
    "权益分派": "category_qyfpxzcs_szsh",
    "董事会": "category_dshgg_szsh",
    "监事会": "category_jshgg_szsh",
    "股东大会": "category_gddh_szsh",
    "日常经营": "category_rcjy_szsh",
    "公司治理": "category_gszl_szsh",
    "中介报告": "category_zj_szsh",
    "首发": "category_sf_szsh",
    "增发": "category_zf_szsh",
    "股权激励": "category_gqjl_szsh",
    "配股": "category_pg_szsh",
    "解禁": "category_jj_szsh",
    "公司债": "category_gszq_szsh",
    "可转债": "category_kzzq_szsh",
    "其他融资": "category_qtrz_szsh",
    "股权变动": "category_gqbd_szsh",
    "补充更正": "category_bcgz_szsh",
    "澄清致歉": "category_cqdq_szsh",
    "风险提示": "category_fxts_szsh",
    "特别处理和退市": "category_tbclts_szsh",
    "退市整理期": "category_tszlq_szsh",
}

CSV_COLUMNS = [
    "secCode",
    "secName",
    "announcementTitle",
    "announcementId",
    "announcementTime",
    "adjunctUrl",
]


@dataclass(frozen=True)
class CrawlConfig:
    category_cn: str = "年报"
    start_date: str = "2000-01-01"
    end_date: str | None = None
    page_size: int = 30
    column: str = "szse"


def _today_yyyy_mm_dd() -> str:
    return date.today().strftime("%Y-%m-%d")


def load_stock_orgid_map(stock_info_path: str | pathlib.Path = "stock_info.json") -> dict[str, str]:
    stock_info_path = pathlib.Path(stock_info_path)
    stock_info = None
    # 尝试读取本地 json 文件
    if stock_info_path.exists():
        try:
            with stock_info_path.open("r", encoding="utf-8") as f:
                stock_info = json.load(f)
        except Exception:
            stock_info = None
    # 若文件不存在或内容无效，则自动下载
    if not stock_info or "stockList" not in stock_info:
        print("stock_info.json 不存在或无效，正在自动下载...")
        import requests
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36'
        }
        info_url =  'http://www.cninfo.com.cn/new/data/szse_stock.json'
        response = requests.get(info_url, headers=headers)
        with stock_info_path.open('w', encoding='utf-8') as f:
            f.write(response.text)
        stock_info = response.json()
        print('公司信息更新成功！')

    mapping: dict[str, str] = {}
    for item in stock_info.get("stockList", []):
        code = str(item.get("code", "")).strip()
        org_id = str(item.get("orgId", "")).strip()
        if code and org_id:
            mapping[code] = org_id
    return mapping


def build_full_stock_code(stock_code: str, orgid_map: dict[str, str]) -> str:
    stock_code = str(stock_code).strip()
    org_id = orgid_map.get(stock_code)
    if not org_id:
        raise KeyError(f"stock_code={stock_code} not found in stock_info.json")
    return f"{stock_code},{org_id}"


def iter_announcements_for_stock(
    full_stock_code: str,
    *,
    config: CrawlConfig,
) -> Iterable[dict]:
    category = CATEGORY_MAP.get(config.category_cn, "") if config.category_cn else ""
    end_date = config.end_date or _today_yyyy_mm_dd()
    se_date = f"{config.start_date}~{end_date}"

    has_more = True
    page_num = 1

    while has_more:
        form_data = {
            "pageNum": page_num,
            "pageSize": config.page_size,
            "column": config.column,
            "tabName": "fulltext",
            "isHLtitle": True,
            "stock": full_stock_code,
            "category": category,
            "seDate": se_date,
        }

        resp = requests.post(QUERY_URL, headers=HEADERS, data=form_data, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        resp.close()

        has_more = bool(payload.get("hasMore"))
        page_num += 1

        for announcement in payload.get("announcements", []):
            yield announcement


def append_announcements_to_csv(
    announcements: Iterable[dict],
    *,
    csv_path: str | pathlib.Path,
) -> int:
    csv_path = pathlib.Path(csv_path)
    is_new_file = not csv_path.exists()

    rows = []
    for a in announcements:
        rows.append({k: a.get(k, "") for k in CSV_COLUMNS})

    if not rows:
        return 0

    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    df.to_csv(
        csv_path,
        mode="w" if is_new_file else "a",
        index=False,
        header=is_new_file,
        encoding="utf-8",
    )
    return len(df)


def download_pdfs_from_csv(
    csv_path: str | pathlib.Path = "pdf_to_download.csv",
    *,
    pdf_dir: str | pathlib.Path = "pdf",
) -> int:
    csv_path = pathlib.Path(csv_path)
    pdf_dir = pathlib.Path(pdf_dir)
    pdf_dir.mkdir(exist_ok=True)

    df = pd.read_csv(csv_path, dtype={"secCode": object})

    if "adjunctUrl" not in df.columns:
        raise ValueError(
            "pdf_to_download.csv 缺少 adjunctUrl 列：请用新版抓取脚本重新生成 CSV（需要从公告 json 中写入 adjunctUrl）"
        )

    # announcementTime may be 'YYYY-MM-DD' or a Unix timestamp in milliseconds
    s = df["announcementTime"]
    dt = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    s_num = pd.to_numeric(s, errors="coerce")
    dt_ms = pd.to_datetime(s_num, unit="ms", errors="coerce")
    dt = dt.fillna(dt_ms).fillna(pd.to_datetime(s, errors="coerce"))
    df["announcementTime"] = dt.dt.date

    downloaded = 0
    for i in range(len(df)):
        code_name = df["secCode"][i]
        firm_name = df["secName"][i]

        code_dir = pdf_dir.joinpath(str(code_name))
        code_dir.mkdir(exist_ok=True)

        pdf_name = f"{firm_name}：{df['announcementTitle'][i]}"
        print(f"正在下载 -- {pdf_name}")

        adjunct_url = str(df["adjunctUrl"][i]).strip()
        if adjunct_url == "" or adjunct_url.lower() == "nan":
            raise ValueError(f"第 {i} 行缺少 adjunctUrl，无法下载：{code_name} / {pdf_name}")

        pdf_url = STATIC_PDF_HOST.rstrip("/") + "/" + adjunct_url.lstrip("/")
        resp = requests.get(pdf_url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        pdf_path = code_dir.joinpath(pdf_name + ".pdf")
        with pdf_path.open("wb") as f:
            f.write(resp.content)

        downloaded += 1

    print("\n全部文件下载完毕！")
    return downloaded


def crawl_and_download(
    stock_codes: Iterable[str],
    *,
    config: CrawlConfig | None = None,
    csv_path: str | pathlib.Path = "pdf_to_download.csv",
    pdf_dir: str | pathlib.Path = "pdf",
) -> None:
    if config is None:
        config = CrawlConfig()

    orgid_map = load_stock_orgid_map()

    csv_path = pathlib.Path(csv_path)
    if csv_path.exists():
        csv_path.unlink()

    total_rows = 0
    failed: list[str] = []

    for stock_code in stock_codes:
        stock_code = str(stock_code).strip()
        if not stock_code:
            continue

        try:
            full_code = build_full_stock_code(stock_code, orgid_map)
            announcements = iter_announcements_for_stock(full_code, config=config)
            rows = append_announcements_to_csv(announcements, csv_path=csv_path)
            total_rows += rows
            print(f"{stock_code} 解析完成，新增 {rows} 条")
        except Exception as e:  # noqa: BLE001
            failed.append(stock_code)
            print(f"{stock_code} 失败：{e}")

    print(f"\n汇总完成：共写入 {total_rows} 条到 {csv_path}")
    if failed:
        print("失败股票代码：" + ", ".join(failed))

    if total_rows > 0:
        download_pdfs_from_csv(csv_path, pdf_dir=pdf_dir)


if __name__ == "__main__":
    # 你只需要改这里：传入股票代码 list（字符串即可，保留前导 0）
    stock_codes = {
        "600884",
        "835185",
        "603659",
        "300077",
        "603659",
        "300035",
        "001301",
    }

    crawl_and_download(stock_codes)
