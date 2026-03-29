# GTA 楼市底部信号追踪器

**Oakville & Mississauga 每周市场指标追踪 + 底部信号仪表盘**

[![Weekly Market Data Scrape](https://github.com/your-username/gta-market-tracker/actions/workflows/weekly-scrape.yml/badge.svg)](https://github.com/your-username/gta-market-tracker/actions/workflows/weekly-scrape.yml)

🔗 **Live Dashboard → https://your-username.github.io/gta-market-tracker/**

---

## 追踪的核心指标

| 指标 | 含义 | 触底信号阈值 |
|------|------|------|
| SNLR 销新比 | 成交量 / 新挂牌量 | 跌破 40% 后回升 |
| MOI 库存月数 | 现有库存 / 月均成交 | 从 5+ 月回落至 3 月以下 |
| DOM 在市天数 | 平均挂牌到成交天数 | 从 30+ 天收窄至 20 天以下 |
| 价格月环比 | 本月均价 vs 上月 | 连续 2 个月正增长 |
| BoC 利率 | 加拿大央行政策利率 | 首次降息后信心回升 |

---

## 项目结构

```
gta-market-tracker/
├── .github/
│   └── workflows/
│       └── weekly-scrape.yml   # 每周日自动运行爬虫
├── data/
│   └── market_data.json        # 历史数据（每周自动更新）
├── scraper/
│   ├── scrape.py               # 爬虫主程序
│   └── requirements.txt        # Python 依赖
├── index.html                  # 仪表盘（GitHub Pages 托管）
└── README.md
```

---

## 快速部署

### 第一步：Fork 本仓库

点击右上角 **Fork**，将仓库复制到你的 GitHub 账号。

### 第二步：开启 GitHub Pages

1. 进入 **Settings → Pages**
2. Source 选择 **Deploy from a branch**
3. Branch 选择 `main`，目录选择 `/ (root)`
4. 保存后约 1 分钟即可访问 `https://your-username.github.io/gta-market-tracker/`

### 第三步：验证 GitHub Actions

1. 进入 **Actions** 标签页
2. 找到 `Weekly Market Data Scrape` workflow
3. 点击 **Run workflow** 手动触发一次，确认爬虫正常运行

> 每周日 09:00 UTC（东部时间 05:00）自动运行。

---

## 本地运行爬虫

```bash
# 安装依赖
pip install -r scraper/requirements.txt

# 运行爬虫
python scraper/scrape.py

# 在本地预览仪表盘（需要 HTTP server，不能直接用 file://）
python -m http.server 8080
# 访问 http://localhost:8080
```

---

## 数据来源

| 数据类型 | 来源 | 频率 |
|---------|------|------|
| BoC 政策利率 | [Bank of Canada Valet API](https://www.bankofcanada.ca/valet/docs) | 实时 |
| 5 年期国债收益率 | Bank of Canada Valet API | 实时 |
| 加拿大失业率 | [Statistics Canada LFS](https://www150.statcan.gc.ca/n1/pub/71-607-x/2018014/lfs-ena.htm) | 每月 |
| Oakville / Mississauga 房价 | Wahi.com / Zoocasa.com | 每月 |
| SNLR / MOI / DOM | 根据成交量 + 挂牌量计算 | 每月 |

> **注意**：TRREB 的完整市场报告需要会员资格。本项目使用公开来源数据。
> 如果你有 TRREB 账号，可以在 `scraper/scrape.py` 中添加对应接口。

---

## 手动更新数据

如果爬虫无法获取某个月的数据（网站结构变化等），可以手动编辑 `data/market_data.json`，按照现有格式添加一条记录：

```json
{
  "month": "2026-04",
  "boc_rate": 2.25,
  "unemployment": 6.7,
  "oakville": {
    "avg_price": 1275000,
    "new_listings": 370,
    "sales": 128,
    "active_listings": 690,
    "dom": 27,
    "snlr": 0.346,
    "moi": 5.4
  },
  "mississauga": {
    "avg_price": 816000,
    "new_listings": 814,
    "sales": 243,
    "active_listings": 1449,
    "dom": 29,
    "snlr": 0.299,
    "moi": 6.0
  }
}
```

---

## 信号解读

**五灯全绿 = 强底部确认**：
- ✅ SNLR ≥ 50%（需求明显改善）
- ✅ MOI ≤ 3.0 月（供需重新平衡）
- ✅ DOM ≤ 22 天（买家信心回归）
- ✅ 价格月环比连续 +（价格企稳）
- ✅ BoC 利率 < 2.0%（政策催化到位）

---

## License

MIT — 自由使用、修改、分发。
