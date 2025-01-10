# 練習專案一：兩百個國家、兩百年、四分鐘

## 簡介
這個專案「兩百個國家、兩百年、四分鐘」復刻了名聞遐邇的 [Hans Rosling's 200 Countries, 200 Years, 4 Minutes](https://www.youtube.com/watch?v=jbkSRLYSojo) 資料視覺化，我們使用了 `pandas` 與 `sqlite3` 建立了資料庫，利用 `matplotlib` 進行概念驗證，最後以 `plotly.express` 做出成品。

## 如何重現
- 安裝 [Miniconda](https://docs.anaconda.com/miniconda/)
- 依據 `environment.yml` 建立環境：

```bash
conda env create -f environment.yml
```

- 將 `data/` 資料夾中的四個 CSV 檔案置放於工作目錄中的 `data/` 資料夾。
- 啟動環境並執行 `python create_gapminder_db.py` 就能-在 `data/` 資料夾中建立 `gapminder.db`
- 啟動環境並執行 `python plot_with_px.py` 就能生成 `gapminder_clone.html`