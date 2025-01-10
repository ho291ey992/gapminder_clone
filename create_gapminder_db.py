import pandas as pd
import sqlite3 # 內建資料庫

class CreateGapminderDB:
    def __init__(self):
        self.file_name = ["ddf--datapoints--gdp_pcap--by--country--time",
                          "ddf--datapoints--lex--by--country--time",
                          "ddf--datapoints--pop--by--country--time",
                          "ddf--entities--geo--country"]
        self.table_name = ["gdp_per_capita", "life_expectancy", "population", "geography"]

    # 讀取csv資料
    def data_to_dict(self):
        df_dict = dict()
        for file_name, table_name in zip(self.file_name, self.table_name):
            file_path = f"練習專案一：兩百個國家、兩百年、四分鐘/Data/{file_name}.csv"
            df = pd.read_csv(file_path)
            df_dict[table_name] = df
        return df_dict
    
    # 建立資料庫
    def create_database(self):
        self.connection = sqlite3.connect("練習專案一：兩百個國家、兩百年、四分鐘/Data/gapminder.db") # sqlite3.connect:連線資料庫、創建資料庫
        df_dict = self.data_to_dict()
        for k, v in df_dict.items():
            v.to_sql(name=k, con=self.connection, index=False, if_exists="replace")
        self.create_view()

    # 串聯資料表 & 建立虛擬資料表
    def create_view(self):
        drop_view_sql = "DROP VIEW IF EXISTS plotting;          -- 檢查是否有plotting資料表"         
        create_view_sql = """
                          CREATE VIEW plotting AS               -- 建立虛擬資料表
                          SELECT g.name AS country_name,        -- 國家名稱
                                 g.world_4region AS continent,  -- 洲別
                                 gpc.time as dt_year,           -- 年
                                 gpc.gdp_pcap AS gdp_per_capita,-- 平均GDP
                                 le.lex AS life_expectancy,     -- 平均壽命
                                 p.pop AS population            -- 人口數
                           FROM gdp_per_capita gpc 
                           JOIN geography g 
                             ON g.country = gpc.country 
                           JOIN life_expectancy le 
                             ON le.country = gpc.country AND 
                                le.time  = gpc.time 
                           JOIN population p 
                             ON p.country = gpc.country AND 
                                p.time = gpc.time 
                           WHERE gpc.time < 2024;
                          """
        # connection.execute():對資料庫執行 SQL 指令
        self.connection.execute(drop_view_sql)
        self.connection.execute(create_view_sql)
        self.connection.close()

create_gapminder_db = CreateGapminderDB()
create_gapminder_db.create_database()


