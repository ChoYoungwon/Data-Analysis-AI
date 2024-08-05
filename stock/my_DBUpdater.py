import pymysql
import pandas as pd
from datetime import datetime
from datetime import timedelta
import requests, calendar, time, json, re
from bs4 import BeautifulSoup
from threading import Timer

class DBUpdater:
    def __init__(self):
        """생성자 : MariaDB 연결 및 종목코드 딕셔너리 생성"""
        # self.conn = pymysql.connect(host='', port=, db='',
        #                              user='', passwd='', charset='')
        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS company_inform (
                code VARCHAR(20),
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (code))
            """
            curs.execute(sql)
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price (
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date))   
            """
            curs.execute(sql)
        self.conn.commit()
        self.codes = dict()

    def __del__(self):
        self.conn.close()

    def read_krx_code(self):
        """KRX로부터 상장기업 목록 파일을 읽어와서 데이터프레임으로 반환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx = pd.read_html(url, header=0, encoding='euc-kr')[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드': 'code', '회사명': 'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx

    def update_comp_info(self):
        """종목코드를 company_info 테이블에 업데이트한 후 딕셔너리에 저장"""
        sql = "SELECT * FROM company_inform"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]
        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) From company_inform"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_inform (code, company, last"\
                          f"_update) VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] {idx:04d} REPLACE INTO company_inform "\
                          f"VALUES ('{code}', '{company}', '{today})")
                self.conn.commit()
                print('')

    def read_naver(self, code, company, pages_to_fetch):
        try:
            url = f"https://finance.naver.com/item/sise_day.naver?code={code}"
            html = requests.get(url, headers={'User-agent': 'Mozilla/5.0'}).text
            bs = BeautifulSoup(html, "lxml")
            pgrr = bs.find("td", class_="pgRR")
            if pgrr is None:
                return None
            s = str(pgrr.a["href"]).split("=")
            lastpage = s[-1]
            df = pd.DataFrame()
            if not pages_to_fetch:
                pages = int(lastpage)
            else:
                pages = min(int(lastpage), pages_to_fetch)
            for page in range(1, pages + 1):
                u = '{}&page={}'.format(url, page)
                req = requests.get(u, headers={'User-agent': 'Mozilla/5.0'})
                df = pd.concat([df, pd.read_html(req.text, header=0)[0]], axis=0)
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow, company, code, page, pages))
            df = df.rename(columns={'날짜': 'date', '종가': 'close', '전일비': 'diff', '시가': 'open',
                                    '고가': 'high', '저가': 'low', '거래량': 'volume'})
            df['date'] = df['date'].replace('.', '-')
            df = df.dropna()
            df[['close', 'open', 'high', 'low', 'volume']] = df[['close', 'open',
                                                                         'high', 'low', 'volume']].astype(int)
            df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
        except Exception as e:
            print('EXception occured :', str(e))
            return None
        return df

    def replace_into_db(self, df, code, company):
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES ('{code}', "\
                      f"'{r.date}', {r.open}, {r.high}, {r.low}, {r.close}, "\
                      f"{r.volume})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] {} ({}) : {} rows > REPLACE INTO daily_'\
                  'price [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), company, code, len(df)))

    def update_price(self, code, pages_to_fetch):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                return
            self.replace_into_db(df, code, self.codes[code])

    def set_stock(self, code, pages_to_fetch=None):
        self.update_comp_info()
        self.update_price(code, pages_to_fetch)


class MarketDB:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        # self.conn = pymysql.connect(host='', user='', password='',
        #                             db='', charset='')
        self.codes = {}
        self.get_comp_info()

    def __del__(self):
        self.conn.close()

    def get_comp_info(self):
        sql = "SELECT * FROM company_inform"
        krx = pd.read_sql(sql, self.conn)
        for idx in range(len(krx)):
            self.codes[krx['code'].values[idx]] = krx['company'].values[idx]

    def get_daily_price(self, code, start_date=None, end_date=None):
        """KRX 종목의 일별 시세를 데이터프레임 형태로 반환
            - code          : KRX 종목코드('005930') 또는 상장기업명('삼성전자')
            - start_date    : 조회 시작일('2020-01-01'), 미입력 시 1년 전 오늘
            - end_date      : 조회 종료일('2020-12-31'), 미입력 시 오늘 날짜
        """
        if not start_date:
            one_year_ago = datetime.today() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y-%m-%d')
            print("start_date is initialized to '{}'".format(start_date))
        else:
            start_lst = re.split('\D+', start_date)
            if start_lst[0] == '':
                start_lst = start_lst[1:]
            start_year = int(start_lst[0])
            start_month = int(start_lst[1])
            start_day = int(start_lst[2])
            if start_year < 1990 or start_year > 2200:
                print(f"ValueError: start_year({start_year:d}) is wrong.")
                return
            if start_month < 1 or start_month > 12:
                print(f"ValueError: start_month({start_month:d}) is wrong.")
                return
            if start_day < 1 or start_day > 31:
                print(f"ValueError: start_day({start_day:d}) is wrong.")
                return
            start_date = f"{start_year:04d}-{start_month:02d}-{start_day:02d}"
        if not end_date:
            end_date = datetime.today().strftime('%Y-%m-%d')
            print("end_date is initialized to '{}'".format(end_date))
        else:
            end_lst = re.split('\D+', end_date)
            if end_lst[0] == '':
                end_lst = end_lst[1:]
            end_year = int(end_lst[0])
            end_month = int(end_lst[1])
            end_day = int(end_lst[2])
            if end_year < 1990 or end_year > 2200:
                print(f"ValueError: end_year({end_year:d}) is wrong.")
                return
            if end_month < 1 or end_month > 12:
                print(f"ValueError: end_month({end_month:d}) is wrong.")
                return
            if end_day < 1 or end_day > 31:
                print(f"ValueError: end_day({end_day:d}) is wrong.")
                return
            end_date = f"{end_year:04d}-{end_month:02d}-{end_day:02d}"

        codes_keys = list(self.codes.keys())
        codes_values = list(self.codes.values())
        if code in codes_keys:
            pass
        elif code in codes_values:
            idx = codes_values.index(code)
            code = codes_keys[idx]
        else:
            print("ValueError: Code({}) doesn't exist.".format(code))

        sql = f"SELECT * FROM daily_price WHERE code = '{code}'"\
              f" and date >= '{start_date}' and date <= '{end_date}'"

        df = pd.read_sql(sql, self.conn)
        df.index = df['date']
        return df

if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.update_comp_info()
