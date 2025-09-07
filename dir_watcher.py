# 특정경로의 하위의 파일과 하위 디렉토리 내부의 파일까지 모두 감시한다.
# 새로 생성된 파일과 행수가 늘어난 파일을 각각 관리한다.
# 새로 생성된 파일 : 8행이 Time 문자열로 되어 있고, 9행은 숫자로 시작되고, tab으로 분리된 4x열로 구성되어 있는지 확인하여 9행부터 line_insert작업
# 행수가 늘어난 파일 : tab으로 분리된 4x열로 구성되어 있는지 확인하여, 늘어난 행 모두 line_insert작업
# insert 작업은 insert_to_retool.py 파일을 이용한다.
# 현재 디렉토리 경로는 C:\dev\ev\watch_insert\test_data 이다.
# insert하기전에 line_counter.py 파일을 이용하여 현재 파일의 최대 time을 구한다.
# hostname, dir, filename으로 최대 time을 구한다. line_counter.py의 get_max_time함수를 이용
# hostname은 get_hostname함수를 이용
# dir은 각 파일의 파일명을 제외한 path이다.
# filename은 각 파일의 파일명이다.
# 감시한 파일의 9행 부터는 데이터인데, 데이터행의 첫열이 time데이터인데 , 가져온 max보다 큰 경우에만 insert작업

import os
import time
import psycopg2
from psycopg2 import sql
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# DB 접속 정보
conn_str = ""

def get_hostname():
    with open(os.path.join(os.path.dirname(__file__), 'hostname.txt'), 'r', encoding='euc-kr') as f:
        return f.read().strip()


class ProtocolFileHandler(FileSystemEventHandler):
    def __init__(self):
        self.file_line_counts = {}
        self.hostname = get_hostname()
        self.conn = psycopg2.connect(conn_str)
    
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def get_max_time(self, dir_path, filename):
        with self.conn.cursor() as cur:
            cur.execute("SELECT MAX(time) FROM tb_proto_data WHERE hostname = %s AND dir = %s AND filename = %s", 
                       (self.hostname, dir_path, filename))
            result = cur.fetchone()
            return result[0] if result and result[0] is not None else -1
    
    def insert_proto_data_lines(self, data_lines, filename, dir_path):
        if isinstance(data_lines, str):
            lines = data_lines.strip().split('\n')
        else:
            lines = data_lines

        rows = []
        max_c = 0
        for line in lines:
            fields = line.strip().split('\t')
            time = float(fields[0])
            idx = int(fields[1])
            item = fields[2]
            c_values = list(fields[3:])
            rows.append([self.hostname, dir_path, filename, time, idx, item] + c_values)
            if len(c_values) > max_c:
                max_c = len(c_values)

        columns = ['hostname', 'dir', 'filename', 'time', 'idx', 'item']
        for i in range(max_c):
            columns.append('c' + str(i + 1))

        for row in rows:
            while len(row) < len(columns):
                row.append(None)

        insert_sql = sql.SQL("INSERT INTO tb_proto_data ({fields}) VALUES {values}").format(
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            values=sql.SQL(', ').join(
                sql.SQL('({})').format(sql.SQL(', ').join(sql.Placeholder() * len(columns)))
                for _ in rows
            )
        )

        flat_values = []
        for row in rows:
            flat_values.extend(row)

        with self.conn.cursor() as cur:
            cur.execute(insert_sql, flat_values)
        self.conn.commit()
    
    def get_data_lines(self, file_path, start_line=8):
        with open(file_path, 'r', encoding='euc-kr') as f:
            lines = f.readlines()
        return [line.strip() for line in lines[start_line:] if line.strip() and line.split('\t')[0].replace('.', '').isdigit()]
    
    def process_file(self, file_path, is_new_file=False):
        
        dir_path, filename = os.path.dirname(file_path), os.path.basename(file_path)
        
        if is_new_file:
            data_lines = self.get_data_lines(file_path)
        else:
            current_count = len(open(file_path, 'r', encoding='euc-kr').readlines())
            prev_count = self.file_line_counts.get(file_path, 0)
            if current_count <= prev_count:
                return
            data_lines = self.get_data_lines(file_path, prev_count)
            self.file_line_counts[file_path] = current_count
        
        if data_lines:
            max_time = self.get_max_time(dir_path, filename)
            if max_time == -1 or float(data_lines[0].split('\t')[0]) > max_time:
                self.insert_proto_data_lines(data_lines, filename, dir_path)
                print(f"Inserted {len(data_lines)} lines for {filename}")
    
    def on_created(self, event):
        if not event.is_directory:
            time.sleep(0.1)
            self.process_file(event.src_path, True)
    
    def on_modified(self, event):
        if not event.is_directory:
            time.sleep(0.1)
            self.process_file(event.src_path, False)

def start_watching():
    watch_directory = r"C:\Users\home\Desktop\Protocol Data"
    print(f"Watching: {watch_directory}")
    
    handler = ProtocolFileHandler()
    observer = Observer()
    observer.schedule(handler, watch_directory, recursive=True)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        handler.conn.close()
        print("Stopped")

if __name__ == "__main__":
    start_watching()
