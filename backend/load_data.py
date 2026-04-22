import sqlite3
import pandas as pd
import time
import os

def init_real_db():
    db_dir = "data"
    db_file = os.path.join(db_dir, "drivee.db")
    csv_file = "train.csv"
    
    if not os.path.exists(csv_file):
        return

    os.makedirs(db_dir, exist_ok=True)
    
    conn = sqlite3.connect(db_file)
    conn.execute("DROP TABLE IF EXISTS orders")
  
    chunksize = 100000
    for i, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunksize)):
        chunk.to_sql("orders", conn, if_exists="append", index=False)
        
    conn.execute("CREATE INDEX idx_order_id ON orders(order_id);")
    conn.execute("CREATE INDEX idx_status ON orders(status_order, status_tender);")
    conn.execute("CREATE INDEX idx_time ON orders(order_timestamp);")
    
    conn.commit()
    conn.close()
    

if __name__ == "__main__":
    init_real_db()
