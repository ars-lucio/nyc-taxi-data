import unittest
from database_conn import DataBase
import sqlite3


class TestDB(unittest.TestCase):
    def test_same_tables(self):
        db_name = './taxi.db'
        db = DataBase(db_name)
        tables = db.run_string("SELECT name FROM sqlite_master WHERE type='table';")
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables2 = c.fetchall()
        c.close()
        self.assertEqual(tables, tables2, "Should have same tables")






if __name__ == '__main__':
    unittest.main()
