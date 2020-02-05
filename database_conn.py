import sqlite3
class DataBase():
      def __init__(self,db_file):
            self.db_file = db_file
            self.conn = self.create_connection()
      def create_connection(self):
            self.conn = None
            try:
                self.conn = sqlite3.connect(self.db_file)
                return self.conn
            except Error as e:
                print(e)
            return self.conn

      def run_string(self,sql_string):
           try:
               c = self.conn.cursor()
               c.execute(sql_string)
               result = c.fetchall()
               c.close()
               return result
           except Error as e:
               print(e)

      def insert_row(self,table_name,columns,row):
            sql_string = f''' INSERT INTO {table_name}{tuple([i for i in columns])}
                      VALUES{tuple(["?" for i in range(len(columns))])}'''
            cur = self.conn.cursor()
            cur.execute(sql_string,row)
            return cur.lastrowid
