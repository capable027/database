'''import pyodbc


class SQLServerDB:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password

        # 建立数据库连接
        try:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
                       f"SERVER={self.server};" \
                       f"DATABASE={self.database};" \
                       f"UID={self.username};" \
                       f"PWD={self.password};"
            self.conn = pyodbc.connect(conn_str)
            print("已连接数据库")
            self.cursor = self.conn.cursor()
        except pyodbc.Error as e:
            print(f"数据库连接失败：{str(e)}")
            raise  # 抛出异常，终止实例化

    def create_table(self):
        """创建电影表"""
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Movies')
        CREATE TABLE Movies (
            Id INT PRIMARY KEY IDENTITY(1,1),
            Rank INT,
            Title NVARCHAR(255),
            Url NVARCHAR(500),
            Director NVARCHAR(255),
            Actors NVARCHAR(500),
            Year NVARCHAR(50),
            Country NVARCHAR(255),
            Genre NVARCHAR(255),
            Rating FLOAT,
            Votes INT,
            Summary NVARCHAR(MAX)
        )
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def insert_data(self, data_list):
        """插入数据"""
        insert_sql = """
        INSERT INTO Movies (
            Rank, Title, Url, Director, Actors, Year, Country, Genre, Rating, Votes, Summary
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
        for data in data_list:
            params = (
                data["序号"],
                data["标题"],
                data["链接"],
                data["导演"],
                data["主演"],
                data["年份"],
                data["国家"],
                data["类型"],
                float(data["评分"]),
                int(data["评价人数"].replace("人评价", "")),
                data["简介"]
            )
            self.cursor.execute(insert_sql, params)
        self.conn.commit()

    def close_connection(self):
        """关闭连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()'''
import pyodbc

class SQLServerDB:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password

        # 建立数据库连接
        try:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
                       f"SERVER={self.server};" \
                       f"DATABASE={self.database};" \
                       f"UID={self.username};" \
                       f"PWD={self.password};"
            self.conn = pyodbc.connect(conn_str)
            print("已连接数据库")
            self.cursor = self.conn.cursor()
        except pyodbc.Error as e:
            print(f"数据库连接失败：{str(e)}")
            raise  # 抛出异常，终止实例化

    def create_table(self):
        """创建电影表"""
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Movies')
        CREATE TABLE Movies (
            Id INT PRIMARY KEY IDENTITY(1,1),
            Rank INT,
            Title NVARCHAR(255),
            Url NVARCHAR(500),
            Director NVARCHAR(255),
            Actors NVARCHAR(500),
            Year NVARCHAR(50),
            Country NVARCHAR(255),
            Genre NVARCHAR(255),
            Rating FLOAT,
            Votes INT,
            Summary NVARCHAR(MAX)
        )
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def create_meta_table(self):
        """创建电影元数据表"""
        sql = """
           IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Movies_Meta')
           CREATE TABLE Movies_Meta (
               Id INT PRIMARY KEY,
               Country NVARCHAR(100),
               Genre NVARCHAR(100),
               FOREIGN KEY (Id) REFERENCES Movies_Basic(Id)
           )
           """
        self.cursor.execute(sql)
        self.conn.commit()

    def create_basic_table(self):
        """创建电影基础信息表"""
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Movies_Basic')
        CREATE TABLE Movies_Basic (
            Id INT PRIMARY KEY IDENTITY(1,1),
            Rank INT,
            Title NVARCHAR(255),
            Year NVARCHAR(50),
            Rating FLOAT,
            Votes INT
        )
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def create_links_table(self):
        """创建电影链接表"""
        sql = """
           IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Movies_Links')
           CREATE TABLE Movies_Links (
               Id INT PRIMARY KEY,
               Url NVARCHAR(255),
               FOREIGN KEY (Id) REFERENCES Movies_Basic(Id)
           )
           """
        self.cursor.execute(sql)
        self.conn.commit()

    def create_crew_table(self):
        """创建电影演职人员表"""
        sql = """
           IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Movies_Crew')
           CREATE TABLE Movies_Crew (
               Id INT PRIMARY KEY,
               Director NVARCHAR(100),
               Actors NVARCHAR(MAX),
               FOREIGN KEY (Id) REFERENCES Movies_Basic(Id)
           )
           """
        self.cursor.execute(sql)
        self.conn.commit()

    def create_content_table(self):
        """创建电影内容表"""
        sql = """
           IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Movies_Content')
           CREATE TABLE Movies_Content (
               Id INT PRIMARY KEY,
               Summary NVARCHAR(MAX),
               FOREIGN KEY (Id) REFERENCES Movies_Basic(Id)
           )
           """
        self.cursor.execute(sql)
        self.conn.commit()

    def migrate_basic_data(self):
        """迁移基础信息数据"""
        # 开启 IDENTITY_INSERT
        self.cursor.execute("SET IDENTITY_INSERT Movies_Basic ON")
        try:
            sql = """
               INSERT INTO Movies_Basic (Id, Rank, Title, Year, Rating, Votes)
               SELECT 
                   Id, 
                   Rank, 
                   Title,
                   TRY_CAST(SUBSTRING(Year, 1, PATINDEX('%[^0-9]%', Year + 'a') - 1) AS INT),
                   Rating, 
                   Votes
               FROM Movies
               """
            self.cursor.execute(sql)  # 确保这里没有额外的参数
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            # 关闭 IDENTITY_INSERT
            self.cursor.execute("SET IDENTITY_INSERT Movies_Basic OFF")
    def migrate_meta_data(self):
        """迁移元数据"""
        sql = """
           INSERT INTO Movies_Meta (Id, Country, Genre)
           SELECT Id, Country, Genre FROM Movies
           """
        self.cursor.execute(sql)
        self.conn.commit()

    def migrate_links_data(self):
        """迁移链接数据"""
        sql = """
           INSERT INTO Movies_Links (Id, Url)
           SELECT Id, Url FROM Movies
           """
        self.cursor.execute(sql)
        self.conn.commit()

    def migrate_crew_data(self):
        """迁移演职人员数据"""
        sql = """
           INSERT INTO Movies_Crew (Id, Director, Actors)
           SELECT Id, Director, Actors FROM Movies
           """
        self.cursor.execute(sql)
        self.conn.commit()

    def migrate_content_data(self):
        """迁移内容数据"""
        sql = """
           INSERT INTO Movies_Content (Id, Summary)
           SELECT Id, Summary FROM Movies
           """
        self.cursor.execute(sql)
        self.conn.commit()

    def execute_vertical_sharding(self):
        """执行完整的垂直分表操作"""
        try:
            print("开始创建分表结构...")
            self.create_basic_table()
            self.create_meta_table()
            self.create_links_table()
            self.create_crew_table()
            self.create_content_table()

            print("开始迁移数据...")
            self.migrate_basic_data()
            self.migrate_meta_data()
            self.migrate_links_data()
            self.migrate_crew_data()
            self.migrate_content_data()

            print("垂直分表完成!")
        except Exception as e:
            print(f"分表过程中发生错误: {str(e)}")
            self.conn.rollback()

    def create_registry_table(self):
        """创建注册表，包含身份字段"""
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Registry')
        CREATE TABLE Registry (
            Username NVARCHAR(255) PRIMARY KEY,
            Password NVARCHAR(255),
            Role NVARCHAR(50) DEFAULT '用户' 
        )
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def close_connection(self):
        """关闭连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def insert_user(self, username, password, role):
        """插入用户信息"""
        insert_sql = "INSERT INTO Registry (Username, Password, Role) VALUES (?, ?, ?)"
        try:
            self.cursor.execute(insert_sql, (username, password, role))
            self.conn.commit()
            return True
        except pyodbc.IntegrityError:
            return False

    def insert_data(self, data_list):
        """插入数据"""
        insert_sql = """
        INSERT INTO Movies (
            Rank, Title, Url, Director, Actors, Year, Country, Genre, Rating, Votes, Summary
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
        inserted_titles = set()
        for data in data_list:
            title = data["标题"]
            if title in inserted_titles:
                print(f"重复插入的电影标题: {title}")
                continue
            inserted_titles.add(title)
            params = (
                data["序号"],
                data["标题"],
                data["链接"],
                data["导演"],
                data["主演"],
                data["年份"],
                data["国家"],
                data["类型"],
                float(data["评分"]),
                int(data["评价人数"].replace("人评价", "")),
                data["简介"]
            )
            self.cursor.execute(insert_sql, params)
        self.conn.commit()

    def check_user(self, username, password):
        """检查用户信息"""
        query = "SELECT * FROM Registry WHERE Username = ? AND Password = ?"
        self.cursor.execute(query, (username, password))
        return self.cursor.fetchone() is not None

    def close_connection(self):
        """关闭连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()