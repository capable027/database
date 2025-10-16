from douban_spider import DoubanSpider
from db_connect import SQLServerDB
from flask import Flask, request, jsonify, render_template, redirect, session, url_for
from functools import wraps
import secrets
import pyodbc

app = Flask(__name__, static_folder='static', static_url_path='/static')
app.secret_key = secrets.token_hex(32)  # 生成安全的会话密钥

# 数据库配置（需根据实际环境修改）
DB_CONFIG = {
    "server": "localhost",
    "database": "MOVE4",
    "username": "sa2",
    "password": "123456"
}


class CrawlAndSaveData:
    def __init__(self, db_config):
        self.db_config = db_config

    def run(self):
        try:
            spider = DoubanSpider()
            data_list = spider.run()
            print(f"成功爬取 {len(data_list)} 条电影数据")

            if len(data_list) == 0:
                return render_template('error.html', error="未成功爬取到电影数据，请检查网络或爬虫代码。")

            db = SQLServerDB(**self.db_config)
            db.create_table()
            db.insert_data(data_list)
            try:
                print("开始创建分表结构...")
                db.create_basic_table()
                db.create_meta_table()
                db.create_links_table()
                db.create_crew_table()
                db.create_content_table()

                print("开始迁移数据...")
                try:
                    db.migrate_basic_data()
                    print("基础信息数据迁移成功")
                except Exception as e:
                    print(f"基础信息数据迁移错误: {str(e)}")
                    raise

                try:
                    db.migrate_meta_data()
                    print("元数据迁移成功")
                except Exception as e:
                    print(f"元数据迁移错误: {str(e)}")
                    raise

                try:
                    db.migrate_links_data()
                    print("链接数据迁移成功")
                except Exception as e:
                    print(f"链接数据迁移错误: {str(e)}")
                    raise

                try:
                    db.migrate_crew_data()
                    print("演职人员数据迁移成功")
                except Exception as e:
                    print(f"演职人员数据迁移错误: {str(e)}")
                    raise

                try:
                    db.migrate_content_data()
                    print("内容数据迁移成功")
                except Exception as e:
                    print(f"内容数据迁移错误: {str(e)}")
                    raise

                print("垂直分表完成!")
            except Exception as e:
                print(f"分表过程中发生错误: {str(e)}")
                # 去掉 self.conn.rollback()
            db.close_connection()

            return render_template('success.html', message=f"数据爬取并保存成功，共 {len(data_list)} 条记录")
        except Exception as e:
            print(f"爬取保存错误: {str(e)}")
            return render_template('error.html', error=f"数据爬取或保存失败: {str(e)}")

def require_admin(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            return redirect('/login')  # 未登录，重定向到登录页

        db = SQLServerDB(**DB_CONFIG)
        try:
            db.cursor.execute(
                "SELECT Role FROM Registry WHERE Username = ?",
                (session['username'],)
            )
            role = db.cursor.fetchone()
        except pyodbc.Error as e:
            print(f"数据库查询错误: {str(e)}")
            return jsonify({"error": "数据库查询失败"}), 500
        finally:
            db.close_connection()

        if not role or role[0] != '管理员':
            return render_template('error.html', error="权限不足，仅管理员可执行此操作")

        return f(*args, **kwargs)

    return decorated_function


# 应用启动时创建数据库表
def create_tables():
    db = SQLServerDB(**DB_CONFIG)
    try:
        db.create_table()
        db.create_registry_table()
        print("数据库表创建成功")
    except pyodbc.Error as e:
        print(f"数据库表创建失败: {str(e)}")
    finally:
        db.close_connection()


# 执行表创建
create_tables()


@app.route('/')
def index():
    if 'username' in session:
        return render_template('index.html')
    return redirect('/login')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if not username or not password:
            return render_template('login.html', error="用户名和密码不能为空")

        db = SQLServerDB(**DB_CONFIG)
        try:
            db.cursor.execute(
                "SELECT Username, Role FROM Registry WHERE Username = ? AND Password = ?",
                (username, password)
            )
            user = db.cursor.fetchone()
        except pyodbc.Error as e:
            print(f"登录查询错误: {str(e)}")
            return render_template('login.html', error="数据库查询失败")
        finally:
            db.close_connection()

        if user:
            session['username'] = username
            session['role'] = user[1]  # 存储用户角色
            return redirect('/')
        else:
            return render_template('login.html', error="用户名或密码错误")
    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        role = request.form.get('role', '用户')  # 默认为普通用户

        if not username or not password:
            return render_template('register.html', error="用户名和密码不能为空")

        db = SQLServerDB(**DB_CONFIG)
        try:
            success = db.insert_user(username, password, role)
        except pyodbc.Error as e:
            print(f"注册插入错误: {str(e)}")
            return render_template('register.html', error="数据库操作失败")
        finally:
            db.close_connection()

        if success:
            return redirect('/login')
        else:
            return render_template('register.html', error="用户名已存在")
    return render_template('register.html')


@app.route('/logout')
def logout():
    session.pop('username', None)
    session.pop('role', None)
    return redirect('/login')


@app.route('/crawl_and_save', methods=['POST'])
def crawl_and_save_data():
    if 'username' not in session:
        return redirect('/login')
    crawler = CrawlAndSaveData(DB_CONFIG)
    return crawler.run()


@app.route('/movies')
def get_all_movies():
    try:
        db = SQLServerDB(**DB_CONFIG)
        db.cursor.execute("SELECT * FROM Movies ORDER BY Rank")
        columns = [column[0] for column in db.cursor.description]
        movies = [dict(zip(columns, row)) for row in db.cursor.fetchall()]

        # 手动去除重复项
        unique_movies = []
        movie_titles = set()
        for movie in movies:
            if movie['Title'] not in movie_titles:
                unique_movies.append(movie)
                movie_titles.add(movie['Title'])

        db.close_connection()

        return render_template('movies.html', movies=unique_movies, title="所有电影")
    except pyodbc.Error as e:
        print(f"获取电影列表错误: {str(e)}")
        return render_template('error.html', error="获取电影数据失败")


@app.route('/movies/search')
def search_movies():
    keyword = request.args.get('keyword', '').strip()
    if not keyword:
        return render_template('error.html', error="请提供搜索关键词")

    try:
        db = SQLServerDB(**DB_CONFIG)
        query = "SELECT * FROM Movies WHERE Title LIKE ? ORDER BY Rank"
        params = [f"%{keyword}%"]
        db.cursor.execute(query, params)
        columns = [column[0] for column in db.cursor.description]
        movies = [dict(zip(columns, row)) for row in db.cursor.fetchall()]

        # 手动去除重复项
        unique_movies = []
        movie_titles = set()
        for movie in movies:
            if movie['Title'] not in movie_titles:
                unique_movies.append(movie)
                movie_titles.add(movie['Title'])

        db.close_connection()

        return render_template('movies.html',
                               movies=unique_movies,
                               title=f"搜索结果: '{keyword}'",
                               search_keyword=keyword)
    except pyodbc.Error as e:
        print(f"搜索电影错误: {str(e)}")
        return render_template('error.html', error="搜索失败")


@app.route('/movies/rating_range')
def filter_movies_by_rating_range():
    if 'username' not in session:
        return redirect('/login')

    min_rating = request.args.get('min_rating', type=float)
    max_rating = request.args.get('max_rating', type=float)

    # 参数验证
    if min_rating is None and max_rating is None:
        return render_template('error.html', error="至少需要提供 min_rating 或 max_rating 参数")
    if min_rating is not None and max_rating is not None and min_rating > max_rating:
        return render_template('error.html', error="min_rating 不能大于 max_rating")

    try:
        db = SQLServerDB(**DB_CONFIG)
        query = "SELECT * FROM Movies WHERE 1=1"
        params = []

        if min_rating is not None:
            query += " AND Rating >= ?"
            params.append(min_rating)
        if max_rating is not None:
            query += " AND Rating <= ?"
            params.append(max_rating)

        query += " ORDER BY Rating DESC"

        # 添加调试信息
        print("查询语句:", query)
        print("查询参数:", params)

        db.cursor.execute(query, params)
        columns = [column[0] for column in db.cursor.description]
        movies = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
        db.close_connection()

        title = "评分范围筛选结果"
        if min_rating is not None and max_rating is not None:
            title = f"评分 {min_rating} - {max_rating} 的电影"
        elif min_rating is not None:
            title = f"评分 ≥ {min_rating} 的电影"
        else:
            title = f"评分 ≤ {max_rating} 的电影"

        return render_template('movies.html',
                               movies=movies,
                               title=title,
                               min_rating=min_rating,
                               max_rating=max_rating)
    except pyodbc.Error as e:
        print(f"按评分范围筛选错误: {str(e)}")
        return render_template('error.html', error="筛选失败")


@app.route('/movies/delete', methods=['GET', 'POST'])
@require_admin
def delete_movie_page():
    error = None
    success = None
    movies = []

    try:
        # 获取所有电影列表
        db = SQLServerDB(**DB_CONFIG)
        db.cursor.execute("SELECT * FROM Movies ORDER BY Title")
        columns = [column[0] for column in db.cursor.description]
        movies = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
        db.close_connection()

        # 手动去除重复项
        unique_movies = []
        movie_titles = set()
        for movie in movies:
            if movie['Title'] not in movie_titles:
                unique_movies.append(movie)
                movie_titles.add(movie['Title'])
        movies = unique_movies

        # 处理删除请求
        if request.method == 'POST':
            movie_id = request.form.get('movie_id')
            if not movie_id:
                error = "无效的电影ID"
            else:
                # 执行删除
                db = SQLServerDB(**DB_CONFIG)
                db.cursor.execute("DELETE FROM Movies WHERE Id = ?", (movie_id,))
                db.conn.commit()
                db.close_connection()

                # 刷新电影列表
                db = SQLServerDB(**DB_CONFIG)  # 重新连接数据库
                db.cursor.execute("SELECT * FROM Movies ORDER BY Title")
                columns = [column[0] for column in db.cursor.description]
                movies = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
                db.close_connection()

                # 再次手动去除重复项
                unique_movies = []
                movie_titles = set()
                for movie in movies:
                    if movie['Title'] not in movie_titles:
                        unique_movies.append(movie)
                        movie_titles.add(movie['Title'])
                movies = unique_movies

                success = "电影已成功删除"

    except Exception as e:
        error = f"操作失败: {str(e)}"
        app.logger.error(f"删除电影页面错误: {str(e)}")

    return render_template('delete_movie.html',
                           error=error,
                           success=success,
                           movies=movies)


@app.route('/movies/delete/<int:movie_id>', methods=['POST'])
@require_admin
def confirm_delete_movie_post(movie_id):
    try:
        db = SQLServerDB(**DB_CONFIG)
        # 查询电影名称用于提示
        db.cursor.execute("SELECT Title FROM Movies WHERE Id = ?", (movie_id,))
        movie = db.cursor.fetchone()

        if not movie:
            db.close_connection()
            return jsonify({"success": False, "message": "电影不存在"})

        title = movie[0]
        # 执行删除
        db.cursor.execute("DELETE FROM Movies WHERE Id = ?", (movie_id,))
        db.conn.commit()
        db.close_connection()

        # 返回成功消息（用于前端提示）
        return jsonify({"success": True, "message": f"电影《{title}》已成功删除"})

    except pyodbc.Error as e:
        print(f"删除电影错误: {str(e)}")
        db.conn.rollback()
        db.close_connection()
        return jsonify({"success": False, "message": f"删除失败: {str(e)}"})




if __name__ == '__main__':
    with app.app_context():
        # 在main函数开头执行爬取和保存数据的操作
        crawler = CrawlAndSaveData(DB_CONFIG)
        crawler.run()
    app.run(host='0.0.0.0', port=5000, debug=True)