from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

mysql_config = {
    'host': '127.0.0.1',  # use IP para evitar problemas de resolução de "localhost"
    'user': 'root',
    'password': '123456789',
    'database': 'fashion_trends_db'
}

# Use pymysql
connection_url = f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}/{mysql_config['database']}"

try:
    print("Tentando conectar...")
    engine = create_engine(connection_url, connect_args={"connect_timeout": 5}, echo=True)
    with engine.connect() as connection:
        print("Conexão estabelecida!")

        version = connection.execute(text("SELECT VERSION();")).scalar()
        print("MySQL Server versão:", version)

        current_db = connection.execute(text("SELECT DATABASE();")).scalar()
        print("Banco de dados atual:", current_db)

except SQLAlchemyError as e:
    print("Erro ao conectar ao MySQL com SQLAlchemy:", e)

finally:
    if 'engine' in locals():
        engine.dispose()
        print("Conexão encerrada.")
