from pymongo import MongoClient


def connect_mongo():
    try:
        uri = "mongodb+srv://gabriellcc20_db_user:TEi7JhRIFqDyXQUa@cluster0.ejpww0t.mongodb.net/?retryWrites=true&w=majority&tls=true"

        client = MongoClient(uri)
        db = client["PlataformadeVideos"] 

        print("Conexao realizada mongo")
        return db

    except Exception as e:
        print("Erro:", e)
        return None