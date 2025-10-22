from pymongo import MongoClient


def connect_mongo():
    try:
        uri = "mongodb+srv://gabriellcc20_db_user:3VrmZvOWy5dtbcdH@cluster0.ejpww0t.mongodb.net/"

        client = MongoClient(uri)
        db = client["PlataformadeVideos"] 

        print("Conexao realizada")
        return db

    except Exception as e:
        print("Erro:", e)
        return None