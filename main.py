import random
from faker import Faker

from Service2.postgres_connection import connect_postgres
from Service2.mongo_connection import connect_mongo
from Service2.cassandra_connection import connected_cassandra

fake = Faker('pt_BR') 

def populate_postgres(conn, numUsers):

    listUser = []
    
    try:
        with conn.cursor() as cursor:
            for i in range(numUsers):
                name = fake.name()
                email = fake.unique.email() 
                
                insertUserQuery = "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id;"
                cursor.execute(insertUserQuery, (name, email))
                
                userId = cursor.fetchone()[0]
                
                plan = random.choice(['básico', 'premium', 'família'])
                status = random.choice(['ativo', 'cancelado', 'pendente'])
                
                insertSubQuery = "INSERT INTO subscriptions (user_id, plan, status) VALUES (%s, %s, %s);"
                cursor.execute(insertSubQuery, (userId, plan, status))
                listUser.append({"id": userId, "name": name})

            conn.commit()
            print(f"{numUsers} usuarios e assinaturas inseridos")
            return listUser

    except Exception as e:
        conn.rollback() 
        print(f"Erro: {e}")
        return []

def populate_mongo(db, numVideos):

    videosList = []
    
    try:
        collectionVideos = db["videos"] 
        
        insertVideos = []
        for i in range(numVideos):

            doc_id = f"vid_{fake.uuid4()}"
            doc_title = fake.sentence(nb_words=6)
            doc_duration = random.randint(60, 3600)
            
            documentVideo = {
                "_id": f"vid_{fake.uuid4()}", 
                "title": fake.sentence(nb_words=6),
                "description": fake.paragraph(nb_sentences=3),
                "tags": fake.words(nb=random.randint(1, 5), unique=True),
                "upload_date": fake.iso8601(),
                "duration": random.randint(60, 3600),
                "views": random.randint(0, 1000000)
            }
            insertVideos.append(documentVideo)

            videosList.append({
                "id": doc_id, 
                "title": doc_title, 
                "duration": doc_duration
            })
            
        collectionVideos.insert_many(insertVideos)
        
        print(f"{len(insertVideos)} inserido")
        return videosList
        
    except Exception as e:
        print(f"Erro: {e}")
        return []
    
def populate_astra(db_astra, listUser, videos):
    historico = db_astra.get_collection("historico_visualizacoes")

    registros = []
    for user in listUser:
        qtd_visualizacoes = random.randint(1, len(videos))
        videos_assistidos = random.sample(videos, qtd_visualizacoes)

        for video in videos_assistidos:
            registro = {
                "user_id": user["id"],
                "user_name": user["name"],
                "video_id": video["id"],
                "titulo_video": video["title"],  
                "data_visualizacao": fake.iso8601(),
                "tempo_assistido": random.randint(10, video["duration"]),
                "dispositivo": random.choice(["desktop", "mobile", "tv"])
            }
            registros.append(registro)

    historico.insert_many(registros)
    print(f"{len(registros)} visualizações inseridas no Astra DB.")


def main():
    
    pg_conn = None
    mongo_db = None
    astra_db = None
    
    try:
        pg_conn = connect_postgres()
        mongo_db = connect_mongo() 
        astra_db = connected_cassandra()
        
        if pg_conn is not None and mongo_db is not None and astra_db is not None:
            user_ids = populate_postgres(pg_conn, numUsers=5)
            videos = populate_mongo(mongo_db, numVideos=10)
            populate_astra(astra_db, user_ids, videos)
            print("Dados inseridos")
        else:
            print("Erro ao inserir")
            
    finally:
        if pg_conn:
            pg_conn.close()
            print("conexao fechada")

if __name__ == "__main__":
    main()