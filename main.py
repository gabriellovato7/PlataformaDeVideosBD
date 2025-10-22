import random
from faker import Faker

from postgres_connection import connect_postgres
from mongo_connection import connect_mongo

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
                listUser.append(userId)
                
                plan = random.choice(['básico', 'premium', 'família'])
                status = random.choice(['ativo', 'cancelado', 'pendente'])
                
                insertSubQuery = "INSERT INTO subscriptions (user_id, plan, status) VALUES (%s, %s, %s);"
                cursor.execute(insertSubQuery, (userId, plan, status))

            conn.commit()
            print(f"{numUsers} usuarios e assinaturas inseridos")
            return listUser

    except Exception as e:
        conn.rollback() 
        print(f"Erro: {e}")
        return []

def populate_mongo(db, numVideos):
    
    try:
        collectionVideos = db["videos"] 
        
        insertVideos = []
        for i in range(numVideos):
            
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
            
        collectionVideos.insert_many(insertVideos)
        
        print(f"{len(insertVideos)} inserido")
        
    except Exception as e:
        print(f"Erro: {e}")

def main():
    
    pg_conn = None
    mongo_db = None
    
    try:
        pg_conn = connect_postgres()
        mongo_db = connect_mongo() 
        
        if pg_conn is not None and mongo_db is not None:
            populate_postgres(pg_conn, numUsers=5)
            
            populate_mongo(mongo_db, numVideos=10)

            print("Dados inseridos")
        else:
            print("Erro ao inserir")
            
    finally:
        if pg_conn:
            pg_conn.close()
            print("conexao fechada")

if __name__ == "__main__":
    main()