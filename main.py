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
	
        temas = [
            "tecnologia", "culinária", "viagem", "música", "esportes",
            "cinema", "educação", "história", "moda", "games",
            "ciência", "animais", "humor", "carros", "notícias"
        ]

        estruturas_titulo = [
            "Como {acao} sobre {tema}",
            "Aprenda {acao} de {tema}",
            "Dicas para {acao} em {tema}",
            "Os melhores vídeos sobre {tema}",
            "Curiosidades sobre {tema}",
            "História de {tema}",
            "Top 10 de {tema}",
            "Tudo sobre {tema}"
        ]

        exemplos_acoes = [
            "melhorar", "aprender", "dominar", "entender", "explorar",
            "conhecer", "descobrir", "criar", "aproveitar", "assistir"
        ]

        for i in range(numVideos):
            tema = random.choice(temas)
            estrutura = random.choice(estruturas_titulo)
            acao = random.choice(exemplos_acoes)

            titulo = estrutura.format(tema=tema, acao=acao).capitalize()

            descricao = (
                f"Neste vídeo, você vai {acao} mais sobre {tema}. "
                f"Falamos sobre dicas, curiosidades e informações importantes "
                f"para quem se interessa por {tema}. Não se esqueça de curtir e se inscrever no canal!"
            )

            duracao = random.randint(60, 3600)
            tags = [tema] + fake.words(nb=random.randint(1, 4), unique=True)
            upload_date = fake.date_time_between(
                    start_date=datetime(2010, 1, 1),
    				end_date=datetime(2025, 12, 31)
            ).isoformat()

            documentVideo = {
                "_id": f"vid_{fake.uuid4()}",
                "title": titulo,
                "description": descricao,
                "tags": tags,
                "upload_date": upload_date,
                "duration": duracao,
                "views": random.randint(0, 1_000_000)
            }
            insertVideos.append(documentVideo)

            videosList.append({
                "id": documentVideo["_id"],
                "title": titulo,
                "duration": duracao
            })

        collectionVideos.insert_many(insertVideos)
        print(f"{len(insertVideos)} vídeos inseridos no MongoDB.")
        return videosList

    except Exception as e:
        print(f"Erro: {e}")
        return []
    
from datetime import datetime, timedelta

def populate_astra(db_astra, listUser, videos):
    historico = db_astra.get_collection("historico_visualizacoes")

    registros = []
    now = datetime.now()

    for user in listUser:
        qtd_visualizacoes = random.randint(1, len(videos))
        videos_assistidos = random.sample(videos, qtd_visualizacoes)

        for video in videos_assistidos:
            try:
                upload_dt = datetime.fromisoformat(video.get("upload_date", now.isoformat()))

                inicio = upload_dt
                fim = now

                if inicio > fim:
                    inicio = fim - timedelta(days=random.randint(0, 30))

                delta = fim - inicio
                random_days = random.randint(0, max(delta.days, 0))
                random_seconds = random.randint(0, 86400)
                data_visualizacao = (inicio + timedelta(days=random_days, seconds=random_seconds)).isoformat()

                registro = {
                    "user_id": user["id"],
                    "user_name": user["name"],
                    "video_id": video["id"],
                    "titulo_video": video["title"],
                    "data_visualizacao": data_visualizacao,
                    "tempo_assistido": random.randint(10, video["duration"]),
                    "dispositivo": random.choice(["desktop", "mobile", "tv"])
                }
                registros.append(registro)

            except Exception as e:
                print(f"Erro ao gerar data para vídeo {video['id']}: {e}")

    if registros:
        historico.insert_many(registros)
        print(f"{len(registros)} visualizações inseridas no Astra DB.")
    else:
        print("Nenhum registro gerado (lista vazia).")

def get_user_data(pg_conn, user_id):
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT u.id, u.name, u.email, s.plan, s.status
                FROM users u
                JOIN subscriptions s ON u.id = s.user_id
                WHERE u.id = %s;
                """,
                (user_id,)
            )
            user = cursor.fetchone()
            if user:
                return {
                    "id": user[0],
                    "name": user[1],
                    "email": user[2],
                    "plan": user[3],
                    "status": user[4]
                }
            else:
                return None
    except Exception as e:
        print(f"Erro: {e}")
        return None

def get_user_history(astra_db, user_id):
    try:
        historico = astra_db.get_collection("historico_visualizacoes")
        result = historico.find({"user_id": user_id})
        return list(result)
    except Exception as e:
        print(f"Erro ao buscar histórico no Astra DB: {e}")
        return []

def main():
    
    pg_conn = None
    mongo_db = None
    astra_db = None
    
    try:
        pg_conn = connect_postgres()
        mongo_db = connect_mongo() 
        astra_db = connected_cassandra()
        
        if pg_conn is None or mongo_db is None or astra_db is None:
            print("Erro na conexão com um ou mais bancos.")
            return
        
        print("\n=== MENU PRINCIPAL ===")
        print("1 - Inserir dados aleatórios")
        print("2 - Consultar dados de um usuário")
        opcao = input("Escolha uma opção: ")

        if opcao == "1":
            user_ids = populate_postgres(pg_conn, numUsers=5)
            videos = populate_mongo(mongo_db, numVideos=10)
            populate_astra(astra_db, user_ids, videos)
            print("Inserção concluída.")
        
        elif opcao == "2":
            user_id = input("Digite o ID do usuário: ")
            user_data = get_user_data(pg_conn, user_id)

            if not user_data:
                print("Usuário não encontrado.")
                return

            print(f"\n--- Dados do Usuário ---")
            print(f"ID: {user_data['id']}")
            print(f"Nome: {user_data['name']}")
            print(f"E-mail: {user_data['email']}")
            print(f"Plano: {user_data['plan']}")
            print(f"Status: {user_data['status']}")

            historico = get_user_history(astra_db, user_data["id"])
            if not historico:
                print("\nNenhum vídeo assistido encontrado.")
            else:
                print("\n--- Histórico de Visualizações ---")
                for h in historico:
                    print(f"- {h['titulo_video']} ({h['tempo_assistido']} seg, {h['dispositivo']})")
        else:
            print("Opção inválida.")

            
    finally:
        if pg_conn:
            pg_conn.close()
            print("conexao fechada")

if __name__ == "__main__":
    main()