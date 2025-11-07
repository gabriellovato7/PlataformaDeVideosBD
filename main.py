import random
from faker import Faker
from datetime import datetime

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
            print(f"{numUsers} usuários e assinaturas inseridos.")
            return listUser

    except Exception as e:
        conn.rollback() 
        print(f"Erro: {e}")
        return []

def populate_mongo(db, numVideos, numFilms, numSeries):

    videosList = []

    try:
        collectionVideos = db["videos"]
        collectionFilms = db["filmes"]
        collectionSeries = db["series"]

        insertVideos = []
        insertFilms = []
        insertSeries = []
	
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

        generos = ["ação", "comédia", "drama", "terror", "ficção científica", "romance", "animação", "aventura"]

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

            duracao_min = random.randint(5, 60)
            duracao = f"{duracao_min} min"

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
                "duration": duracao_min,
                "type": "video"
            })

        for i in range(numFilms):
            titulo = fake.sentence(nb_words=3).replace(".", "")
            diretor = fake.name()
            duracao_min = random.randint(80, 180)  
            duracao = f"{duracao_min} min"
            genero = random.choice(generos)
            ano = random.randint(1970, 2025)
            sinopse = f"Um filme de {genero} dirigido por {diretor}, lançado em {ano}."

            documentFilm = {
                "_id": f"film_{fake.uuid4()}",
                "title": titulo,
                "director": diretor,
                "gender": genero,
                "synopsis": sinopse,
                "year": ano,
                "duration": duracao,
            }
            insertFilms.append(documentFilm)
            videosList.append({
                "id": documentFilm["_id"],
                "title": titulo,
                "duration": duracao_min,
                "type": "filme"
            })

        for i in range(numSeries):
            titulo = fake.sentence(nb_words=3).replace(".", "")
            temporadas = random.randint(1, 10)
            episodios = random.randint(5, 20)
            genero = random.choice(generos)
            ano_inicio = random.randint(1995, 2025)
            sinopse = f"Série de {genero} com {temporadas} temporadas e {episodios} episódios por temporada."
            rating = round(random.uniform(1, 10), 1)
            duracaoEpisodio = random.randint(20, 60)

            documentSeries = {
                "_id": f"series_{fake.uuid4()}",
                "title": titulo,
                "gender": genero,
                "synopsis": sinopse,
                "year": ano_inicio,
                "episodesPerSeason": episodios,
                "season": temporadas,
                "durationEpisodios": f"{duracaoEpisodio} min",
                "rating": rating
            }
            insertSeries.append(documentSeries)

            videosList.append({
                "id": documentSeries["_id"],
                "title": titulo,
                "duration": duracaoEpisodio,
                "type": "serie"
            })

        collectionVideos.insert_many(insertVideos)
        collectionFilms.insert_many(insertFilms)
        collectionSeries.insert_many(insertSeries)
        print(f"{len(insertVideos)} vídeos inseridos no MongoDB.")
        print(f"{len(insertFilms)} filmes inseridos no MongoDB.")
        print(f"{len(insertSeries)} séries inseridas no MongoDB.")
        return videosList

    except Exception as e:
        print(f"Erro: {e}")
        return []

def populate_astra(db_astra, listUser, videos):
    historico = db_astra.get_collection("historico_visualizacoes")

    registros = []
    now = datetime.now()

    for user in listUser:
        qtd_visualizacoes = random.randint(1, len(videos))
        videos_assistidos = random.sample(videos, qtd_visualizacoes)

        for video in videos_assistidos:
            try:
                data_visualizacao = fake.date_time_between(start_date='-1y', end_date='now').isoformat()

                registro = {
                    "user_id": user["id"],
                    "user_name": user["name"],
                    "id": video["id"],
                    "titulo": video["title"],
                    "tipo": video.get("type", "video"),
                    "data_visualizacao": data_visualizacao,
                    "tempo_assistido": random.randint(5, video["duration"]),
                    "dispositivo": random.choice(["desktop", "mobile", "tv"])
                }
                registros.append(registro)

            except Exception as e:
                print(f"Erro ao gerar visualização para vídeo {video['id']}: {e}")

    if registros:
        historico.insert_many(registros)
        print(f"{len(registros)} visualizações inseridas no Astra DB.")
    else:
        print("Nenhum registro gerado (lista vazia).")

def get_all_users(pg_conn):
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute("SELECT id, name FROM users;")
            users = cursor.fetchall()
            return [{"id": u[0], "name": u[1]} for u in users]
    except Exception as e:
        print(f"Erro ao buscar usuários: {e}")
        return []
    
def get_all_videos_mongo(db):
    try:
        collections = ["videos", "filmes", "series"]
        all_videos = []

        for col_name in collections:
            collection = db[col_name]
            docs = list(collection.find())

            for doc in docs:
                tipo = "video" if col_name == "videos" else "filme" if col_name == "filmes" else "serie"

                duracao_str = (
                    doc.get("duration") or
                    doc.get("durationEpisodios") or
                    "0 min"
                )
                try:
                    duracao = int(str(duracao_str).split()[0])
                except Exception:
                    duracao = random.randint(10, 120)  

                all_videos.append({
                    "id": doc.get("_id", fake.uuid4()),
                    "title": doc.get("title", "Sem título"),
                    "duration": duracao,
                    "type": tipo
                })

        return all_videos

    except Exception as e:
        print(f"Erro: {e}")
        return []

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

        while True:
            print("\n=== MENU PRINCIPAL ===")
            print("1 - Inserir usuários")
            print("2 - Inserir vídeos")
            print("3 - Inserir filmes")
            print("4 - Inserir séries")
            print("5 - Inserir histórico (Astra)")
            print("6 - Inserir tudo (usuários, vídeos, filmes, séries e histórico)")
            print("7 - Consultar dados de um usuário")
            print("0 - Sair")
            opcao = input("Escolha uma opção: ")

            if 'listUser' not in locals():
                listUser = []
            if 'videosList' not in locals():
                videosList = []

            if opcao == "1":
                numUsers = int(input("Digite a quantidade de usuários que deseja inserir: "))
                listUser = populate_postgres(pg_conn, numUsers)

            elif opcao == "2":
                numVideos = int(input("Digite a quantidade de vídeos que deseja inserir: "))
                videosList += populate_mongo(mongo_db, numVideos, 0, 0)

            elif opcao == "3":
                numFilms = int(input("Digite a quantidade de filmes que deseja inserir: "))
                videosList += populate_mongo(mongo_db, 0, numFilms, 0)

            elif opcao == "4":
                numSeries = int(input("Digite a quantidade de séries que deseja inserir: "))
                videosList += populate_mongo(mongo_db, 0, 0, numSeries)

            elif opcao == "5":
                all_users = get_all_users(pg_conn)
                all_videos = get_all_videos_mongo(mongo_db)

                if not all_users:
                    print("Nenhum usuário encontrado.")
                elif not all_videos:
                    print("Nenhum vídeo, filme ou série encontrado.")
                else:
                    populate_astra(astra_db, all_users, all_videos)
                    print("Histórico gerado")


            elif opcao == "6":
                numUsers = int(input("Quantidade de usuários: "))
                numVideos = int(input("Quantidade de vídeos: "))
                numFilms = int(input("Quantidade de filmes: "))
                numSeries = int(input("Quantidade de séries: "))
                listUser = populate_postgres(pg_conn, numUsers)
                videosList = populate_mongo(mongo_db, numVideos, numFilms, numSeries)
                populate_astra(astra_db, listUser, videosList)
                print("Inserção concluída.")

            elif opcao == "7":
                user_id = input("Digite o ID do usuário: ")
                user_data = get_user_data(pg_conn, user_id)
                if not user_data:
                    print("Usuário não encontrado.")
                    continue

                historico = get_user_history(astra_db, user_data["id"])
                if not historico:
                    print("\nNenhum vídeo assistido encontrado.")
                else:
                    print("\n--- Histórico de Visualizações ---")
                    for h in historico:
                        print(f"- {h['titulo']} ({h['tipo']}, {h['dispositivo']})")
            elif opcao == "0":
                print("Encerrando")
                break
            else:
                print("Opção inválida, tente novamente.")

    finally:
        if pg_conn:
            pg_conn.close()
            print("conexao fechada")

if __name__ == "__main__":
    main()