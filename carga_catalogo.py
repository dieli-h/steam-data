import requests
import time
import pymongo


client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = client["steam_analytics"]
colecao = db["steam_catalogo"]

colecao.drop() 

res_lista = requests.get("https://steamspy.com/api.php?request=all&page=0", timeout=10).json()

# Pega os IDs desses 1000 jogos
appids = list(res_lista.keys())[:1000] 

contador = 0

for appid in appids:
    contador += 1
    try:
        # bater no endpoint de detalhes de cada jogo
        url_detalhe = f"https://steamspy.com/api.php?request=appdetails&appid={appid}"
        info = requests.get(url_detalhe, timeout=10).json()
        
        # puxa o gênero e transforma em lista
        texto_genero = info.get("genre", "")
        lista_generos = [g.strip() for g in texto_genero.split(",") if g.strip()]
        
        jogo = {
            "appid": int(appid),
            "name": info.get("name"),
            "developer": info.get("developer"),
            "publisher": info.get("publisher"),
            "genres": lista_generos, 
            "positive_reviews": info.get("positive", 0),
            "negative_reviews": info.get("negative", 0)
        }
        
        colecao.insert_one(jogo)
        

        if contador % 50 == 0:
            print(f"{contador} jogos processados...")
            
    except Exception as e:
        print(f"Erro no jogo {appid}: {e}")
        
    # Pausa de 1.5 segundos para o SteamSpy não banir o IP
    time.sleep(1.5)

print("Finalizado.")