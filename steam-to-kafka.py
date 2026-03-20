import time
import json
import requests
import pymongo
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["steam_analytics"]
col_names = db["game_names"]

TOPICO = 'steam_trending'

def obter_nome_jogo(appid):
    jogo_salvo = col_names.find_one({"appid": appid})
    if jogo_salvo:
        return jogo_salvo['name']
    
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&filters=basic"
    try:
        res = requests.get(url, timeout=10).json()
        if res and str(appid) in res and res[str(appid)]['success']:
            nome = res[str(appid)]['data']['name']
            col_names.update_one({"appid": appid}, {"$set": {"name": nome}}, upsert=True)
            return nome
    except:
        pass
    return "Desconhecido"

def main():
    
    while True:
        url = "https://api.steampowered.com/ISteamChartsService/GetGamesByConcurrentPlayers/v1/"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                ranks = response.json().get("response", {}).get("ranks", [])
                timestamp = time.strftime('%Y-%m-%dT%H:%M:%S')
                
                print(f"[{timestamp}] Processando {len(ranks)} jogos...")
                
                for item in ranks:
                    appid = item.get("appid")
                    v_players = item.get("concurrent_in_game") or 0
                    nome_real = obter_nome_jogo(appid)
                    
                    mensagem = {
                        "rank": item.get("rank"),
                        "appid": appid,
                        "nome": nome_real,
                        "concurrent_players": int(v_players), 
                        "last_updated": timestamp
                    }
                    
                    producer.produce(TOPICO, value=json.dumps(mensagem).encode("utf-8"))
                
                producer.flush()
                print(f"DEBUG: {nome_real} processado com {v_players} players.")
            else:
                print(f"Erro Steam: {response.status_code}")
        except Exception as e:
            print(f"Erro: {e}")
        
        print("Aguardando 10 min...")
        time.sleep(600)

if __name__ == "__main__":
    main()