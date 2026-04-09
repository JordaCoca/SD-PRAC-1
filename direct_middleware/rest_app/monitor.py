import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

print("--- LISTADO DE LLAVES DE MÉTRICAS EN REDIS ---")
keys = r.keys("metrics:*")
if not keys:
    print("No se encontraron llaves que empiecen por 'metrics:'")
else:
    for k in sorted(keys):
        val = r.get(k)
        print(f"{k} => {val}")
print("----------------------------------------------")