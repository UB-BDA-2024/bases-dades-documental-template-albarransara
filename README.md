[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=14098895&assignment_repo_type=AssignmentRepo)
# Pràctica 3: Bases de dades documentals

Benvingut/da a la tercera pràctica de l'assignatura de Bases de dades clau valor. Aquesta pràctica parteix de la plantilla de la pràctica anterior. Com sabeu tenim una API REST que ens permet crear, esborrar, modificar i consultar les dades d'una aplicació de sensors. Durant aquesta pràctica, treballarem amb una base de dades documental i farem servir MongoDB per a implementar-la.

## Com començar?

Per començar, necessitaràs clonar aquest repositori. Pots fer-ho amb el següent comandament:

```bash
git clone url-del-teu-assignment
```

Recorda que fem servir docker-compose per a executar l'entorn de desenvolupament i provar el que anem desenvolupant. Per a arrancar l'entorn de desenvolupament, pots fer servir el següent comandament:

```bash
docker-compose up -d
```

Recorda parar l'entorn de desenvolupament de la setmana passada abans de començar a treballar amb aquesta pràctica.

Si vols parar l'entorn de desenvolupament, pots fer servir el següent comandament:

```bash
docker-compose down
```

Cal que tinguis en compte que si fas servir aquest comandament, no esborraràs tota la informació que tinguis a la base de dades, ja que per defecte down només esborra els conteidors i la xarxa entre ells. Si vols esborrar tota la informació que tinguis a la base de dades, pots fer servir el següent comandament:

```bash
docker-compose down -v
```

**Important**: Quan executem `docker-compose up`, Docker construeix una imatge de Docker amb FastAPI amb una fotografia estàtica del codi que tenim al directori. Això vol dir que si modifiquem el codi, no es veurà reflexat a l'entorn de desenvolupament. Per això, cal que executem docker-compose up cada cop que modifiquem el codi. Si no ho fem, no veurem els canvis que haguem fet.

## Context
Cada cop la nostra api funciona millor, ara som capaços de guardar dades de milers de sensors i oferir velocitats acceptables als nostres usuaris. Això ha despertat interès 
i negoci ha decidit que és el moment d'ampliar el cas d'ús a diferents tipus de sensors.

Si fins ara només oferíem el sensor de temperatura i humitat, hem ampliat el catàleg de sensors amb els següents:

* Sensor de temperatura
* Sensor de llum
* Sensor de velocitat del vent
* Sensor de contaminació atmosfèrica
* Sensor de volumetria de pluja
* Sensor comptador de passatgers
* Sensor comptador de vehicles
* Sensor de velocitat de vehicles

Cadascun d'aquests sensors té un conjunt de dades diferents, però tots tenen un conjunt de dades comú:

* Identificador del sensor
* Data d'incorporació a la xarxa
* MAC del sensor
* Tipus de sensor

A sobre d'aquests cada sensor te un conjunt d'atributs propis com poden ser:

* Fabricant
* Model
* Número de sèrie
* Versió de firmware
* ...

I a més, cada sensor té un conjunt de dades dinàmics que només pot generar ell, per exemple:

* Sensor de temperatura:
	* Temperatura
	* Humitat 
* Sensor de llum: 
	* Intensitat de llum
* Velocitat del vent: 
	* Velocitat del vent
...

D'altres dades dinàmiques, però, són compartides per tots les sensors:

* Nivell de bateria
* Últim accés al sistema 

Per acomodar aquesta informació a la nostra API, hem decidit incorporar una nova base de dades documental, que ens permeti guardar aquesta informació semiestructurada. Volem conservar la base de dades relacional per apoder guardar la informació estàtica i estructurada dels sensors, que ens permetrà més endavant relacionar amb altres taules de la base de dades relacional.

Així doncs, volem crear una nova instància de MongoDB, que ens permeti guardar aquesta informació. 

A més a més, beient que mongoDB ens permet també realitzar índexs geoespacials, volem també guardar allà la informació de localització dels sensors (latitud i longitud), per tal de  poder fer cerques per cercar sensors que estiguin a una distància determinada d'un punt.


## Objectius

* Crear una nova instància de MongoDB
* Modificar la API per guardar la informació de localització dels sensors i les dades semiestructurades a la nova base de dades
* Estendre els Schemas per a que funcioni bé l'API de FastApi.
* Estendre redis per a guardar els diferents tipus de dades dinàmiques del nous sensors.
* Poder fer consultes CRUD als sensors que facin servir les dues bases de dades per darrera
* Poder fer cerques a la nova base de dades per cercar sensors que estiguin a una distància determinada d'un punt


## Què hem de fer?

Abans de res, explora el codi que s'ha creat per aquesta pràctica. Pots veure que s'ha mantingut la classe `Sensor` que representa un sensor.

### Punt 1: Mirar i Provar els tests

Tal i com vem fer a la setmana passada, hem creat una sèrie de tests per a comprovar que el codi que hem creat funciona correctament. Per a executar els tests, pots fer servir el següent comandament:

```bash
docker exec bdda_api sh -c "pytest"
```

També pots comprovar els tests a l'autograding de github, aquesta setmana la puntuació màxima és de 150 punts i hi ha un total de `15` tests amb un valor de `10` punts cadascun:

Veuràs que 15 tests fallen i 2, els pings, funcionen. Això és normal, ja que encara no hem implementat el codi que passa els tests.

### Punt 2: Mirar els endpoints al fitxer controller.py:


*  `@router.get("")`
*  `@router.get("/{sensor_id}")`
*  `@router.post("")`
*  `@router.delete("/{sensor_id}")`
*  `@router.post("/{sensor_id}/data")`
*  `@router.get("/{sensor_id}/data")`
*  `@router.get("/near")`
 

### Punt 2: Connectar-nos a la base de dades de MongoDB


Per a escriure dades a MongoDB necesitarem una instancia de MongoDB, fixeu-vos que ja tenim una instància de MongoDB is a la nostra configuració de docker-compose. Aquesta instància de MongoDB està configurada per a que es pugui accedir des de qualsevol altre contenidor de docker que estigui a la mateixa xarxa.

Per a accedir a la instància de MongoDB des de Python, podeu fer servir la llibreria [pymongo](https://pymongo.readthedocs.io/en/stable/#). Veureu que ja tenim instal·lada aquesta llibreria a la nostra imatge de Docker.

A continuació podeu veure un exemple de com escriure dades a mogodb des de Python:

```python
import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Select database
mydb = client["mydatabase"]

# Select collection
mycol = mydb["customers"]

# Define document to insert
mydoc = {"name": "John", "address": "Highway 37"}

# Insert document into collection
x = mycol.insert_one(mydoc)

# Print document ID
print(x.inserted_id)

```

En aquest exemple, primer importem la llibreria PyMongo i ens connectem al nostre servidor MongoDB usant `MongoClient()`. A continuació, seleccionem la base de dades i la col·lecció amb la que volem treballar, en aquest cas, mydatabase i customers, respectivament.

A continuació, definim el document que volem inserir com un diccionari Python amb parells de claus i valors que representen els camps i valors que volem incloure en el document.

Finalment, cridem el mètode `insert_one()` en la nostra col·lecció, passant el nostre document com a argument. Aquest mètode retorna un objecte `InsertOneResult` que inclou la ID del document inserit, que imprimim a la consola.

Per a llegir dades de Mongodb, podeu fer servir el mètode `find_one`:

```python
import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Select database
mydb = client["mydatabase"]

# Select collection
mycol = mydb["customers"]

# Find one document in collection
x = mycol.find_one()

# Print document
print(x)


```

En aquest exemple, connectem al servidor MongoDB i seleccionem la base de dades i la col·lecció amb la que volem treballar. A continuació, cridem el mètode `find_one()` en la nostra col·lecció per recuperar un document. Aquest mètode retorna un diccionari Python que representa el document.

Finalment, imprimim el document a la consola.

Aquest exemple senzill us mostra com recuperar un únic document de la base de dades, però és possible recuperar diversos documents alhora fent servir el mètode `find()`. També podeu especificar un criteri de cerca per filtrar els documents que voleu recuperar.

Com us podeu imaginar, anar obtenint la connexió a la base de dades cada vegada que volem escriure o llegir dades és molt ineficient. Per això,  creeu una classe `MongoDBClient` que encapsuli la connexió a la base de dades i que tingui els mètodes `set` i `get` per a escriure i llegir dades de la base de dades.


### Punt 3: Codificar els endpoints de controller.py

Codifiqueu de nou  els endpoints de `controller.py`de tal forma que utilitzin MongoDb, PostgreSQL i Redis.
Haureu de modificar els mètodes a `repository.py` per tal d'implementar aquests canvis. Fixeu-vos bé amb els `schemas` que heu de fer servir per a que els `payloads` de l'API de fastAPI funcioni adequadament. 
També modifiqueu el client de MongoDB i Redis per tal d'encapsular les funcions que necessiteu per a interactuar amb les diferents bases de dades. 

### Punt 4: Executar els tests

Ara que ja has implementat les rutes, pots tornar a executar els tests per a veure si has fet bé les coses. Per fer-ho, has de fer servir el següent comandament:

```bash
docker exec bdda_api sh -c "pytest"
```

Si tot ha anat bé, hauries de veure que tots els tests passen.

## Entrega

Durant les pràctiques farem servir GitHub Classroom per gestionar les entregues. Per tal de clonar el repositori ja has hagut d'acceptar l'assignment a GitHub Classroom. Per entregar la pràctica has de pujar els canvis al teu repositori de GitHub Classroom. El repositori conté els tests i s'executa automàticament cada vegada que puges els canvis al teu repositori. Si tots els tests passen, la pràctica està correctament entregada.

Per pujar els canvis al teu repositori, has de fer servir el següent comandament:

```bash
git add .
git commit -m "Missatge de commit"
git push
```

## Puntuació

Aquesta pràctica té una puntuació màxima de 10 punts. La puntuació es repartirà de la següent manera:

- 6 punts: Correcta execució dels tests. Important, per a que la pràctica sigui avaluable heu d'aconseguir que com a mínim `12` dels `15` tests s'executin correctament.
- 2 punts: L'estil del codi i la seva estructura i documentació.
- 2 punts: La correcta implementació de la funcionalitat.

## Qüestionari d'avaluació de cada pràctica

Cada pràctica té un qüestionari d'avaluació. Aquest qüestionari ens permet avaluar el coneixement teòric i de comprensió de la pràctica. És obligatori i forma part de l'avaluació continua de l'assignatura. Per contestar el qüestionari, has d'anar al campus virtual de l'assignatura i anar a la secció de qüestionaris.
 


