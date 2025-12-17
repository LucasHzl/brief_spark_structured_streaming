# 🚀 Pipeline de Streaming IoT avec Spark Structured Streaming & Kafka

---

## 🧠 Contexte

**SmartTech** est une entreprise spécialisée dans les **services connectés** et les **solutions IoT**. Des capteurs installés dans des bâtiments intelligents génèrent en continu des données (température, humidité, CO₂, etc.).

🎯 **Objectif du projet** : concevoir et implémenter un **pipeline de streaming temps réel** capable de :

* ingérer des données en continu,
* les transformer et nettoyer,
* les stocker de manière fiable pour l’analyse.

🧰 **Stack technique (on‑premise)** :

* 🟠 **Apache Kafka** — message broker
* 🔵 **Apache Spark Structured Streaming** — moteur de traitement temps réel
* 🟢 **Delta Lake** — stockage transactionnel

---

## 🏗️ Architecture globale

```text
[ Simulateur de capteurs ]
            |
            v
        [ Kafka ]
            |
            v
[ Spark Structured Streaming ]
            |
            v
     [ Delta Lake ]
      (Bronze / Silver)
```

---

## ⚙️ Prérequis & Nettoyage (OBLIGATOIRE)

### 🔹 Prérequis

* Docker Desktop installé et lancé
* Python **3.10+**
* **uv** installé

---

### 🔹 Installation des dépendances

Le projet utilise **uv** pour la gestion des dépendances.

✅ Si `pyproject.toml` et `uv.lock` sont présents :

```bash
uv sync
```

🔁 Sinon :

```bash
uv add pyspark delta-spark confluent-kafka
uv sync
```

---

### 🧹 Nettoyage des sorties (AVANT TOUTE EXÉCUTION)

⚠️ **Étape obligatoire**, notamment lors du clonage.

Les dossiers de checkpoints et de sorties Delta contiennent des **états locaux** (offsets Kafka, état Spark). Sans nettoyage, le pipeline peut ne pas redémarrer correctement.

📍 Depuis la racine du projet :

#### 🥉 Bronze (Partie 2.1)

```bash
rm -rf data/checkpoints/bronze_checkpoints/*
rm -rf data/output/bronze_output/*
```

#### 🥈 Silver (Partie 2.2)

```bash
rm -rf data/checkpoints/silver_kafka
rm -rf data/output/silver_delta
```

---

## 🥉 Partie 2.1 — Pipeline simple (JSON → Delta Bronze)

### 🎯 Objectif

Découvrir **Spark Structured Streaming** via un flux simulé par fichiers, sans message broker.

### 🛠️ Fonctionnalités

* Lecture de fichiers **JSON en streaming**
* Nettoyage et typage des données
* Écriture en **Delta Lake — Bronze**
* Tolérance aux pannes via **checkpointing**

### 📂 Flux de données

```text
Fichiers JSON → Spark Structured Streaming → Delta Bronze
```

### ▶️ Lancer la partie Bronze

**Terminal 1** :

```bash
uv run python src/bronze_stream.py
```

**Terminal 2** (simulation du flux) :

```bash
cp data/source_json/*.json data/input/
```

💡 Copier les fichiers progressivement permet d’observer le comportement streaming.

---

## 🥈 Partie 2.2 — Pipeline avancé avec Kafka (Kafka → Delta Silver)

### 🎯 Objectif

Mettre en place un **vrai flux temps réel** grâce à Kafka, afin de découpler les producteurs et consommateurs et fiabiliser l’architecture.

---

### 🧩 Rôle de Kafka

Kafka apporte :

* 🔗 le **découplage** producteurs / consommateurs
* 📈 l’absorption des pics de charge
* 🛡️ la tolérance aux pannes
* 🔁 la **rejouabilité** via les offsets

📌 Concepts clés :

* **Topic** : `iot_smartech`
* **Partitions** : parallélisme
* **Offsets** : position de lecture
* **Consumer groups** : scalabilité

---

### ⚙️ Mise en œuvre

#### 1️⃣ Lancer Kafka (Docker)

```bash
cd kafka
docker compose up -d
docker compose ps
```

---

#### 2️⃣ Créer le topic Kafka

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --topic iot_smartech \
  --partitions 3 \
  --replication-factor 1
```

Vérification :

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 --list
```

---

#### 3️⃣ Consumer Spark (Kafka → Silver)

**Terminal 1** :

```bash
uv run python src/silver_from_kafka.py
```

⏳ Le job reste actif : comportement normal en streaming.

---

#### 4️⃣ Producer IoT (simulateur de capteurs)

**Terminal 2** :

```bash
uv run python src/producer_iot.py
```

Les événements IoT sont envoyés en continu dans Kafka.

---

## 🧱 Architecture Médaillon

| Niveau    | Rôle                                      |
| --------- | ----------------------------------------- |
| 🥉 Bronze | Données brutes / quasi brutes             |
| 🥈 Silver | Données nettoyées et normalisées          |
| 🥇 Gold   | Données métiers agrégées (non implémenté) |

---

## 🧪 Vérifications

* Le topic `iot_smartech` est visible dans Kafka
* Les offsets augmentent côté consumer
* Le dossier `data/output/silver_delta` contient :

  * `_delta_log/`
  * des fichiers `part-*.parquet`

---

## 🛑 Arrêt du pipeline

* Stopper le producer : `Ctrl + C`
* Stopper le consumer Spark : `Ctrl + C`
* Arrêter Kafka :

```bash
docker compose down
```

---

## 🧠 Conclusion

Ce projet met en œuvre une **architecture de streaming temps réel complète**, proche des standards industriels. Il démontre :

* l’intérêt de **Kafka** pour le découplage et la fiabilité,
* la puissance de **Spark Structured Streaming** pour le traitement continu,
* l’apport de **Delta Lake** pour un stockage transactionnel.

Le pipeline est **scalable, tolérant aux pannes et rejouable**, constituant une base solide pour des usages avancés (dashboards temps réel, alerting, machine learning).

*Projet réalisé par Lucas Hzl*