# Rapport de Projet Big Data
## Système de Détection de Fraude Bancaire en Temps Réel

**Étudiant :** Mohamed Bendaamar  
**Filière :** Polytech International  
**Date :** Avril 2026

---

## Table des Matières

1. [Introduction et Contexte](#1-introduction-et-contexte)
2. [Problématique et Objectifs](#2-problématique-et-objectifs)
3. [Dataset](#3-dataset)
4. [Architecture Globale](#4-architecture-globale)
5. [Technologies Utilisées](#5-technologies-utilisées)
6. [Implémentation Détaillée](#6-implémentation-détaillée)
   - [6.1 MySQL — Base de données source](#61-mysql--base-de-données-source)
   - [6.2 Apache Kafka — Streaming en temps réel](#62-apache-kafka--streaming-en-temps-réel)
   - [6.3 Apache HDFS — Stockage distribué](#63-apache-hdfs--stockage-distribué)
   - [6.4 Apache Sqoop — Import/Export MySQL ↔ HDFS](#64-apache-sqoop--importexport-mysql--hdfs)
   - [6.5 Apache Spark — Analyse et Détection](#65-apache-spark--analyse-et-détection)
   - [6.6 Apache Flume — Collecte de Logs](#66-apache-flume--collecte-de-logs)
   - [6.7 Docker — Containerisation](#67-docker--containerisation)
7. [Pipeline Complet End-to-End](#7-pipeline-complet-end-to-end)
8. [Résultats et Analyse](#8-résultats-et-analyse)
9. [Difficultés Rencontrées et Solutions](#9-difficultés-rencontrées-et-solutions)
10. [Conclusion](#10-conclusion)

---

## 1. Introduction et Contexte

La fraude bancaire représente un problème majeur pour le secteur financier mondial. Selon les estimations, les pertes liées à la fraude par carte bancaire dépassent plusieurs dizaines de milliards d'euros par an. Détecter ces transactions frauduleuses en temps réel, parmi des millions d'opérations légitimes, est un défi technique considérable.

Ce projet propose une **architecture Big Data complète** capable de :
- Ingérer des transactions bancaires en streaming temps réel via **Apache Kafka**
- Stocker les données de manière distribuée via **Apache HDFS**
- Transférer des données entre une base relationnelle et HDFS via **Apache Sqoop**
- Analyser les transactions et détecter les fraudes via **Apache Spark**
- Collecter et centraliser les logs d'alertes via **Apache Flume**

L'ensemble du système est déployé en **conteneurs Docker**, garantissant reproductibilité, portabilité et isolation des services.

---

## 2. Problématique et Objectifs

### Pourquoi le Big Data ?

Les solutions traditionnelles (bases de données relationnelles seules, traitement séquentiel) ne sont pas adaptées à ce problème pour plusieurs raisons :

| Contrainte | Solution Traditionnelle | Solution Big Data |
|-----------|------------------------|-------------------|
| Volume de données | Limité par un seul serveur | Distribué sur plusieurs nœuds |
| Vitesse d'ingestion | Traitement batch différé | Streaming temps réel (Kafka) |
| Scalabilité | Verticale (upgrade serveur) | Horizontale (ajout de nœuds) |
| Tolérance aux pannes | Point de défaillance unique | Réplication automatique |
| Coût | Licences coûteuses | Open-source |

### Objectifs du Projet

1. **Ingérer** les transactions en temps réel depuis un fichier CSV simulant un flux bancaire
2. **Stocker** les données brutes dans une base relationnelle MySQL et dans HDFS
3. **Transférer** les données entre MySQL et HDFS dans les deux sens avec Sqoop
4. **Analyser** les transactions avec Spark pour détecter les anomalies
5. **Collecter** les logs d'alertes système avec Flume vers HDFS
6. **Containeriser** l'ensemble de l'infrastructure avec Docker

---

## 3. Dataset

### Source
Dataset Kaggle : **"Bank Transaction Dataset for Fraud Detection"**  
URL : Kaggle Banking Transactions Dataset

### Caractéristiques

| Attribut | Valeur |
|----------|--------|
| Nombre de transactions | **2 512 lignes** |
| Nombre de colonnes | **16 colonnes** |
| Taille du fichier | **345 KB** |
| Format | CSV (comma-separated) |
| Période couverte | 2023 |

### Description des Colonnes

| Colonne | Type | Description |
|---------|------|-------------|
| `TransactionID` | VARCHAR(20) | Identifiant unique (ex: TX000001) |
| `AccountID` | VARCHAR(20) | Identifiant du compte (ex: AC00128) |
| `TransactionAmount` | DECIMAL(10,2) | Montant en euros/dollars |
| `TransactionDate` | DATETIME | Date et heure de la transaction |
| `TransactionType` | VARCHAR(10) | "Debit" ou "Credit" |
| `Location` | VARCHAR(100) | Ville de la transaction |
| `DeviceID` | VARCHAR(20) | Identifiant de l'appareil utilisé |
| `IPAddress` | VARCHAR(45) | Adresse IP source |
| `MerchantID` | VARCHAR(20) | Identifiant du commerçant |
| `Channel` | VARCHAR(20) | Canal : ATM, Online, Branch |
| `CustomerAge` | INT | Âge du client |
| `CustomerOccupation` | VARCHAR(50) | Profession du client |
| `TransactionDuration` | INT | Durée de la transaction (secondes) |
| `LoginAttempts` | INT | Nombre de tentatives de connexion |
| `AccountBalance` | DECIMAL(10,2) | Solde du compte |
| `PreviousTransactionDate` | DATETIME | Date de la dernière transaction |

### Exemple de Données

```
TX000001, AC00128, 14.09,  2023-04-11 16:29:14, Debit,  San Diego, D000380, 162.198.218.92, M015, ATM,    70, Doctor,  81, 1, 5112.21, 2024-11-04
TX000002, AC00455, 376.24, 2023-06-27 16:44:19, Debit,  Houston,   D000051, 13.149.61.4,    M052, ATM,    68, Doctor, 141, 1, 13758.91, 2024-11-04
TX000027, AC00441, 246.93, 2023-04-17 16:37:01, Debit,  Miami,     D000046, 55.154.161.250, M029, ATM,    23, Student,158, 5,  673.35,  2024-11-04  ← FRAUDE (5 tentatives)
```

---

## 4. Architecture Globale

### Schéma du Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SOURCE DE DONNÉES                               │
│                                                                         │
│   transactions.csv (2512 lignes, 16 colonnes, 345 KB)                  │
└─────────────────────┬───────────────────────────┬───────────────────────┘
                      │                           │
                      ▼                           ▼
┌─────────────────────────────┐   ┌───────────────────────────────────────┐
│  MySQL 8.0 (bankdb)         │   │  Kafka Producer (Python)              │
│  ─────────────────          │   │  ─────────────────────────────────    │
│  Table: transactions        │   │  Lit le CSV ligne par ligne           │
│  2512 lignes chargées via   │   │  Envoie chaque transaction au topic   │
│  LOAD DATA INFILE au        │   │  "transactions" toutes les 0.5s       │
│  démarrage du conteneur     │   └───────────────┬───────────────────────┘
└──────────────┬──────────────┘                   │
               │                                  ▼
               │ SQOOP IMPORT              ┌──────────────┐
               │ (MySQL → HDFS)            │ Kafka Broker │
               ▼                           │ (Topic:      │
┌────────────────────────────┐             │ transactions)│
│  HDFS (Hadoop 3.2.1)       │             └──────────────┘
│  ──────────────────        │
│  /data/sqoop_import        │◄──── Sqoop Import ──── MySQL
│   └── part-m-00000         │
│       (2512 transactions)  │
│                            │
│  /data/fraud_alerts        │◄──── Spark écrit les résultats
│   └── part-*.csv           │
│       (461 fraudes)        │
│                            │
│  /data/flume_logs          │◄──── Flume collecte les logs
│   └── FlumeData.*          │
└──────────────┬─────────────┘
               │
               │ lecture par Spark
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Apache Spark (PySpark)                                                 │
│  ─────────────────────                                                  │
│  Lit les 2512 transactions depuis HDFS                                  │
│  Applique les règles de détection de fraude :                           │
│    - TransactionAmount > 1000                                           │
│    - LoginAttempts > 3                                                  │
│    - TransactionDuration > 200                                          │
│  Résultat : 461 transactions suspectes (18.3%)                          │
│  Sauvegarde dans HDFS /data/fraud_alerts                                │
└──────────────────────────────────────────────────────────────────────────┘
               │
               │ SQOOP EXPORT (HDFS → MySQL)
               ▼
┌────────────────────────────────────────┐
│  MySQL 8.0 (bankdb)                    │
│  Table: fraud_alerts (461 lignes)      │
└────────────────────────────────────────┘

Flux parallèle :
./logs/*.log  ──── Flume SpoolDir ──► HDFS /data/flume_logs
```

### Infrastructure Docker (9 conteneurs)

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network                           │
│                                                             │
│  zookeeper ──► kafka ──► producer                          │
│                                                             │
│  namenode ──► datanode                                      │
│     ▲                                                       │
│     │──── sqoop                                             │
│     │──── flume                                             │
│     │──── spark                                             │
│                                                             │
│  mysql ──► sqoop (connexion JDBC)                           │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. Technologies Utilisées

### Apache Kafka 7.5.0 (Confluent)

**Qu'est-ce que c'est ?**  
Apache Kafka est une plateforme de streaming distribuée. C'est un système de messagerie publish/subscribe conçu pour traiter des millions d'événements par seconde avec une latence très faible.

**Composants Kafka dans ce projet :**
- **ZooKeeper** : Gère les métadonnées du cluster Kafka (coordination)
- **Kafka Broker** : Serveur qui reçoit, stocke et distribue les messages
- **Producer** : Application Python qui publie des messages dans un topic
- **Topic "transactions"** : Canal de communication où transitent les messages

**Pourquoi Kafka pour ce projet ?**  
Les banques réelles traitent des milliers de transactions par seconde. Kafka permet d'ingérer ce flux en temps réel, de découpler les producteurs de données des consommateurs, et garantit la durabilité des messages (stockage sur disque).

**Concepts clés :**
- **Topic** : Catégorie dans laquelle les messages sont publiés (comme un canal)
- **Partition** : Division d'un topic pour le parallélisme
- **Offset** : Position d'un message dans une partition
- **Consumer Group** : Groupe de consommateurs qui lisent en parallèle

---

### Apache HDFS (Hadoop 3.2.1)

**Qu'est-ce que c'est ?**  
HDFS (Hadoop Distributed File System) est un système de fichiers distribué conçu pour stocker des fichiers de très grande taille sur un cluster de machines ordinaires.

**Architecture HDFS :**
- **NameNode** : Maître — stocke les métadonnées (quels blocs sont sur quels DataNodes)
- **DataNode** : Esclave — stocke réellement les blocs de données

**Pourquoi HDFS pour ce projet ?**  
HDFS est le système de stockage central du pipeline. Toutes les données transitent par HDFS avant d'être analysées par Spark. Il offre une tolérance aux pannes via la réplication des blocs (par défaut 3 copies).

**Facteur de réplication :** Dans ce projet, on utilise la réplication = 1 (environnement de développement mono-nœud).

---

### Apache Sqoop 1.4.7

**Qu'est-ce que c'est ?**  
Sqoop (SQL-to-Hadoop) est un outil conçu spécifiquement pour transférer des données entre des bases de données relationnelles (MySQL, Oracle, PostgreSQL...) et HDFS.

**Deux directions de transfert :**

```
IMPORT : MySQL ──────────────────────────────► HDFS
          (base relationnelle)    (système distribué)

EXPORT : HDFS  ──────────────────────────────► MySQL
          (résultats d'analyse)   (base relationnelle)
```

**Pourquoi Sqoop pour ce projet ?**  
- MySQL est la source de données structurées (transactions bancaires)
- HDFS est le lac de données pour l'analyse Spark
- Sqoop est le pont entre ces deux mondes
- Sqoop utilise MapReduce en arrière-plan pour paralléliser les transferts

**Fonctionnement interne :**
1. Sqoop interroge MySQL pour obtenir le schéma de la table
2. Génère du code Java (codegen) qui sait lire/écrire cette table
3. Lance des tâches MapReduce qui parallélisent le transfert
4. Les données sont écrites en CSV dans HDFS

---

### Apache Spark 4.x (PySpark)

**Qu'est-ce que c'est ?**  
Apache Spark est un moteur de traitement distribué ultra-rapide. Il peut traiter des données en mémoire (RAM), ce qui le rend 100x plus rapide que MapReduce classique.

**Composants Spark :**
- **SparkSession** : Point d'entrée de l'application
- **DataFrame** : Table distribuée (comme un tableau pandas mais sur un cluster)
- **Transformation** : Opération lazy (filter, select, groupBy...)
- **Action** : Déclenche l'exécution (count, show, write...)

**Pourquoi Spark pour ce projet ?**  
Spark peut lire directement depuis HDFS, appliquer des transformations sur des milliards de lignes en parallèle, et écrire les résultats dans HDFS. C'est le moteur d'analyse central du pipeline Big Data.

**Règles de détection appliquées :**
```python
fraud_df = df.filter(
    (col("TransactionAmount") > 1000)    # Montant anormalement élevé
    | (col("LoginAttempts") > 3)         # Trop de tentatives de connexion
    | (col("TransactionDuration") > 200) # Transaction anormalement longue
)
```

---

### Apache Flume 1.11.0

**Qu'est-ce que c'est ?**  
Apache Flume est un service de collecte, agrégation et transport de grandes quantités de données de logs. Il est conçu pour être fiable, distribué et configurable.

**Architecture Flume (modèle Agent) :**

```
Source ──────► Channel ──────► Sink
(collecte)    (tampon)        (destination)

SpoolDir      Memory          HDFS
(./logs)      (10000 msgs)    (/data/flume_logs)
```

**Composants dans ce projet :**
- **Source SpoolDir** : Surveille un dossier, ingère chaque nouveau fichier
- **Channel Memory** : Tampon en mémoire entre la source et le sink
- **Sink HDFS** : Écrit les événements collectés dans HDFS

**Pourquoi Flume pour ce projet ?**  
Dans une banque réelle, des milliers de serveurs génèrent des logs d'alertes (connexions suspectes, tentatives de fraude, erreurs système). Flume centralise tous ces logs vers HDFS pour archivage et analyse ultérieure.

**Comportement SpoolDir :**  
Quand Flume traite un fichier dans `./logs/`, il le renomme en `fichier.log.COMPLETED` pour éviter de le traiter deux fois. C'est une garantie de fiabilité (exactly-once processing).

---

### MySQL 8.0

**Rôle dans le projet :**  
MySQL est la **source de données principale**. Le fichier CSV des transactions est chargé dans MySQL au démarrage du conteneur. MySQL représente le système de base de données opérationnel d'une banque (OLTP).

**Deux tables :**
- `transactions` : Les 2512 transactions brutes (source)
- `fraud_alerts` : Les 461 transactions frauduleuses détectées (destination Sqoop export)

**Pourquoi MySQL et pas juste HDFS ?**  
Dans une architecture Big Data réelle, les systèmes opérationnels (banques, e-commerce) stockent leurs données dans des bases relationnelles. HDFS est le lac de données analytique. Sqoop fait le pont entre les deux. C'est l'architecture Lambda typique.

---

### Docker & Docker Compose

**Pourquoi Docker ?**  
- **Reproducibilité** : Le projet tourne identiquement sur n'importe quelle machine
- **Isolation** : Chaque service a son propre environnement, ses dépendances
- **Orchestration** : Docker Compose démarre tous les services dans le bon ordre
- **Portabilité** : Pas besoin d'installer Hadoop, Kafka, MySQL en local

**Les 9 conteneurs du projet :**

| Conteneur | Image | Rôle |
|-----------|-------|------|
| `zookeeper` | confluentinc/cp-zookeeper:7.5.0 | Coordination Kafka |
| `kafka` | confluentinc/cp-kafka:7.5.0 | Broker de messages |
| `namenode` | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 | Maître HDFS |
| `datanode` | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 | Stockage HDFS |
| `producer` | Build ./producer | Producteur Kafka Python |
| `spark` | apache/spark:latest | Moteur d'analyse |
| `mysql` | mysql:8.0 | Base de données source |
| `sqoop` | Build ./sqoop | Import/Export MySQL↔HDFS |
| `flume` | Build ./flume | Collecte de logs |

---

## 6. Implémentation Détaillée

### 6.1 MySQL — Base de données source

**Fichier :** `mysql/init.sql`

Ce script s'exécute automatiquement au premier démarrage du conteneur MySQL (mécanisme `docker-entrypoint-initdb.d`).

```sql
-- Création de la base
CREATE DATABASE IF NOT EXISTS bankdb;
USE bankdb;

-- Table principale des transactions
CREATE TABLE IF NOT EXISTS transactions (
  TransactionID           VARCHAR(20) PRIMARY KEY,
  AccountID               VARCHAR(20),
  TransactionAmount       DECIMAL(10,2),
  TransactionDate         DATETIME,
  TransactionType         VARCHAR(10),
  Location                VARCHAR(100),
  DeviceID                VARCHAR(20),
  IPAddress               VARCHAR(45),
  MerchantID              VARCHAR(20),
  Channel                 VARCHAR(20),
  CustomerAge             INT,
  CustomerOccupation      VARCHAR(50),
  TransactionDuration     INT,
  LoginAttempts           INT,
  AccountBalance          DECIMAL(10,2),
  PreviousTransactionDate DATETIME
);

-- Chargement automatique du CSV
LOAD DATA INFILE '/var/lib/mysql-files/transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Table de destination pour les résultats Sqoop export
CREATE TABLE IF NOT EXISTS fraud_alerts (
  -- Mêmes colonnes que transactions
  TransactionID VARCHAR(20),
  ...
);
```

**Volume Docker pour LOAD DATA INFILE :**  
MySQL interdit LOAD DATA INFILE sauf depuis `/var/lib/mysql-files/`. Dans `docker-compose.yml`, on monte `./data` sur ce chemin :
```yaml
volumes:
  - ./data:/var/lib/mysql-files
```

---

### 6.2 Apache Kafka — Streaming en temps réel

**Fichier :** `producer/producer.py`

```python
import csv, time
from kafka import KafkaProducer

time.sleep(20)  # Attendre que Kafka soit prêt

producer = KafkaProducer(
    bootstrap_servers="kafka:29092",
    value_serializer=lambda v: v.encode("utf-8")
)

with open("/app/data/transactions.csv", "r") as file:
    reader = csv.reader(file)
    next(reader)  # Skip header

    for row in reader:
        message = ",".join(row)
        producer.send("transactions", message)
        print("Sent:", message)
        time.sleep(0.5)  # 1 transaction toutes les 500ms
```

**Pourquoi `kafka:29092` et non `localhost:9092` ?**  
Dans Docker, les conteneurs communiquent entre eux via le nom du service. Le broker Kafka expose deux listeners :
- `PLAINTEXT://kafka:29092` → pour les communications **inter-conteneurs** (Producer → Kafka)
- `PLAINTEXT_HOST://localhost:9092` → pour les connexions depuis **la machine hôte**

**Configuration Kafka dans docker-compose.yml :**
```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

---

### 6.3 Apache HDFS — Stockage distribué

**Configuration dans docker-compose.yml :**
```yaml
namenode:
  image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
  environment:
    CLUSTER_NAME: test
    CORE_CONF_fs_defaultFS: hdfs://namenode:9000
  ports:
    - "9870:9870"  # Interface Web HDFS (accessible via http://localhost:9870)
    - "9000:9000"  # Port RPC HDFS

datanode:
  environment:
    CORE_CONF_fs_defaultFS: hdfs://namenode:9000
    SERVICE_PRECONDITION: "namenode:9870"  # Attend que le NameNode soit prêt
```

**Structure des répertoires HDFS utilisés :**
```
hdfs://namenode:9000/
└── data/
    ├── sqoop_import/        ← Import MySQL (2512 transactions)
    │   ├── _SUCCESS
    │   └── part-m-00000     ← Fichier CSV sans en-tête
    ├── fraud_alerts/        ← Résultats Spark (461 fraudes)
    │   ├── _SUCCESS
    │   └── part-*.csv
    └── flume_logs/          ← Logs collectés par Flume
        └── FlumeData.*
```

**Interface Web HDFS :** `http://localhost:9870`  
Permet de naviguer dans l'arborescence HDFS, voir les statistiques du cluster, l'espace disque, etc.

---

### 6.4 Apache Sqoop — Import/Export MySQL ↔ HDFS

**Fichier :** `sqoop/Dockerfile`

```dockerfile
FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

# Fix des dépôts Debian Stretch (EOL — dépôts déplacés vers archive.debian.org)
RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && ...
    apt-get update && apt-get install -y wget

# Sqoop 1.4.7
RUN wget https://archive.apache.org/dist/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz

# Connecteur JDBC MySQL 8
RUN wget mysql-connector-java-8.0.28.jar → /opt/sqoop/lib/

# commons-lang 2.6 (requis par Sqoop, supprimé dans Hadoop 3.x)
RUN wget commons-lang-2.6.jar → /opt/sqoop/lib/

ENV SQOOP_HOME=/opt/sqoop
ENV HADOOP_HOME=/opt/hadoop-3.2.1
```

**Problème de compatibilité Sqoop + Hadoop 3.x :**  
Sqoop 1.4.7 a été conçu pour Hadoop 2.x. Hadoop 3.x a supprimé la dépendance `commons-lang` v2 (gardée seulement v3). Sqoop en a besoin. Solution : télécharger `commons-lang-2.6.jar` manuellement dans le Dockerfile.

**Commande d'import MySQL → HDFS :**
```bash
sqoop import \
  --connect "jdbc:mysql://mysql:3306/bankdb?allowPublicKeyRetrieval=true&useSSL=false" \
  --username sqoop_user --password sqoop_pass \
  --table transactions \
  --target-dir /data/sqoop_import \
  --m 1 \
  --driver com.mysql.jdbc.Driver
```

Paramètres expliqués :
- `--connect` : URL JDBC de connexion MySQL. `allowPublicKeyRetrieval=true` est requis pour MySQL 8+
- `--table transactions` : Table source à importer
- `--target-dir` : Répertoire HDFS de destination
- `--m 1` : Utiliser 1 seul mapper MapReduce (1 seul DataNode dans ce projet)
- `--driver` : Classe du driver JDBC

**Commande d'export HDFS → MySQL :**
```bash
sqoop export \
  --connect "jdbc:mysql://mysql:3306/bankdb?allowPublicKeyRetrieval=true&useSSL=false" \
  --username sqoop_user --password sqoop_pass \
  --table fraud_alerts \
  --export-dir /data/fraud_alerts \
  --input-fields-terminated-by "," \
  --m 1
```

---

### 6.5 Apache Spark — Analyse et Détection

**Fichier :** `spark/fraud_export.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("FraudDetectionExport") \
    .getOrCreate()

# Définition explicite du schéma (les données Sqoop n'ont pas d'en-tête)
schema = StructType([
    StructField("TransactionID", StringType()),
    StructField("AccountID", StringType()),
    StructField("TransactionAmount", DoubleType()),
    StructField("TransactionDate", StringType()),
    StructField("TransactionType", StringType()),
    StructField("Location", StringType()),
    StructField("DeviceID", StringType()),
    StructField("IPAddress", StringType()),
    StructField("MerchantID", StringType()),
    StructField("Channel", StringType()),
    StructField("CustomerAge", IntegerType()),
    StructField("CustomerOccupation", StringType()),
    StructField("TransactionDuration", IntegerType()),
    StructField("LoginAttempts", IntegerType()),
    StructField("AccountBalance", DoubleType()),
    StructField("PreviousTransactionDate", StringType()),
])

# Lecture depuis HDFS (données importées par Sqoop)
df = spark.read.csv(
    "hdfs://namenode:9000/data/sqoop_import/part-m-00000",
    header=False,
    schema=schema
)

# Détection de fraude — 3 règles
fraud_df = df.filter(
    (col("TransactionAmount") > 1000)    # Montant > 1000€
    | (col("LoginAttempts") > 3)         # Plus de 3 tentatives de connexion
    | (col("TransactionDuration") > 200) # Transaction > 200 secondes
)

# Sauvegarde dans HDFS pour Sqoop export
fraud_df.coalesce(1).write.mode("overwrite") \
    .option("header", "false") \
    .csv("hdfs://namenode:9000/data/fraud_alerts")
```

**Pourquoi `coalesce(1)` ?**  
Par défaut, Spark écrit autant de fichiers que de partitions (workers). `coalesce(1)` force la consolidation en un seul fichier, ce qui est requis par Sqoop export qui attend un fichier unique cohérent.

**Commande de lancement :**
```bash
spark-submit \
  --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /app/fraud_export.py
```

- `--master local[*]` : Utilise tous les cœurs CPU locaux (pas de cluster Spark dédié)
- `--conf spark.hadoop.fs.defaultFS` : Indique à Spark où trouver HDFS

---

### 6.6 Apache Flume — Collecte de Logs

**Fichier :** `flume/flume.conf`

```properties
# Agent composé d'une Source, un Channel, un Sink
agent.sources  = src1
agent.sinks    = sink1
agent.channels = ch1

# SOURCE : SpoolDir — surveille /logs en permanence
agent.sources.src1.type       = spooldir
agent.sources.src1.spoolDir   = /logs
agent.sources.src1.fileHeader = true  # Ajoute le nom du fichier dans l'événement
agent.sources.src1.channels   = ch1

# CHANNEL : tampon mémoire
agent.channels.ch1.type                = memory
agent.channels.ch1.capacity            = 10000  # Max 10000 événements en mémoire
agent.channels.ch1.transactionCapacity = 1000   # Max 1000 par transaction

# SINK : HDFS
agent.sinks.sink1.type              = hdfs
agent.sinks.sink1.hdfs.path         = hdfs://namenode:9000/data/flume_logs
agent.sinks.sink1.hdfs.fileType     = DataStream  # Format brut (pas SequenceFile)
agent.sinks.sink1.hdfs.writeFormat  = Text
agent.sinks.sink1.hdfs.rollInterval = 30  # Rotation de fichier toutes les 30 secondes
agent.sinks.sink1.hdfs.rollSize     = 0   # Pas de rotation par taille
agent.sinks.sink1.hdfs.rollCount    = 0   # Pas de rotation par nombre de lignes
agent.sinks.sink1.channel           = ch1
```

**Pourquoi `rollInterval = 30` ?**  
HDFS n'est pas optimisé pour les petits fichiers. Flume accumule les événements pendant 30 secondes, puis les écrit en un seul fichier HDFS. C'est un paramètre de tuning important pour éviter le "small files problem" de Hadoop.

**Fichier :** `flume/Dockerfile`

```dockerfile
FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

# Installation de Flume 1.11.0
RUN wget apache-flume-1.11.0-bin.tar.gz → /opt/flume

# Copie des JARs Hadoop nécessaires au HDFS sink
RUN cp /opt/hadoop-3.2.1/share/hadoop/hdfs/hadoop-hdfs-*.jar /opt/flume/lib/
RUN cp /opt/hadoop-3.2.1/share/hadoop/common/hadoop-common-*.jar /opt/flume/lib/
RUN cp /opt/hadoop-3.2.1/share/hadoop/common/lib/hadoop-auth-*.jar /opt/flume/lib/
...

# Remplacement de Guava 11 (Flume) par Guava 27 (Hadoop 3.x)
RUN rm -f /opt/flume/lib/guava-11.0.2.jar
RUN cp /opt/hadoop-3.2.1/share/hadoop/common/lib/guava-27.0-jre.jar /opt/flume/lib/
```

**Problème Guava :**  
Flume 1.11.0 livre Guava 11.0.2. Hadoop 3.x nécessite Guava 27+. En cas de conflit, le HDFS sink plante. Solution : remplacer le JAR Guava de Flume par celui de Hadoop.

---

### 6.7 Docker — Containerisation

**Fichier :** `docker-compose.yml`

Structure complète avec les dépendances entre services :
```yaml
services:
  zookeeper:         # Pas de dépendance
  kafka:             # depends_on: zookeeper
  namenode:          # Pas de dépendance
  datanode:          # depends_on: namenode
  producer:          # depends_on: kafka
  spark:             # depends_on: namenode
  mysql:             # Pas de dépendance
  sqoop:             # depends_on: namenode, mysql
  flume:             # depends_on: namenode
```

**Volumes importants :**
```yaml
mysql:
  volumes:
    - ./mysql:/docker-entrypoint-initdb.d  # init.sql auto-exécuté
    - ./data:/var/lib/mysql-files          # CSV accessible par LOAD DATA INFILE

flume:
  volumes:
    - ./logs:/logs                          # Dossier de logs monté dans le conteneur
```

---

## 7. Pipeline Complet End-to-End

Voici le déroulement complet dans l'ordre chronologique :

### Étape 1 — Démarrage des services
```
docker compose up -d --build
```
- Tous les 9 conteneurs démarrent
- MySQL exécute init.sql → charge 2512 transactions depuis transactions.csv
- Kafka Producer démarre et commence à envoyer des transactions au topic "transactions"

### Étape 2 — Sqoop Import (MySQL → HDFS)
```
Sqoop codegen + sqoop import --table transactions --target-dir /data/sqoop_import
```
- Sqoop se connecte à MySQL via JDBC
- Lit les 2512 lignes de la table `transactions`
- Écrit un fichier CSV dans HDFS `/data/sqoop_import/part-m-00000` (355 KB)

### Étape 3 — Analyse Spark (HDFS → HDFS)
```
spark-submit /app/fraud_export.py
```
- Spark lit `part-m-00000` depuis HDFS
- Charge 2512 transactions dans un DataFrame distribué
- Applique les 3 règles de détection de fraude
- Filtre : 461 transactions suspectes sur 2512 (18.3%)
- Écrit les résultats dans HDFS `/data/fraud_alerts/`

### Étape 4 — Sqoop Export (HDFS → MySQL)
```
sqoop export --table fraud_alerts --export-dir /data/fraud_alerts
```
- Sqoop lit les fichiers CSV depuis HDFS `/data/fraud_alerts/`
- Insère les 461 lignes dans la table MySQL `fraud_alerts`
- Vérification : `SELECT COUNT(*) FROM fraud_alerts;` → 461

### Étape 5 — Collecte de Logs (Flume)
```
Démarrage agent Flume + création de fichiers .log dans ./logs/
```
- L'agent Flume surveille le dossier `/logs` (monté depuis `./logs`)
- Quand un fichier `.log` apparaît, Flume l'ingère ligne par ligne
- Après `rollInterval=30s`, Flume écrit les événements dans HDFS `/data/flume_logs/`
- Le fichier traité est renommé `.log.COMPLETED` (garantie exactly-once)

---

## 8. Résultats et Analyse

### Résultats Quantitatifs

| Métrique | Valeur |
|----------|--------|
| Total transactions analysées | **2 512** |
| Transactions frauduleuses détectées | **461** |
| Taux de fraude | **18.3%** |
| Transactions légitimes | **2 051 (81.7%)** |

### Répartition des Fraudes par Critère

| Règle de détection | Description |
|-------------------|-------------|
| `TransactionAmount > 1000` | Montant anormalement élevé |
| `LoginAttempts > 3` | Trop de tentatives de connexion suspectes |
| `TransactionDuration > 200s` | Transaction anormalement longue (potentiel skimming) |

> Note : Une transaction peut déclencher plusieurs règles simultanément. Spark applique un `OR` logique, donc une transaction suspecte sur au moins un critère est incluse dans le résultat.

### Exemples de Fraudes Détectées

```
TX000027  AC00441  246.93  ATM    LoginAttempts=5    → Suspect (tentatives)
TX000008  AC00069  171.42  Branch TransactionDuration=291 → Suspect (durée)
TX000030  AC00313   56.17  Branch TransactionDuration=283 → Suspect (durée)
```

### Données dans HDFS après Pipeline Complet

```
/data/sqoop_import/part-m-00000   → 355 KB (2512 transactions)
/data/fraud_alerts/part-*.csv     → ~65 KB (461 fraudes)
/data/flume_logs/FlumeData.*      → Variable (logs d'alertes)
```

---

## 9. Difficultés Rencontrées et Solutions

### Problème 1 — Debian Stretch EOL dans les images Hadoop

**Contexte :** Les images `bde2020/hadoop-*` sont basées sur Debian Stretch (2017), dont les dépôts ont été déplacés vers `archive.debian.org`.

**Erreur :**
```
E: Failed to fetch http://deb.debian.org/debian/dists/stretch/main/...
   404 Not Found
```

**Solution :** Rediriger les dépôts dans les Dockerfiles avant tout `apt-get` :
```dockerfile
RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i 's|http://security.debian.org|http://archive.debian.org|g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list
```

---

### Problème 2 — commons-lang manquant dans Sqoop + Hadoop 3.x

**Contexte :** Sqoop 1.4.7 utilise `commons-lang` v2. Hadoop 3.x ne livre que v3 (`commons-lang3`). L'API a changé entre v2 et v3 (package différent).

**Erreur :**
```
java.lang.NoClassDefFoundError: org/apache/commons/lang/StringUtils
```

**Solution :** Télécharger `commons-lang-2.6.jar` dans le Dockerfile de Sqoop :
```dockerfile
RUN wget "https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar" \
    -O /opt/sqoop/lib/commons-lang-2.6.jar
```

---

### Problème 3 — Sqoop ClassNotFoundException avec Hadoop 3.x

**Contexte :** Sqoop génère du code Java et compile une classe (ex: `transactions.java`). Cette classe doit être dans le CLASSPATH de MapReduce pour l'import. Avec Hadoop 3.x, il y a une rupture dans la résolution du classpath.

**Erreur :**
```
java.lang.ClassNotFoundException: Class transactions not found
```

**Solution en deux étapes :**
1. `sqoop codegen` : Pré-compiler la classe et obtenir le JAR
2. Exporter `HADOOP_CLASSPATH` pour inclure ce JAR avant l'import :

```bash
SQOOP_JAR=$(ls /tmp/sqoop-root/compile/*/transactions.jar | head -1)
SQOOP_DIR=$(dirname $SQOOP_JAR)
export HADOOP_CLASSPATH="${SQOOP_DIR}/:${HADOOP_CLASSPATH}"
sqoop import ...
```

---

### Problème 4 — Guava version conflict dans Flume

**Contexte :** Flume 1.11.0 livre Guava 11.0.2. Le HDFS sink utilise l'API Hadoop 3.x qui requiert Guava 27+. Conflit de version au classpath.

**Erreur :**
```
java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument
```

**Solution :** Remplacer Guava dans le Dockerfile Flume :
```dockerfile
RUN rm -f /opt/flume/lib/guava-11.0.2.jar && \
    cp /opt/hadoop-3.2.1/share/hadoop/common/lib/guava-27.0-jre.jar /opt/flume/lib/
```

---

### Problème 5 — HDFS Permission Denied pour Spark

**Contexte :** Le conteneur Spark tourne en tant qu'utilisateur `spark`. HDFS `/data/` appartient à `root`. Spark ne peut pas écrire.

**Erreur :**
```
org.apache.hadoop.security.AccessControlException: 
Permission denied: user=spark, access=WRITE, inode="/data":root:supergroup:drwxr-xr-x
```

**Solution :**
```bash
docker exec namenode hdfs dfs -chmod 777 /data
```

---

### Problème 6 — LOAD DATA INFILE dans MySQL

**Contexte :** MySQL restreint `LOAD DATA INFILE` au répertoire `/var/lib/mysql-files/` pour des raisons de sécurité.

**Solution :** Monter `./data` sur ce chemin dans docker-compose.yml :
```yaml
mysql:
  volumes:
    - ./data:/var/lib/mysql-files
```

---

## 10. Conclusion

### Ce qui a été réalisé

Ce projet implémente une **architecture Big Data complète et fonctionnelle** pour la détection de fraude bancaire. Toutes les technologies requises par l'énoncé ont été intégrées :

| Technologie | Statut | Rôle |
|-------------|--------|------|
| Apache Kafka | Fonctionnel | Streaming temps réel des transactions |
| Apache HDFS | Fonctionnel | Stockage distribué central |
| Apache Sqoop | Fonctionnel | Import MySQL→HDFS + Export HDFS→MySQL |
| Apache Spark | Fonctionnel | Détection de fraude sur 2512 transactions |
| Apache Flume | Fonctionnel | Collecte de logs vers HDFS |
| MySQL | Fonctionnel | Source de données relationnelle |
| Docker | Fonctionnel | Containerisation des 9 services |

### Résultat Principal
Sur **2 512 transactions bancaires**, le système a détecté **461 transactions frauduleuses** (18.3%) en appliquant des règles sur le montant, les tentatives de connexion et la durée des transactions.

### Pistes d'Amélioration

1. **Machine Learning avec Spark MLlib** : Remplacer les règles fixes par un modèle de classification (Random Forest, Gradient Boosting) entraîné sur des données historiques labellisées

2. **Kafka Consumer** : Ajouter un consommateur Kafka qui lit le topic `transactions` et déclenche la détection en temps réel (stream processing avec Spark Structured Streaming)

3. **Dashboard de monitoring** : Intégrer Grafana + InfluxDB pour visualiser les fraudes en temps réel

4. **Hive** : Ajouter une couche de métastore Hive sur HDFS pour requêter les données avec du SQL standard

5. **Cluster YARN** : Déployer Spark sur YARN (Resource Manager Hadoop) pour un vrai traitement distribué multi-nœuds

6. **Réplication HDFS** : Passer le facteur de réplication à 3 (standard production) avec plusieurs DataNodes

---

*Rapport généré dans le cadre du projet Big Data — Polytech International 2026*
