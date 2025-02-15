# Blood_presure_monitoring_master-
Système de Surveillance des Données de Pression Artérielle avec Kafka, Elasticsearch et Kibana

Introduction

Ce projet vise à développer un système de surveillance des données de pression artérielle en temps réel à l'aide d'une architecture distribuée basée sur Kafka, Elasticsearch et Kibana. Le système gère les lectures de pression artérielle, les classe selon des seuils médicaux prédéfinis et stocke différemment les valeurs normales et anormales.

###Objectifs

#Objectif principal

Générer et transmettre des lectures de pression artérielle en temps réel.

Identifier et classifier ces lectures en fonction des seuils médicaux.

Sauvegarder les données normales localement.

Indexer les données anormales dans Elasticsearch pour une visualisation avancée via Kibana.

#Objectifs spécifiques

Implémenter un Producer Kafka pour générer des données conformes au standard FHIR (Fast Healthcare Interoperability Resources).

Développer un Consumer Kafka pour analyser et classifier les lectures.

Stocker les données normales localement et indexer les anomalies dans Elasticsearch.

Configurer Kibana pour visualiser les anomalies.


 1. Génération des Données

Le Producer génère des messages FHIR JSON contenant les valeurs systolique et diastolique.

Les messages sont publiés sur Kafka dans le topic blood_pressure_topic.

 2. Transmission des Données

Kafka transmet les messages du Producer au Consumer en temps réel.

 3. Analyse des Données

Le Consumer analyse les données pour détecter les anomalies.

Les données normales sont sauvegardées en JSON localement.

Les données anormales sont indexées dans Elasticsearch.

 4. Visualisation des Données

Kibana permet d'afficher les anomalies sous forme de tableaux de bord interactifs.

#Configuration de Kafka

Producer Configuration
--
bootstrap_servers: "kafka:9092"
value_serializer: JSON
Topic: "blood_pressure_topic"
--
Consumer Configuration
--
bootstrap_servers: "kafka:9092"
value_deserializer: JSON
auto_offset_reset: "earliest"
enable_auto_commit: True
--
Modèle d'Index Elasticsearch

Les données anormales sont stockées dans l'index blood_pressure_anomalies avec les champs suivants :
--
patient_id : Identifiant unique du patient.
--
timestamp : Date et heure de la mesure.
--
systolic_pressure : Valeur de la pression systolique.
--
diastolic_pressure : Valeur de la pression diastolique.
--
category : Type d'anomalie (ex. hypertension, hypotension).
--
location : Coordonnées GPS de la mesure.
--
Tableau de Bord Kibana

Visualisation interactive des anomalies.

Analyse des tendances et motifs des données.

Identification rapide des patients à risque.

#Conclusion

Ce système illustre l'utilisation de Kafka, Elasticsearch et Kibana dans un contexte de surveillance médicale distribuée. Il offre une solution robuste pour analyser et visualiser en temps réel les données de pression artérielle avec des perspectives d'extension à d'autres applications médicales.

#Installation & Exécution

Cloner ce repository :
--
git clone https://github.com/votre-utilisateur/nom-du-repository.git
--
Lancer les services avec Docker Compose :
--
docker-compose up -d
--
Accéder à Kibana via
--
http://localhost:5601
--
