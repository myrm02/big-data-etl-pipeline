# Projet Pipeline ETL (Big Data)
Projet d'amélioration de pipeline ETL dans le cadre d'un cours de Big Data avec Vicent Vidal.

## Setup du projet

1. Initialisation environnement docker :
   ```bash
   docker compose up -d
   ```

2. Initialisation environnement virtuel python :

L'environnement doit être initialisé au niveau de la racine du projet via cette commande : 
   ```bash
   py -3.14 -m venv venv
   ```
   Pour activer l'environnement, veuillez vous référez à cette documentation pour avoir la commande à entrer selon votre système d'exploitation : 

3. Installation des dépendances :

Après avoir activé l'environnement virtuel, veuillez installer les dépendances nécessaires via cette commande :
   ```bash
   pip install -r requirements.txt
   ```
4. Génération des fichiers :

Dans le dossier *files_generation*, vous trouverez tous les fichiers csv qui serviront de tests pour tester la pipeline ETL et ces différents acheminement afin d'évaluer les différents résultats provenant de chaque étape (bronze, silver et gold).

Voici une présentation des fichiers pour leurs usage dans les tests qui suivront :

    - **generate_data.py** : Deux fichiers CSV basiques avec des données standard (tous les layers)
    
    - **generate_data_new_items_test.py** : Un fichier CSV avec des ajouts de données additionnels au fichier *generate_data.py* pour évaluer la flexibilité d'ingestion de nouvelles données (layer bronze et silver)
    
    - **generate_data_additional_random_test.py** : Deux fichiers CSV bonus pour tester la flexibilité de l'ingestion de nouvelle données si y a plusieurs fichiers principalement, séparation des données liés aux achats et aux clients du fichier initial *generate_data_new_items_test.py* (layer bronze et silver)
    
    - **generate_data_null_missing_test.py** : Un fichiers CSV avec des valeurs manquantes pour le nettoyage et structuration des données (layer silver)
    
    - **generate_data_quality_test.py** : Un fichiers CSV avec des données potentiellement pour les résultats d'analyse staistiques et additionnels finaux (layer gold)

4. Génération des fichiers :

Dans le dossier *flow*, vous trouverez toutes les layer de la pipeline ETL.

Voici un guide que vous pouvez suivre pour tester les différentes étapes de la pipeline par layer (classé par ordre de layering):

    - **bronze_ingestion.py** : Upload des fichiers sources *(files_generation)* dans un bucket *BUCKET_BRONZE*
    
    - **silver_transform.py** : Traitement et nettoyage de la donnée pour garantir une qualité et une version des plus optimale des données d'entrainement d'analyse data à partir de la donnée accessible dans *BUCKET_BRONZE*
    
    - **gold_analytics.py** : Présentation des résultats d'analyse réalisé à partir de la donnée mis au propre et mis à jour dans le layer silver *(BUCKET_SILVER)*
   

Pour tester les différentes étapes de la pipeline par layer, voici un guide que vous pouvez suivre pour vous aider avec des comandes en guise d'exemple (classé par ordre de layering):  

   ```bash
   python generate_data.py
   python bronze_ingestion.py
   ```

   ```bash
   python silver_transform.py
   ```

   ```bash
   python gold_analytics.py
   ```

Ensuite, vous essayez les mêmes commandes avec un autre fichier du dossier *files_generation* lors de l'activation du flow du layer bronze.
