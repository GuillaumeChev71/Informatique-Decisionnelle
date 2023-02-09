Data warehouse :

- Data mart : Commerce
    - Analyse secteur activite (horaire d'ouverture)
    - nb commentaire
    - moyenne etoile
    - nbClient
    - Tout ça part jour par semaine par mois
    - Par geographie aussi
    - notation pas rapport au menu
    - Notation si ils ont un parking

- Data mart : Utilisateur
    - Utilisateur qui visite souvent
    - Utilisateur qui commentent souvent
    - Utilisateurs qui ont le plus de réaction à leurs commentaire
    - nombre d'amis
    - Le nombre d'arrivé des utilisateurs par années par mois, jours etc
    - Pourcentage entre des commentaires drole ou cool
    - Type de compliment le plus reçu
    - Nombre de fan

Variable d'environnement du projet : 
```shell
export JAVA HOME=/usr/gide/jdk-1.8
export PATH=usr/gide/jdk-1.8/bin:$PATH

export PATH=/usr/gide/sbt-1.3.13/bin:$PATH
```

Compilation du projet : 
```shell
sbt clean compile
```

Execution du projet : 
```shell
sbt run
```


