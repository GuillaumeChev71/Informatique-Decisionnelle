Data warehouse :

- Data mart : Commerce
  - Analyse secteur activite (horaire d'ouverture)
  - nb commentaire
  - moyenne etoile
  - nombre de visite
  - Par geographie aussi
  - notation pas rapport au menu
  - Notation si ils ont un parking
  - Nombre de visiteur par mois
  - Restaurant les plus populaire par ville
  - Le nombre de nouveau avis pour chaque restaurant chaque mois
  - Les utilisateurs les plus affluent
  - peut-on distinguer une évolution temporelle des notes ?
  - quels sont les commerces dont les notes augmentent le plus ?
  - quels types de commerces sont les plus notés, les mieux notés, les moins bien notés
  - les caractéristiques annexes comme les menus végétariens dans les restaurants influent-elles sur la notation ?



- Data mart : Utilisateurs au sein de l'applis
  - Utilisateur qui visite souvent TOP 10
  - Utilisateur qui commentent souvent TOP 10
  - Utilisateurs qui ont le plus de réaction à leurs commentaire TOP 10
  - Le nombre d'arrivé des utilisateurs par années par mois, jours etc
  - Type de commentaires drole ou cool ou utile le plus sur le site
  - Type de compliment le plus reçu
  - Nombre d'amis influent sur le nombre de commentaire
  - pareil pour les fans
  - Pour chaque personne on prend le max des 3 type de vote (nbVoteFunny, nbVoteCool et nbVoteUseful) pour savoir lequel représente le plus la personne et on compte le nombre de commentaire par type de personne 

Variable d'environnement du projet :
```shell
export JAVA_HOME=/usr/gide/jdk-1.8.0_261
export PATH=usr/gide/jdk-1.8.0_261/bin:$PATH

export PATH=/usr/gide/sbt-1.3.13/bin:$PATH
```


Compilation du projet :
```shell
    sbt clean compile
```

Execution du projet :
```shell
sbt run

sbt run -J-Xmx8G
```


Pour Metabase :

Voir les versions
```shell
ls /usr/gide
```

Aller dans le bashrc pour changer les variables d'environnement :
```shell
gedit ~/.bashrc
```

Taper i
Rajouter les lignes suivantes à la fin :
```shell
export PATH=/usr/gide/jdk-12.0.2/bin:$PATH
export JAVA_HOME=/usr/gide/jdk-12.0.2
```

La ou est placé le jar créer un dossier "plugins" et y mettre le jar ojdbc

Lancer Metabase:
```shell
java -jar metabase.jar
```

Aller à l'adresse suivante :
http://localhost:3000/setup


mdp metabase :
```
ClementGuillaumeMathias3
```


Réinitialiser BD sur metabase : Paramètre -> Réglage administrateur -> Base de données -> clique sur la base -> synchroniser le schéma de base de données 



