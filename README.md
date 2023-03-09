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
  - Nombre de visiteur

- Data mart : Utilisateurs au sein de l'applis
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

sbt run -J-Xmx4G
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
