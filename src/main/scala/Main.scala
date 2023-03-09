import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._


object Main {
  def main(args: Array[String]): Unit = {

    val connPostgres: Connexion = new Connexion("Postgres")
    val connOracle: Connexion = new Connexion("Oracle")

    //Appel du dialect
    val dialect = new OracleDialect
    JdbcDialects.registerDialect(dialect)


    //Initialisation de Spark
    val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()


    /**************** CSV ****************/

    val fichierCSV = "yelp_academic_dataset_tip.csv"

    //Chargement du fichier CSV
    var tips = spark.read.format("csv").option("header", "true").load(fichierCSV)

    //Extraction des user_id
    var tip = tips.select("business_id", "compliment_count", "date", "user_id")

    //Insertion des données
    tip.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "tip", connOracle.connectionProperties)

    /*******************************************/





    /**************** POSTGRES ****************/
    val users = spark.read.jdbc(connPostgres.url, "yelp.user", connPostgres.connectionProperties)

    val dimensionUtilisateur = users.select("user_id", "name")

    //Insertion des données
    dimensionUtilisateur.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "utilisateur", connOracle.connectionProperties)



    val reviews = spark.read.jdbc(connPostgres.url, "yelp.review", connPostgres.connectionProperties)

    val review = users.select("user_id", "name")

    //Insertion des données
    user.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "utilisateur", connOracle.connectionProperties)

    //val donnees = df.select(col("date")).show()


    /*spark.read
      .option("partitionColumn", "yelping_since")
      .option("lowerBound", "2004-10-12")
      .option("upperBound", "2019-12-13")
      .option("numPartitions", 50)
      .jdbc(conn.url, "yelp.\"user\"", conn.connectionProperties)*/

    /*******************************************/





    /*
    //Initialisation de Spark
    val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

    val businessFile = "yelp_academic_dataset_business.json"

    //Chargement du fichier JSON
    var businesses = spark.read.json(businessFile).cache()

    // Extraction des amis, qui formeront une table "user_id - friend_id"
    var business = businesses.select("business_id", "name")

    // Suppression de la colonne "business" dans le DataFrame users
    //businesses = businesses.drop(col("business"))

    val conn: ConnexionOracle = new ConnexionOracle()

    // Enregistrement du DataFrame users dans la table "business"
    business.write.mode(SaveMode.Overwrite).jdbc(conn.retournerUrl(), "business", conn.seConnecter())*/


    spark.stop()
  }
}


