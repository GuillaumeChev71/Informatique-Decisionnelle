
object Main {
  def main(args: Array[String]): Unit = {

    val conn: Connexion = new Connexion("Oracle")
    println(conn.url)

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
  }
}