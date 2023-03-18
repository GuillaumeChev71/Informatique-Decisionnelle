import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column


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

    /*******************************************/



    /**************** JSON ****************/

    val businessFile = "yelp_academic_dataset_business.json"
    val checkinFile = "yelp_academic_dataset_checkin.json"

    // Chargement des fichier JSON
    var businesses = spark.read.json(businessFile).cache()
    var checkin = spark.read.json(checkinFile).cache()

    /*******************************************/


    /**************** POSTGRES ****************/
    val reviews = spark.read.jdbc(connPostgres.url, "yelp.review", connPostgres.connectionProperties)

    val review = reviews.select("business_id", "date", "funny", "stars", "cool", "useful", "user_id")

    /*******************************************/



    // Jointure tip et business
    var jointureBusinessTip = businesses
      .join(tip, businesses("business_id") === tip("business_id"), "inner")
      .select(
        businesses("business_id"),
        concat(businesses("city"), lit(" - "), businesses("postal_code")).alias("city"),
        tip("compliment_count"),
        date_format(tip("date"), "yyyy-MM-dd").alias("date")
      )

    // Aggregation Tip
    var aggregationTip = jointureBusinessTip.groupBy("business_id", "city", "date").agg(
      sum("compliment_count").as("nbCompliment"),
      avg("compliment_count").as("moyenneCompliment"),
      count("compliment_count").as("nbTip")
    ).withColumn("nbCommentaire", lit(null))
      .withColumn("moyenneStars", lit(null))
      .withColumn("nbVoteFunny", lit(null))
      .withColumn("nbVoteCool", lit(null))
      .withColumn("nbVoteUseful", lit(null))
      .withColumn("moyenneVoteFunny", lit(null))
      .withColumn("moyenneVoteCool", lit(null))
      .withColumn("moyenneVoteUseful", lit(null))
      .withColumn("nbVisite", lit(null))



    // Jointure review et business
    var jointureBusinessReview = businesses
      .join(review, businesses("business_id") === review("business_id"), "inner")
      .select(
        businesses("business_id"),
        concat(businesses("city"), lit(" - "), businesses("postal_code")).alias("city"),
        review("useful"),
        review("funny"),
        review("cool"),
        review("stars"),
        review("date")
      )

    // Aggregation Review
    var aggregationReview = jointureBusinessReview.groupBy("business_id", "city", "date").agg(
      count("stars").as("nbCommentaire"),
      avg("stars").as("moyenneStars"),
      sum("funny").as("nbVoteFunny"),
      sum("cool").as("nbVoteCool"),
      sum("useful").as("nbVoteUseful"),
      avg("funny").as("moyenneVoteFunny"),
      avg("cool").as("moyenneVoteCool"),
      avg("useful").as("moyenneVoteUseful")
    ).withColumn("nbCompliment", lit(null))
      .withColumn("moyenneCompliment", lit(null))
      .withColumn("nbTip", lit(null))
      .withColumn("nbVisite", lit(null))


    // Extraction des données sur les Checkin
    val recupCheckin = checkin.withColumn("date", explode(org.apache.spark.sql.functions.split(col("date"), ",")))

    val debutCheckin = recupCheckin.select(col("business_id"), col("date"))

    // Jointure checkin et business
    var jointureBusinessCheckin = businesses
      .join(debutCheckin, businesses("business_id") === debutCheckin("business_id"), "inner")
      .select(
        businesses("business_id"),
        concat(businesses("city"), lit(" - "), businesses("postal_code")).alias("city"),
        date_format(debutCheckin("date"), "yyyy-MM-dd").alias("date")
      )

    // Aggregation Checkin
    var aggregationCheckin = jointureBusinessCheckin.groupBy("business_id", "city", "date").agg(
      count("date").as("nbVisite")
    ).withColumn("nbCommentaire", lit(null))
      .withColumn("moyenneStars", lit(null))
      .withColumn("nbVoteFunny", lit(null))
      .withColumn("nbVoteCool", lit(null))
      .withColumn("nbVoteUseful", lit(null))
      .withColumn("moyenneVoteFunny", lit(null))
      .withColumn("moyenneVoteCool", lit(null))
      .withColumn("moyenneVoteUseful", lit(null))
      .withColumn("nbCompliment", lit(null))
      .withColumn("moyenneCompliment", lit(null))
      .withColumn("nbTip", lit(null))


    //Union entre aggregationReview et aggregationTip
    var unionTipAndReview = aggregationTip.union(aggregationReview)


    //Union entre unionTipAndReview et aggregationCheckin
    var unionTipReviewAndCheckin = unionTipAndReview.union(aggregationCheckin)


    //Regroupement des données pour supprimer les doublons sur les dimensions
    var tipReviewAnCheckinSansDoublon = unionTipReviewAndCheckin.groupBy("business_id", "city", "date").agg(
      sum("nbVisite").as("nbVisite"),
      sum("nbCommentaire").as("nbCommentaire"),
      sum("moyenneStars").as("moyenneStars"),
      sum("nbVoteFunny").as("nbVoteFunny"),
      sum("nbVoteCool").as("nbVoteCool"),
      sum("nbVoteUseful").as("nbVoteUseful"),
      sum("moyenneVoteFunny").as("moyenneVoteFunny"),
      sum("moyenneVoteCool").as("moyenneVoteCool"),
      sum("moyenneVoteUseful").as("moyenneVoteUseful"),
      sum("nbCompliment").as("nbCompliment"),
      sum("moyenneCompliment").as("moyenneCompliment"),
      sum("nbTip").as("nbTip")
    )


    // Création de la dimension Time et récupération du mois, de la semaine et de l'année d'une date
    var dimensionTime = tipReviewAnCheckinSansDoublon.select(
      col("date"),
      year(col("date")).as("year"),
      month(col("date")).as("month"),
      weekofyear(col("date")).as("week")
    ).distinct()
      .withColumn("idTime", monotonically_increasing_id())


    // Jointure pour l'id du time
    var jointureForIdTime = tipReviewAnCheckinSansDoublon
      .join(dimensionTime, tipReviewAnCheckinSansDoublon("date") === dimensionTime("date"), "inner")
      .select(
        dimensionTime("idTime"),
        tipReviewAnCheckinSansDoublon("business_id"),
        tipReviewAnCheckinSansDoublon("city"),
        tipReviewAnCheckinSansDoublon("nbVisite"),
        tipReviewAnCheckinSansDoublon("nbCommentaire"),
        tipReviewAnCheckinSansDoublon("moyenneStars"),
        tipReviewAnCheckinSansDoublon("nbVoteFunny"),
        tipReviewAnCheckinSansDoublon("nbVoteCool"),
        tipReviewAnCheckinSansDoublon("nbVoteUseful"),
        tipReviewAnCheckinSansDoublon("moyenneVoteFunny"),
        tipReviewAnCheckinSansDoublon("moyenneVoteCool"),
        tipReviewAnCheckinSansDoublon("moyenneVoteUseful"),
        tipReviewAnCheckinSansDoublon("nbCompliment"),
        tipReviewAnCheckinSansDoublon("moyenneCompliment"),
        tipReviewAnCheckinSansDoublon("nbTip")
      )


    // Extraction des données géographiques
    var exctractionGeo = businesses.select(concat(businesses("city"), lit(" - "), businesses("postal_code")).alias("cityConcat"),col("city"), col("state"), col("postal_code").cast("int").as("cp")).distinct()
      .withColumn("idGeo", monotonically_increasing_id())


    // Jointure pour l'id du geo
    var FactTableBusiness = jointureForIdTime
      .join(exctractionGeo, jointureForIdTime("city") === exctractionGeo("cityConcat"), "inner")
      .select(
        exctractionGeo("idGeo"),
        jointureForIdTime("idTime"),
        jointureForIdTime("business_id"),
        jointureForIdTime("city"),
        jointureForIdTime("nbVisite"),
        jointureForIdTime("nbCommentaire"),
        jointureForIdTime("moyenneStars"),
        jointureForIdTime("nbVoteFunny"),
        jointureForIdTime("nbVoteCool"),
        jointureForIdTime("nbVoteUseful"),
        jointureForIdTime("moyenneVoteFunny"),
        jointureForIdTime("moyenneVoteCool"),
        jointureForIdTime("moyenneVoteUseful"),
        jointureForIdTime("nbCompliment"),
        jointureForIdTime("moyenneCompliment"),
        jointureForIdTime("nbTip")
      )

    var dimensionGeo = exctractionGeo.select(col("idGeo"), col("city"), col("state"), col("cp"))



    //Extraction des infos d'ouverture et créer une table de 5 colonnes : "idHoraire", "business_id", "type_jour", "heureOuverture" et "heureFermeture"
    var dimensionHoraireOuverture = businesses.selectExpr(
      "monotonically_increasing_id() as idHoraire",
      "business_id",
      "stack(7, " +
        s"'Monday', hours.Monday, " +
        s"'Tuesday', hours.Tuesday, " +
        s"'Wednesday', hours.Wednesday, " +
        s"'Thursday', hours.Thursday, " +
        s"'Friday', hours.Friday, " +
        s"'Saturday', hours.Saturday, " +
        s"'Sunday', hours.Sunday" +
        ") as (type_jour, heures)",
      "substring_index(heures, '-', 1) as heureOuverture",
      "substring_index(heures, '-', -1) as heureFermeture"
    ).drop("heures")


    // Extraction des données sur les Categories de commerces
    var recupCategorie = businesses.withColumn("categories", explode(org.apache.spark.sql.functions.split(col("categories"), ",")))

    // Exctraction des 2 colonnes nécessaire
    var debutDimensionCategories = recupCategorie.select(col("business_id").as("idCommerce"), col("categories").as("categorie"))

    //Récupération des 20 catégories les plus utilisées
    var categorieFamous = debutDimensionCategories.groupBy("categorie").count().orderBy(col("count").desc).limit(20).withColumn("idCategorie", monotonically_increasing_id())

    //Création de la dimension Categorie
    var dimensionCategories = debutDimensionCategories
      .join(categorieFamous, debutDimensionCategories("categorie") === categorieFamous("categorie"), "inner")
      .select(
        categorieFamous("idCategorie"),
        debutDimensionCategories("idCommerce"),
        categorieFamous("categorie")
      )

    //création de la dimension commerce
    var dimensionCommerce = businesses.select(col("business_id"), col("name"))


    /**************** ECRITURE DANS LA BDD ****************/
    // Dimension de temps
    dimensionTime.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "TIME", connOracle.connectionProperties)

    // Dimension géographique
    dimensionGeo.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "GEO", connOracle.connectionProperties)

    // Dimension Horaire Ouverture
    dimensionHoraireOuverture.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "HORAIRE_OUVERTURE", connOracle.connectionProperties)

    // Dimension Categorie
    dimensionCategories.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "CATEGORIE", connOracle.connectionProperties)

    // Dimension Commerce
    dimensionCommerce.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "COMMERCE", connOracle.connectionProperties)

    //Table de fait
    FactTableBusiness.write.mode(SaveMode.Overwrite).jdbc(connOracle.url, "FACT_BUSINESS", connOracle.connectionProperties)

    /*******************************************/


    spark.stop()
  }
}


