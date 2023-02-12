import java.util.Properties

class Connexion(private var typeConnexion:String) {
  // Class variables
  var connectionProperties: Properties = new Properties()
  var url: String = ""
  var username: String = "ch098407"
  var password: String = "ch098407"

  //Initialisation des variables
  connectionProperties.setProperty("user", username)
  connectionProperties.setProperty("password", password)

  if(typeConnexion.equals("Oracle")){
    url = "jdbc:oracle:thin:@stendhal.iem:1521:ENSS2022"
    connectionProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
  }else{
    url = "jdbc:postgresql://stendhal:5432/tpid2020"
    connectionProperties.setProperty("driver", "org.postgresql.Driver")
  }

}