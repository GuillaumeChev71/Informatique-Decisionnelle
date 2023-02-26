import java.util.Properties

class Connexion(private var typeConnexion:String) {
  // Class variables
  var connectionProperties: Properties = new Properties()
  var url: String = ""
  var username: String = ""
  var password: String = ""


  if(typeConnexion.equals("Oracle")){
    url = "jdbc:oracle:thin:@stendhal.iem:1521:ENSS2022"
    connectionProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    username = "ch098407"
    password = "ch098407"
  }else{
    url = "jdbc:postgresql://stendhal:5432/tpid2020"
    connectionProperties.setProperty("driver", "org.postgresql.Driver")
    username = "tpid"
    password = "tpid"
  }

  //Initialisation des variables
  connectionProperties.setProperty("user", username)
  connectionProperties.setProperty("password", password)

}