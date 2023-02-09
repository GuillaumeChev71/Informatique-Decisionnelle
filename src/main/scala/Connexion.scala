import java.util.Properties

class Connexion {

  // Class variables
  var connectionProperties:Properties = new Properties()
  var url: String = "jdbc:oracle:thin:@eluard.iem:1521:ENSE2022"
  var username: String = "ch098407"
  var password: String = "ch098407"

  // Charger les pilotes
  def seConnecter(): Properties = {
    //Class.forName("org.oracle.Driver") //Charger les pilotes
    //Class.forName("oracle.jdbc.driver.OracleDriver")//Charger les pilotes
    Class.forName("org.postgresql.Driver")

    connectionProperties.setProperty("driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user", username)
    connectionProperties.setProperty("password", password)
    connectionProperties
  }

  def retournerUrl(): String = {
    url
  }

}