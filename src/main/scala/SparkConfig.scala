
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object SparkConfig extends App {

  Logger.getLogger("org").setLevel(Level.OFF)


  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
    .setAppName("SparkAssignment")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate

  val sc: SparkContext = sparkSession.sparkContext

  val pathFile = "src/main/resources/Football.csv"

  val footballData: DataFrame = sparkSession.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv(pathFile)

  footballData.createOrReplaceTempView("Football")

      ////TEAM PLAYED AS HomeTEAM
  val homeTeamData = sparkSession.sql("Select HomeTeam as Team,count(HomeTeam) as Total from Football group by HomeTeam ORDER BY count(HomeTeam) DESC")

  homeTeamData.createOrReplaceTempView("OnlyHomeTeam")

  val awayTeamData = sparkSession.sql("Select AwayTeam as Team,'0' as Total from Football where " +
    "AwayTeam NOT IN (SELECT Team FROM OnlyHomeTeam) group by AwayTeam")

  homeTeamData.union(awayTeamData).show()


  ////////TOP 10 TEAM WITH HIGH WINING PERCENTAGE
  val footballTeamWins = sparkSession.sql("Select Team as FootBallTeams, sum(Total) as TotalMatches from " +
    "(Select AwayTeam as Team,count(AwayTeam) as Total from Football group by AwayTeam " +
    "Union All Select HomeTeam as Team,count(HomeTeam) as Total from Football group by HomeTeam )" +
    " group by Team order by sum(Total)")

  footballTeamWins.createOrReplaceTempView("TotalMatchesPlayed")

  val FootballTeamWinsPercentage = sparkSession.sql("Select Team, sum(Total) as TotalMatchWins from " +
    "(Select AwayTeam as Team,count(AwayTeam) as Total from Football where FTR = 'A' group by AwayTeam " +
    "Union All Select HomeTeam as Team,count(HomeTeam) as Total from Football where FTR = 'H' group by HomeTeam )" +
    " group by Team order by sum(Total)")


  FootballTeamWinsPercentage.createOrReplaceTempView("TotalMatchWins")

  val topPerformer = sparkSession.sql("Select FootballTeams,((tw.TotalMatchWins/tp.TotalMatches)*100) as percentage from TotalMatchesPlayed tp " +
    ",TotalMatchWins tw where tp.FootBallTeams == tw.Team order by percentage DESC LIMIT 10")
  topPerformer.show()


  /////////////DATASET///////////////
  import sparkSession.implicits._
  val footballDS= footballData.map[FootBall]((row:Row)=>
  FootBall(row.getString(2),row.getString(3),row.getInt(4),row.getInt(5),row.getString(6)))
  footballDS.show()

  //Matches Played by Each Team
  footballDS.select("HomeTeam").union(footballDS.select("AwayTeam")).groupBy("HomeTeam").count().show()

  //
  footballDS.filter(row=>row.Win.equals("H")).groupBy("HomeTeam").count().union(footballDS.filter(row=>row.Win.equals("A"))
    .groupBy("AwayTeam").count()).groupBy("HomeTeam")
    .sum("count").sort(desc("sum(count)")).withColumnRenamed("sum(count)","TotalWiningMatches").limit(10).show()
  // Thread.sleep(100000)
}
