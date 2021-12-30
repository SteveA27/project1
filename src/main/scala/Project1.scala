import org.apache.spark
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext, SparkSession}
import org.apache.spark.SparkContext
import scala.io.StdIn
import scala.util.parsing.json.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{explode, explode_outer}
import org.apache.spark.sql.SQLImplicits


object Project1 {
  /*def toJsonFormat(usrname:String, pwd: String, user_type: String): String= {
    val jsonString = {"username" : usrname}




    return jsonString
  }*/


  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("Project 1")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    import spark.sqlContext.implicits._

    val result = scala.io.Source.fromURL("https://api.sportsdata.io/v3/nfl/scores/json/Referees?key=096b30fe8c13415290f9b08db562adaf").mkString
    val jsonResponseLine = result.toString().stripLineEnd
    //val sc = new SparkContext()
    val rdd1  = spark.sparkContext.parallelize(jsonResponseLine::Nil)
    val df = spark.read.json(rdd1)
    //df.show()
    val NFLResult = scala.io.Source.fromURL("https://api.sportsdata.io/v3/nfl/scores/json/Players?key=096b30fe8c13415290f9b08db562adaf").mkString
    val jsonResponseLine2 = NFLResult.toString().stripLineEnd
    val NFLRDD = spark.sparkContext.parallelize(jsonResponseLine2::Nil)
    val NFLDF = spark.read.json(NFLRDD)
    //NFLDF.show()
    df.createOrReplaceTempView("RefTableTemp")
    NFLDF.createOrReplaceTempView("CurrentNFLTemp")
    spark.sql("Drop Table If Exists RefTable")
    spark.sql("Create Table RefTable as select * from RefTableTemp")
    spark.sql("Drop Table if exists CurrentNFL")
    spark.sql("Create Table CurrentNFL as select * from CurrentNFLTemp")
    spark.sql("Select * From RefTable").show()
    spark.sql("Select * from CurrentNFL").show()
    spark.sql("Select College From RefTable Where Name = 'Barry Anderson'").show()
    //df.printSchema()
    spark.sql("Select Name From CurrentNFL Where Birthdate like '1997%'").show()
    val answer = "North Carolina State"
    val correct_answer = spark.sql("Select College From RefTable Where Name = 'Barry Anderson'")
    val correct_answerRow = correct_answer.first.getString(0)
    //val collegeName = correct_answerRow(0)
    if( answer == correct_answerRow){
      println("Correct Answer")
    }
    else {
      println("Wrong Answer")
    }
    println(correct_answerRow)
    spark.sql("Drop table if exists Users")
    spark.sql("Create table users (username string, password string, user_type string)")
    /*val usrs = spark.sql("Select * From users")
    println("Enter a username: ")
    val new_username = readLine()
    println("Enter a password: ")
    val new_pwd = readLine()
    val new_usr_type = "Basic"
    //val json =
    val json1 = Seq((new_username, new_pwd, new_usr_type)).toDF("username", "password","user type")

    val new_df = usrs.union(json1)

    new_df.createOrReplaceTempView("UsersTemp")
    spark.sql("Select * From UsersTemp")
    new_df.coalesce(1).write.mode("overwrite").csv("C:\\input\\new_data.txt")
    val new_dfText = spark.read.csv("C:\\input\\new_data.txt")
    new_dfText
    spark.sql("Drop table if exists Users")
    spark.sql("Create table Users (username string, password string, user_type string)")
    val new_df3 = spark.sql("Select * From Users").union(new_dfText)
    //new_df3.show()*/
    var cont = true
    var cont1 = true
    var cont2 = true
    var adminCont = true
    var basicCont = true
    //var usrCont = true

    do{

      println(Console.BOLD)
      println("Welcome to NFL Questionare")
      println(Console.RESET)
      println("Please choose if you are an existing or new user")
      println("1: Create Account")
      println("2: Sign In")
      println("3: Quit")
      var usrInputNum = readLine().toInt
      if(usrInputNum == 1){
        println("Enter a username: ")
        val new_username1 = readLine()
        println("Enter a password: ")
        val new_pwd1 = readLine()
        println("Please enter your user type Basic or Admin: ")
        val new_usr_type1 = readLine()
        val new_df4 = Seq((new_username1, new_pwd1, new_usr_type1)).toDF("username", "password","user type")
        new_df4.write.mode("append").csv("C:\\input\\new_data.txt")
        var new_dfText1 = spark.read.csv("C:\\input\\new_data.txt")
        new_dfText1 = new_dfText1.withColumnRenamed("_c0", "username")
        new_dfText1 = new_dfText1.withColumnRenamed("_c1", "password")
        new_dfText1 = new_dfText1.withColumnRenamed("_c2", "user_type")
        new_dfText1.write.mode("append").format("hive").saveAsTable("Users")
      }
      if(usrInputNum == 2){
        println("Please enter your username: ")
        var usrName = readLine()
        println("Please enter your password: ")
        var usrPwd = readLine()
        val usrNameVal = spark.sql(s"Select username From Users Where username = '$usrName'")
        val usrPwdVal = spark.sql(s"Select password From Users Where password = '$usrPwd'")
        val usrTypeVal = spark.sql(s"Select user_type From Users Where username = '$usrName'")
        val usrNameV = usrNameVal.first.getString(0)
        val usrPwdV = usrPwdVal.first.getString(0)
        val usrTypeV = usrTypeVal.first.getString(0)
        if((usrName == usrNameV) && (usrTypeV == "Admin")&& (usrPwdV == usrPwd)){
          do{
            println(Console.BOLD)
            println("Admin Menu Options")
            println(Console.RESET)
            println("1: Answer NFL questions")
            println("2: Update user")
            println("3: View All users")
            println("4: Create user")
            println("5: Log Out")
            println("6: Delete user")

            println("Please enter a number choice")
            var inputSelected = readLine().toInt

            if(inputSelected == 1){
              do{
                println("Question 1")
                println("Question 2")
                println("Question 3")
                println("Question 4")
                println("Question 5")
                println("Question 6")
                println("7: Return to Main Menu")
                println("Please enter a number to answer a question or 7 to return to the main menu: ")
                var inputQuestion = readLine().toInt

                if(inputQuestion == 1){
                  println("Analytical Question 1: What is the name of the quarter back for the Arizona Cardinals: ")
                  println("A. Kyler Murray B. Derek Henry C. CeeDee Lamb D. Justin Jefferson")
                  val answer1 = readLine()
                  val ariQB = spark.sql("Select name from CurrentNFL Where Position = 'QB' and Team = 'ARI' and College = 'Oklahoma'")
                  val Question1 = ariQB.first.getString(0)
                  if(answer1 == Question1){
                    println("Correct Answer")
                  }
                  else{
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 2){
                  println("Analytical Questin 2: What college did famous NFL referee Gary Arthur attend: ")
                  println("Answer choices are: A. Pembroke State B. Illnois State C. Wright State D. UCF")
                  val answer2 = readLine()
                  val refCollege = spark.sql("Select College From RefTable Where Name = 'Gary Arthur'")

                  val Question2 = refCollege.first.getString(0)
                  if(answer2 == Question2){
                    println("Correct Answer")
                  }
                  else {
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 3){
                  println("Analytical Question 3: What season is all pro running back Deebo Samuel in?: ")
                  println("A. 3rd Season B. 6th Season C. Rookie D. 11th Season")
                  val answer3 = readLine()
                  val deebExp = spark.sql("Select ExperienceString From CurrentNFL Where Name = 'Deebo Samuel'")
                  val Question3 = deebExp.first.getString(0)
                  if(answer3 == Question3){
                    println("Correct Answer")
                  }
                  else {
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 4){
                  println("Analytical Question 4: How many players in the NFL currently attended Oklahoma in college?: ")
                  println("A. 100 B. 5 C. 62 D. 50")
                  val answer4 = readLine().toLong
                  val okNFL = spark.sql("Select count(*) From CurrentNFL Where College = 'Oklahoma'")
                  val Question4 = okNFL.first.getLong(0)
                  if(answer4 == Question4){
                    println("Correct Answer")
                  }
                  else{
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 5){
                  println("Analytical Question 5: How many players in the NFL currently was born in 1993?: ")
                  println("A. 6 B. 90 C. 287 D. 77")
                  val answer5 = readLine().toLong
                  val NFL93 = spark.sql("Select count(*) From CurrentNFL Where Birthdate like '1993%'")
                  val Question5 = NFL93.first.getLong(0)
                  if(answer5 == Question5){
                    println("Correct answer")
                  }
                  else {
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 6){
                  println("Analytical Question 6: How many referees in the NFL today have more than 4 years of experience?: ")
                  println("A. 11 B. 89 C. 63 D. 200")
                  val answer6 = readLine().toLong
                  val refExper = spark.sql("Select count(*) From RefTable Where Experience > 4")
                  val Question6 = refExper.first.getLong(0)
                  if(answer6 == Question6){
                    println("Correct answer")
                  }
                  else{
                    println("Wrong answer")
                  }
                }
                if(inputQuestion == 7){
                  cont1 = false
                }

              }while(cont1)
            }
            if(inputSelected == 4){
              println("Enter a username: ")
              val new_username1 = readLine()
              println("Enter a password: ")
              val new_pwd1 = readLine()
              val new_usr_type1 = "Basic"
              val new_df4 = Seq((new_username1, new_pwd1, new_usr_type1)).toDF("username", "password","user type")
              new_df4.write.mode("append").csv("C:\\input\\new_data.txt")
              var new_dfText1 = spark.read.csv("C:\\input\\new_data.txt")
              new_dfText1 = new_dfText1.withColumnRenamed("_c0", "username")
              new_dfText1 = new_dfText1.withColumnRenamed("_c1", "password")
              new_dfText1 = new_dfText1.withColumnRenamed("_c2", "user_type")
              new_dfText1.write.mode("append").format("hive").saveAsTable("Users")
            }
            if(inputSelected == 3){
              spark.sql("Select * From Users").show()
            }
            if(inputSelected == 2){

              do{
                println("1: Change Password")
                println("2: Change Username")
                println("3: Go Back to Main Menu")
                println("Please choose a number to update account: ")
                var updateUsrNum = readLine().toInt

                if(updateUsrNum == 1){
                  println("Enter your username: ")
                  var inputExistUsr = readLine()
                  val testdf =  spark.sql(s"Select username From Users Where username = '$inputExistUsr'")
                  val testdfString = testdf.first.getString(0)
                  //println(testdfString)

                  if(testdfString == inputExistUsr){
                    println("Please choose a new password: ")
                    var newUsrPwd = readLine()
                    val exUsrType = "Basic"
                    var new_updateUsrDF = Seq((inputExistUsr, newUsrPwd, exUsrType)).toDF("username", "password","user type")
                    var all_otherUsrs = spark.sql(s"Select * From Users Where username != '$inputExistUsr'")
                    var updated_df = all_otherUsrs.union(new_updateUsrDF)
                    updated_df.coalesce(1).write.mode("overwrite").csv("C:\\input\\new_data.txt")
                    var newUpdatedText = spark.read.csv("C:\\input\\new_data.txt")
                    newUpdatedText = newUpdatedText.withColumnRenamed("_c0", "username")
                    newUpdatedText = newUpdatedText.withColumnRenamed("_c1", "password")
                    newUpdatedText = newUpdatedText.withColumnRenamed("_c2", "user_type")
                    newUpdatedText.write.mode("overwrite").format("hive").saveAsTable("Users")
                  }

                }
                if(updateUsrNum == 3){
                  cont2 = false
                }


              }while(cont2)



            }
            if(inputSelected == 5){
              adminCont = false
            }
            if(inputSelected == 6){
              println("Enter your username: ")
              var inputExistUsr = readLine()
              val testdf =  spark.sql(s"Select username From Users Where username = '$inputExistUsr'")
              val testdfString = testdf.first.getString(0)
              if(testdfString == inputExistUsr){
                var all_otherUsrs = spark.sql(s"Select * From Users Where username != '$inputExistUsr'")
                all_otherUsrs.coalesce(1).write.mode("overwrite").csv("C:\\input\\new_data.txt")
                var newUpdatedText = spark.read.csv("C:\\input\\new_data.txt")
                newUpdatedText = newUpdatedText.withColumnRenamed("_c0", "username")
                newUpdatedText = newUpdatedText.withColumnRenamed("_c1", "password")
                newUpdatedText = newUpdatedText.withColumnRenamed("_c2", "user_type")
                newUpdatedText.write.mode("overwrite").format("hive").saveAsTable("Users")

              }
            }
          }while(adminCont)
        }
        else{
          do{
            println(Console.BOLD)
            println("Menu Options")
            println(Console.RESET)
            println("1: Answer NFL questions")
            println("2: Update account")
            println("3: View All users")
            println("4: Delete account")
            println("5: Log Out")


            println("Please enter a number choice")
            var inputSelected = readLine().toInt

            if(inputSelected == 1){
              do{
                println("Question 1")
                println("Question 2")
                println("Question 3")
                println("Question 4")
                println("Question 5")
                println("Question 6")
                println("7: Return to Main Menu")
                println("Please enter a number to answer a question or 7 to return to the main menu: ")
                var inputQuestion = readLine().toInt

                if(inputQuestion == 1){
                  println("Analytical Question 1: What is the name of the quarter back for the Arizona Cardinals: ")
                  println("A. Kyler Murray B. Derek Henry C. CeeDee Lamb D. Justin Jefferson")
                  val answer1 = readLine()
                  val ariQB = spark.sql("Select name from CurrentNFL Where Position = 'QB' and Team = 'ARI' and College = 'Oklahoma'")
                  val Question1 = ariQB.first.getString(0)
                  if(answer1 == Question1){
                    println("Correct Answer")
                  }
                  else{
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 2){
                  println("Analytical Questin 2: What college did famous NFL referee Gary Arthur attend: ")
                  println("Answer choices are: A. Pembroke State B. Illnois State C. Wright State D. UCF")
                  val answer2 = readLine()
                  val refCollege = spark.sql("Select College From RefTable Where Name = 'Gary Arthur'")

                  val Question2 = refCollege.first.getString(0)
                  if(answer2 == Question2){
                    println("Correct Answer")
                  }
                  else {
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 3){
                  println("Analytical Question 3: What season is all pro running back Deebo Samuel in?: ")
                  println("A. 3rd Season B. 6th Season C. Rookie D. 11th Season")
                  val answer3 = readLine()
                  val deebExp = spark.sql("Select ExperienceString From CurrentNFL Where Name = 'Deebo Samuel'")
                  val Question3 = deebExp.first.getString(0)
                  if(answer3 == Question3){
                    println("Correct Answer")
                  }
                  else {
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 4){
                  println("Analytical Question 4: How many players in the NFL currently attended Oklahoma in college?: ")
                  println("A. 100 B. 5 C. 62 D. 50")
                  val answer4 = readLine().toLong
                  val okNFL = spark.sql("Select count(*) From CurrentNFL Where College = 'Oklahoma'")
                  val Question4 = okNFL.first.getLong(0)
                  if(answer4 == Question4){
                    println("Correct Answer")
                  }
                  else{
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 5){
                  println("Analytical Question 5: How many players in the NFL currently was born in 1993?: ")
                  println("A. 6 B. 90 C. 287 D. 77")
                  val answer5 = readLine().toLong
                  val NFL93 = spark.sql("Select count(*) From CurrentNFL Where Birthdate like '1993%'")
                  val Question5 = NFL93.first.getLong(0)
                  if(answer5 == Question5){
                    println("Correct answer")
                  }
                  else {
                    println("Wrong Answer")
                  }
                }
                if(inputQuestion == 6){
                  println("Analytical Question 6: How many referees in the NFL today have more than 4 years of experience?: ")
                  println("A. 11 B. 89 C. 63 D. 200")
                  val answer6 = readLine().toLong
                  val refExper = spark.sql("Select count(*) From RefTable Where Experience > 4")
                  val Question6 = refExper.first.getLong(0)
                  if(answer6 == Question6){
                    println("Correct answer")
                  }
                  else{
                    println("Wrong answer")
                  }
                }
                if(inputQuestion == 7){
                  cont1 = false
                }

              }while(cont1)
            }
            /*if(inputSelected == 4){
              println("Enter a username: ")
              val new_username1 = readLine()
              println("Enter a password: ")
              val new_pwd1 = readLine()
              val new_usr_type1 = "Basic"
              val new_df4 = Seq((new_username1, new_pwd1, new_usr_type1)).toDF("username", "password","user type")
              new_df4.write.mode("append").csv("C:\\input\\new_data.txt")
              var new_dfText1 = spark.read.csv("C:\\input\\new_data.txt")
              new_dfText1 = new_dfText1.withColumnRenamed("_c0", "username")
              new_dfText1 = new_dfText1.withColumnRenamed("_c1", "password")
              new_dfText1 = new_dfText1.withColumnRenamed("_c2", "user_type")
              new_dfText1.write.mode("append").format("hive").saveAsTable("Users")
            }*/
            if(inputSelected == 3){
              spark.sql("Select * From Users").show()
            }
            if(inputSelected == 2){

              do{
                println("1: Change Password")
                println("2: Change Username")
                println("3: Go Back to Main Menu")
                println("Please choose a number to update account: ")
                var updateUsrNum = readLine().toInt

                if(updateUsrNum == 1){
                  println("Enter your username: ")
                  var inputExistUsr = readLine()
                  val testdf =  spark.sql(s"Select username From Users Where username = '$inputExistUsr'")
                  val testdfString = testdf.first.getString(0)
                  //println(testdfString)

                  if(testdfString == inputExistUsr){
                    println("Please choose a new password: ")
                    var newUsrPwd = readLine()
                    val exUsrType = "Basic"
                    var new_updateUsrDF = Seq((inputExistUsr, newUsrPwd, exUsrType)).toDF("username", "password","user type")
                    var all_otherUsrs = spark.sql(s"Select * From Users Where username != '$inputExistUsr'")
                    var updated_df = all_otherUsrs.union(new_updateUsrDF)
                    updated_df.coalesce(1).write.mode("overwrite").csv("C:\\input\\new_data.txt")
                    var newUpdatedText = spark.read.csv("C:\\input\\new_data.txt")
                    newUpdatedText = newUpdatedText.withColumnRenamed("_c0", "username")
                    newUpdatedText = newUpdatedText.withColumnRenamed("_c1", "password")
                    newUpdatedText = newUpdatedText.withColumnRenamed("_c2", "user_type")
                    newUpdatedText.write.mode("overwrite").format("hive").saveAsTable("Users")
                  }

                }
                if(updateUsrNum == 3){
                  cont2 = false
                }


              }while(cont2)



            }
            if(inputSelected == 5){
              basicCont = false
            }
            if(inputSelected == 4){
              println("Enter your username: ")
              var inputExistUsr = readLine()
              val testdf =  spark.sql(s"Select username From Users Where username = '$inputExistUsr'")
              val testdfString = testdf.first.getString(0)
              if(testdfString == inputExistUsr){
                var all_otherUsrs = spark.sql(s"Select * From Users Where username != '$inputExistUsr'")
                all_otherUsrs.coalesce(1).write.mode("overwrite").csv("C:\\input\\new_data.txt")
                var newUpdatedText = spark.read.csv("C:\\input\\new_data.txt")
                newUpdatedText = newUpdatedText.withColumnRenamed("_c0", "username")
                newUpdatedText = newUpdatedText.withColumnRenamed("_c1", "password")
                newUpdatedText = newUpdatedText.withColumnRenamed("_c2", "user_type")
                newUpdatedText.write.mode("overwrite").format("hive").saveAsTable("Users")

              }
            }
          }while(basicCont)
        }

      }
      if(usrInputNum == 3){
        cont = false
      }



    }while(cont)



    //spark.sql("Alter table UsersTemp Rename to users")
    //spark.sql("Select * From users").show()
    //println("Enter a username: ")
    //val new_username1 = readLine()
    //println("Enter a password: ")
    //val new_pwd1 = readLine()
    //val new_usr_type1 = "Basic"
    //spark.sql(s"Insert into users values ('${new_username1}', '${new_pwd1}', '${new_usr_type1}')")
    //spark.sql("Select * From users").show()
    //val json =
    //val json2 = Seq((new_username1, new_pwd1, new_usr_type1)).toDF("username", "password","user type")
    //json2.createOrReplaceTempView("User2Temp")
    //spark.sqlContext.sql("insert into users select * from User2Temp")
    //spark.sql("Select * From users").show()
    //val new_df2 = spark.sql(("Select * From users")).union(json2)
  // new_df2.show()
    //val json_string = json1.mkString
   //val jsonResponseLine3 = json_string.toString().stripLineEnd
    //println(jsonResponseLine3)
    //val rdd3  =  spark.sparkContext.parallelize(jsonResponseLine3::Nil)
    //val new_DF = spark.read.json(rdd3)
    //new_DF.show()
    //val newUsrs = usrs.union(new_DF)
    //newUsrs.show()
    //println("Pleaser enter your new username: ")
    //val usernameCreater = readLine()
    //println("Pleaser enter your new password: ")
    //val passwordCreater = readLine()
    //spark.sql("Insert Into users values(+usernameCreater+, +passwordCreater+)")


    //val dfRDD1 = df.select(explode(df("events"))).toDF("events")//(explode(df("events"))
    //dfRDD1.show()
    //dfRDD1.printSchema()
    //val dfRDD2 = dfRDD1.select(explode(dfRDD1("events"))).toDF().select(explode(dfRDD1.select(explode(dfRDD1("events"))).toDF()("competitions"))).toDF()
    //dfRDD2.show()
   // df.write.mode("append").saveAsTable("test_db1")
    //spark.sql("Select test_db1.events from test_db1").show()
    //spark.sql("DROP table IF EXISTS Test_db2")
    //spark.sql("Create table Test_db2(events string")
    //spark.sql("INSERT INTO Test_db2 (Select events From test_db1")
    //spark.sql("Select * from Test_db2").show()
    //df.show()
    //spark.sql("CREATE TABLE partition_data(name string, dept string, state string)row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'partFile.txt' INTO TABLE partition_data")
   // spark.sql("SELECT * From partition_data").show()

    //spark.sql("CREATE TABLE partitionTable(name string, dept string) PARTITIONED by (state string) row format delimited fields by ','")
    //spark.sql("INSERT INTO partitionTable(SELECT * FROM partition_data WHERE partition_data.dept = 'SC'")

    spark.close()
  }




}
