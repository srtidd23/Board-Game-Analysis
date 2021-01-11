package BGAnalysis

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import java.nio.file.{Files, Paths}
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import org.apache.spark.sql.SparkSession

import java.io.File


object BoardGameAtlasClient {
  val spark = SparkSession.builder()
    .appName("BGClient")
    .getOrCreate()

  def apiCallToFile(fieldQuery: String = "", filename: String = "board_game_data",debug: Boolean): Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder(s"https://api.boardgameatlas.com/api/${fieldQuery}")
    val httpGet = new HttpGet(uriBuilder.build)
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    val reader = new BufferedReader(new InputStreamReader(entity.getContent))
    var line = reader.readLine
    val file = new File(filename)
    val fileWriter = new PrintWriter(file)
    var lineNumber = 1 //track line number to know when to move to new file
    while (line != null) {
      fileWriter.println(line)
      if (debug) {
        println(line)
      }
      line = reader.readLine()
      lineNumber += 1
    }
    fileWriter.close()
  }

  def apiCallToS3(fieldQuery: String = "", foldername: String = "inputs", filename: String = "board_game_data",debug: Boolean): Unit ={
    apiCallToFile(fieldQuery,filename,debug)

    val BUCKET_NAME = "board-game-analysis"
    val FILE_PATH = Paths.get(filename).toFile.getAbsolutePath
    val FOLDER_NAME = foldername
    val FILE_NAME = FOLDER_NAME+"/"+filename
    val AWS_ACCESS_KEY = spark.conf.get("AWS_ACCESS_KEY")//System.getenv("AWS_ACCESS_KEY")
    val AWS_SECRET_KEY = spark.conf.get("AWS_SECRET_KEY") //System.getenv("AWS_SECRET_KEY")

    try {
      val awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
      val amazonS3Client = new AmazonS3Client(awsCredentials)

      // upload file
      val file = new File(FILE_PATH)
      amazonS3Client.putObject(BUCKET_NAME, FILE_NAME, file)
      file.delete()


    } catch {
      case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
      case ace: AmazonClientException => System.err.println("Exception: " + ace.toString)
    }
  }
}
