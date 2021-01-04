package BGAnalysis

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import java.nio.file.{Files, Paths}

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients


object BoardGameAtlasClient {

  def apiCallToFile(fieldQuery: String = "", filename: String = "board_game_data",debug: Boolean): Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder(s"https://api.boardgameatlas.com/api/${fieldQuery}")
    val httpGet = new HttpGet(uriBuilder.build)
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    val reader = new BufferedReader(new InputStreamReader(entity.getContent))
    var line = reader.readLine

    val fileWriter = new PrintWriter(Paths.get(filename).toFile)
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
}
