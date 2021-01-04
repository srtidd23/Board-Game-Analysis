package BGAnalysis

object Main {

  def main(args: Array[String]): Unit = {

    BoardGameAtlasClient.apiCallToFile("search?order_by=rank&ascending=false&client_id=JLBr5npPhV", "top_100", debug=false)
    DataFrameBuilder.top100ToDF()

    BoardGameAtlasClient.apiCallToFile("game/mechanics?client_id=JLBr5npPhV", "game_mechanics", false)
    DataFrameBuilder.gameMechanicsToDF()
  }

}
