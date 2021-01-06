package BGAnalysis

object Main {

  def main(args: Array[String]): Unit = {

    //Initialize DataFrames
    BoardGameAtlasClient.apiCallToFile("search?order_by=rank&ascending=false&client_id=JLBr5npPhV", "top_100", debug=false)
    val top100 = DataFrameBuilder.top100ToDF()
    BoardGameAtlasClient.apiCallToFile("game/mechanics?client_id=JLBr5npPhV", "game_mechanics", false)
    DataFrameBuilder.gameMechanicsToDF()

    //Questions
    Analysis.question1(top100)
    Analysis.question2(top100)
    Analysis.question3(top100)
    Analysis.question4A(top100)
    Analysis.question4B(top100)
    Analysis.question5(top100)
  }

}
