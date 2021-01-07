package BGAnalysis

object Main {

  def main(args: Array[String]): Unit = {

    //Initialize DataFrames
    for(i <- 0 to 4){
      val query = s"search?order_by=rank&ascending=false&client_id=JLBr5npPhV&skip=${i*100}"
      val filename = s"top_100/${i*100}_to_${(i+1)*100}"
      BoardGameAtlasClient.apiCallToFile(query,filename, false)
    }
    val top100 = DataFrameBuilder.top100ToDF()
    //BoardGameAtlasClient.apiCallToFile("game/mechanics?client_id=JLBr5npPhV", "game_mechanics", false)
    //DataFrameBuilder.gameMechanicsToDF()

    //Questions
    Analysis.question1(top100)
    Analysis.question2(top100)
    Analysis.question3(top100)
    Analysis.question4A(top100)
    Analysis.question4B(top100)
    Analysis.question5(top100)
  }

}
