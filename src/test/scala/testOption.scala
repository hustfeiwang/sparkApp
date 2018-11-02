import java.io.FileNotFoundException

object testOption {
  def main(args: Array[String]): Unit ={

    try{
      getDB("1234")
    }catch {
      case e: IndexOutOfBoundsException =>
        println("index")
      case e: ArrayIndexOutOfBoundsException =>
        println("array")
    }


  }

  def getDatabase(db:String): String={
    getDB(db).getOrElse(throw  new ArrayIndexOutOfBoundsException)
  }

  def getDB(db:String):Option[String]={
    try{
      1/0
      Some("db")
    }catch {
      case e: Throwable =>
        throw new IndexOutOfBoundsException

    }
  }

}
