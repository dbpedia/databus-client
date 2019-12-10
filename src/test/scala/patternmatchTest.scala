import org.scalatest.FlatSpec

class patternmatchTest extends FlatSpec{

  ".query" should "match" in {
    val str = ".query"

    if(str matches(".sparql|.query")) println("matches")
  }
}
