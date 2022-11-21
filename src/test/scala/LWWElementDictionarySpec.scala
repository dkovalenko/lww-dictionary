import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.OptionValues
import java.time.Clock
import org.slf4j.LoggerFactory
import org.scalatest.EitherValues
import scala.collection.mutable.SortedMap
import scala.collection.mutable.{Map => MutableMap}

class LWWElemetDictionarySpec extends AnyFlatSpec with should.Matchers with OptionValues with EitherValues {
  
  def newTestDict(tombstoneValue: Int = Int.MinValue) = new LWWElementDictionary[String, Int](
    clock = Clock.systemUTC(), 
    tombstoneValue = tombstoneValue,
    LoggerFactory.getLogger(this.getClass.getName())
  )

  val TS1 = 11111111L
  val TS2 = 22222222L
  val TS3 = 33333333L
  val TS4 = 44444444L
  val TS5 = 55555555L

  "LWW Dictionary" should "init itself with provided values" in {
    val dict = newTestDict()

    dict.init(MutableMap(
      "a" -> SortedMap(TS1 -> 1),
      "b" -> SortedMap(TS2 -> 2)
    ))
    
    dict.get("a").value shouldBe 1
    dict.get("b").value shouldBe 2
    dict.size() shouldBe 2
  }
  
  it should "perform add/lookup operations with current timestamp" in {
    val dict = newTestDict()
    
    dict.put("a", 1).isDefined shouldBe false
    dict.get("a").value shouldBe 1

    dict.put("a", 2).value shouldBe 1
    dict.get("a").value shouldBe 2
    dict.size() shouldBe 1
  }

  it should "perform add/lookup operations with different timestamps(shuffle ordering)" in {
    val dict = newTestDict()
    
    dict.put("a", 1)(TS1)
    dict.get("a").value shouldBe 1

    dict.put("a", 2)(TS2)
    dict.get("a").value shouldBe 2
    
    dict.put("a", 4)(TS4)
    dict.put("a", 3)(TS3)
    dict.get("a").value shouldBe 4
    dict.size() shouldBe 1
  }

  it should "perform remove operations" in {
    val dict = newTestDict()
    
    dict.put("a", 2)
    
    dict.remove("a").value shouldBe 2
    dict.isEmpty() shouldBe true
    dict.sizeWithRemoved() shouldBe 1
  }

  it should "reinsert element after remove operation" in {
    val dict = newTestDict()
    
    dict.put("a", 2)
    
    dict.remove("a").value shouldBe 2
    dict.isEmpty() shouldBe true

    dict.put("a", 3)
    dict.get("a").value shouldBe 3
    dict.size() shouldBe 1
  }

  it should "merge two LWW Element Dictionaries: simple scenario" in {
    val dict1 = newTestDict()
    val dict2 = newTestDict()
    
    dict1.put("a", 1)
    dict1.put("b", 2)

    dict2.put("c", 3)
    dict2.put("d", 4)
    
    val newDict = dict1.merge(dict2)

    newDict.right.value.get("a").value shouldBe 1
    newDict.right.value.get("b").value shouldBe 2
    newDict.right.value.get("c").value shouldBe 3
    newDict.right.value.get("d").value shouldBe 4
  }

  it should "merge two LWW Element Dictionaries: overlapping values" in {
    val dict1 = newTestDict()
    val dict2 = newTestDict()
    
    dict1.put("a", 1)(TS1)
    dict2.put("a", 3)(TS2)
    dict2.put("b", 4)(TS3)
    dict1.put("b", 2)(TS4)
    
    val newDict = dict1.merge(dict2)

    newDict.right.value.get("a").value shouldBe 3
    newDict.right.value.get("b").value shouldBe 2
  }

  it should "merge two LWW Element Dictionaries: overlapping with removal" in {
    val dict1 = newTestDict()
    val dict2 = newTestDict()
    
    dict1.put("a", 1)(TS1)
    dict2.remove("a")(TS2)
    dict2.remove("b")(TS3)
    dict1.put("b", 2)(TS4)
    
    val newDict = dict1.merge(dict2)

    newDict.right.value.get("a").isDefined shouldBe false
    newDict.right.value.get("b").value shouldBe 2
  }

  it should "don't merge two LWW Element Dictionaries with different tombstones" in {
    val dict1 = newTestDict()
    val dict2 = newTestDict(tombstoneValue = -1)
    
    dict1.put("a", 1)(TS1)
    dict2.remove("a")(TS2)
    dict2.remove("b")(TS3)
    dict1.put("b", 2)(TS4)
    
    val newDict = dict1.merge(dict2)

    newDict.isLeft shouldBe true
  }
}