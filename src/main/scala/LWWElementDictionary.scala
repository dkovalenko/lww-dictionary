import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.SortedMap
import java.time.Clock
import org.slf4j.Logger

/**
  * LWW-Element-Dictionary implementation
  * 
  * - Lookup, add, and remove operations
  * - Allow updating the value of a key
  * - A function to merge two dictionaries
  * 
  * clock: Clock - a clock instance to get timestamps for entries. You can use custom Clock in tests
  * tombstoneValue: V - a special value that indicates absence(removal) of value because Scala can't use "null" for V. 
  * Could also be implemented as ADT: abstract class/trait with 2 implementations: Value(v) and Tombstone. 
  * It this case user don't need to provide null(tombstone) value.
  * logger: Logger - a logger to do some tracing of state, injected from constructor
  * 
  * Misc properties:
  * 1. GC is not implemented, but it should in realistic scenario where you cound have hundreds of "revisions"
  * 2. For the simplicity this API is not thread-safe per se, but cound be used in a thread-safe manner from the call site.
  * 
  * Implemented using mutable Map of key to SortedMap of timestamps + value tuple
  */
class LWWElementDictionary[K, V](clock: Clock, val tombstoneValue: V, logger: Logger) {

  private val underlying = MutableMap.empty[K, SortedMap[Long, V]] // key -> Map(timestamp1 -> value, timestamp2 -> value)

  /** Inits(add) new values to internal state of LWW-Element-Dictionary
    *
    * @param newValues values to init LWW-Element-Dictionary
    * @return Unit
    */
  def init(newValues: MutableMap[K, SortedMap[Long, V]]): Unit = {
    underlying.addAll(newValues)
  }
  
  /** Gets a value from LWW-Element-Dictionary
    *
    * @param key    the key to update
    * @return an option value containing the latest value associated with the key.
    *         returns `None` if `key` was not defined or removed from the LWW-Element-Dictionary before.
    */
  def get(key: K): Option[V] = {
    logger.trace(s"Dict value for key '$key': ${underlying.get(key)}")
    underlying
      .get(key)
      .flatMap { sortedMap => 
        sortedMap.headOption.flatMap(pair =>
          Option.unless(pair._2 == tombstoneValue)(pair._2) //check for tombstone and return Option[V]
        ) 
      }
  }

  /** Adds a new key/value pair to LWW-Element-Dictionary and optionally returns latest bound value.
    *  If the LWW-Element-Dictionary already contains a
    *  mapping for the key, it will be overridden by the new value.
    *
    * @param key    the key to update
    * @param value  the new value
    * @return an option value containing the latest value associated with the key
    *         before the `put` operation was executed, or `None` if `key`
    *         was not defined in the LWW-Element-Dictionary before.
    */
  def put(key: K, value: V)(implicit ts: Long = getCurrentTimestampMillis()): Option[V] = {
    val valueBeforeUpdate = get(key)
    
    update(key, value)

    valueBeforeUpdate
  }

  /** Adds a new key/value pair to LWW-Element-Dictionary and optionally returns latest bound value.
    *  If the LWW-Element-Dictionary already contains a
    *  mapping for the key, it will be overridden by the new value.
    *
    * @param key    the key to remove
    * @return an option value containing the latest value associated with the key
    *         before the `remove` operation was executed, or `None` if `key`
    *         was not defined in the LWW-Element-Dictionary before.
    */
  def remove(key: K)(implicit ts: Long = getCurrentTimestampMillis()): Option[V] = {
    val valueBeforeUpdate = get(key)

    put(key, tombstoneValue)

    valueBeforeUpdate
  }

  /** Merges this LWW-Element-Dictionary with other.
    *
    * @param other    other LWW-Element-Dictionary to merge
    * @return Either error(left), or new LWW-Element-Dictionary with values from this and other instances merged(right).
    */
  def merge(other: LWWElementDictionary[K, V]): Either[String, LWWElementDictionary[K, V]] = {
    if (tombstoneValue != other.tombstoneValue) return Left("Can't merge LWW Dictionaries with different tombstone values")
    
    logger.trace(s"Dict of this instance before merge: ${underlying}")
    logger.trace(s"Dict of other instance before merge: ${other.underlying}")

    val allKeys = underlying.keySet ++ other.underlying.keySet

    val mergedValues = allKeys.map { key =>
      (underlying.get(key), other.underlying.get(key)) match {
        case (None, None) => (key, getEmptySortedMap())
        case (None, Some(sortedMap)) => (key, sortedMap)
        case (Some(sortedMap), None) => (key, sortedMap)
        case (Some(sortedMap), Some(sortedMap2)) => (key, sortedMap ++ sortedMap2)
      }
    }
    
    // val mergedValues = underlying.map { case (key, sortedMap) =>
    //   other.underlying.get(key) match {
    //     case None => (key, sortedMap)
    //     case Some(sortedMap2) => (key, sortedMap ++ sortedMap2)
    //   }
    // }
    val newDictionary = new LWWElementDictionary[K, V](clock, tombstoneValue, logger)
    newDictionary.init(MutableMap(mergedValues.toSeq: _*))

    logger.trace(s"Dict after merge: ${mergedValues}")
    
    Right(newDictionary)
  }

  /** 
    * @return true if LWW-Element-Dictionary is empty or has every value removed
    */
  def isEmpty(): Boolean = {
    underlying.keys.forall(key => get(key).isEmpty)
  }

  /** 
    * @return number of non-deleted keys in LWW-Element-Dictionary
    */
  def size(): Int = {
    underlying.keys.filterNot(key => get(key).isEmpty).size
  }

  /** 
    * @return total number of keys in LWW-Element-Dictionary with removed ones
    */
  def sizeWithRemoved(): Int = {
    underlying.size
  }

  private def update(key: K, value: V)(implicit ts: Long = getCurrentTimestampMillis()): Unit = {
    underlying.get(key) match {
      case None => underlying.put(key, getEmptySortedMap.addOne(ts -> value))
      case Some(sortedMap) => sortedMap.put(ts, value)
    }
  }

  private def getCurrentTimestampMillis(): Long = {
    clock.millis()
  }

  private def getEmptySortedMap(): SortedMap[Long, V] = {
    implicit val timestampReverseOrdering = Ordering.Long.reverse //Reverse by timestamp value
    SortedMap.empty[Long, V]
  } 
}
