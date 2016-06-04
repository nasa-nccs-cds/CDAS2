package nasa.nccs.caching

import java.io._
import java.nio.file.{Files, Paths}

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.utilities.{Loggable, Timestamp}
import java.util.AbstractMap

import nasa.nccs.cdapi.cdm.DiskCacheFileMgr
import nasa.nccs.cds2.engine.FragmentPersistence._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * The cache has a defined maximum number of entries it can store. After the maximum capacity is reached new
  * entries cause old ones to be evicted in a last-recently-used manner, i.e. the entries that haven't been accessed for
  * the longest time are evicted first.
  */
final class LruCache[K,V]( val cname: String, val ctype: String, val persistent: Boolean ) extends Cache[K,V] with Loggable {
  val maxCapacity: Int=1000
  val initialCapacity: Int=32
  val cacheFile = DiskCacheFileMgr.getDiskCacheFilePath( cname, ctype )
  require(maxCapacity >= 0, "maxCapacity must not be negative")
  require(initialCapacity <= maxCapacity, "initialCapacity must be <= maxCapacity")

  private[caching] val store = getStore()

  def getStore(): ConcurrentLinkedHashMap[K, Future[V]] = {
    val hmap = new ConcurrentLinkedHashMap.Builder[K, Future[V]].initialCapacity(initialCapacity).maximumWeightedCapacity(maxCapacity).build()
    restore match {
      case Some( entrySeq ) => entrySeq.foreach { case (key,value) => hmap.put(key,Future(value)) }
      case None => Unit
    }
    hmap
  }

  def get(key: K) = Option(store.get(key))

  def getEntries: Seq[(K,V)] = {
    val entries = for (entry: java.util.Map.Entry[K, Future[V]] <- store.entrySet.toSet) yield entry.getValue.value match {
      case Some(Success(value)) ⇒ Some( entry.getKey -> value )
      case x ⇒ None
    }
    entries.flatten.toSeq
  }

  def persist(): Unit = {
    Files.createDirectories( Paths.get(cacheFile).getParent )
    val ostr = new ObjectOutputStream ( new FileOutputStream( cacheFile ) )
    val entries = for( entry: java.util.Map.Entry[K,Future[V]] <- store.entrySet.toSet ) yield entry.getValue.value match  {
      case Some(Success(value))  ⇒ Some(  entry.getKey -> value  )
      case x ⇒ None
    }
    ostr.writeObject( getEntries )
  }

  protected def restore: Option[Seq[(K,V)]] = {
    try {
      val istr = new ObjectInputStream(new FileInputStream(cacheFile))
      Some( istr.readObject.asInstanceOf[Seq[(K,V)]] )
    } catch {
      case err: Throwable => logger.warn("Can't load cache map: " + cacheFile); None
    }
  }

  def put( key: K, value: V ) = store.put( key, Future(value) )

  def apply(key: K, genValue: () ⇒ Future[V])(implicit ec: ExecutionContext): Future[V] = {
    val promise = Promise[V]()
    store.putIfAbsent(key, promise.future) match {
      case null ⇒
        val future = genValue()
        future.onComplete { value ⇒
          promise.complete(value)
          // in case of exceptions we remove the cache entry (i.e. try again later)
          if (value.isFailure) store.remove(key, promise.future)
          else if(persistent) persist
        }
        future
      case existingFuture ⇒ existingFuture
    }
  }

  def remove(key: K) = Option(store.remove(key))

  def clear(): Unit = { store.clear() }

  def keys: Set[K] = store.keySet().asScala.toSet
  def values: Iterable[Future[V]] = store.values().asScala

  def ascendingKeys(limit: Option[Int] = None) =
    limit.map { lim ⇒ store.ascendingKeySetWithLimit(lim) }
      .getOrElse(store.ascendingKeySet())
      .iterator().asScala

  def size = store.size
}

private[caching] class Entry[T](val promise: Promise[T]) {
  @volatile var created = Timestamp.now
  @volatile var lastAccessed = Timestamp.now
  def future = promise.future
  def refresh(): Unit = {
    // we dont care whether we overwrite a potentially newer value
    lastAccessed = Timestamp.now
  }
  override def toString = future.value match {
    case Some(Success(value))     ⇒ value.toString
    case Some(Failure(exception)) ⇒ exception.toString
    case None                     ⇒ "pending"
  }
}