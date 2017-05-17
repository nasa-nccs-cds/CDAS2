package nasa.nccs.caching

import java.io._
import java.nio.file.{Files, Paths}
import nasa.nccs.utilities.Logger
import org.apache.commons.io.FileUtils

import collection.mutable
import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EntryWeigher, EvictionListener}
import nasa.nccs.utilities.Loggable
import nasa.nccs.cdapi.cdm.DiskCacheFileMgr
import nasa.nccs.cdas.utilities.appParameters

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait Cache[K,V] { cache ⇒
  type KeyEventNotifier[K] = (String,K) => Unit
  /**
    * Selects the (potentially non-existing) cache entry with the given key.
    */
  def apply(key: K) = new Keyed(key)

  def put( key: K, value: V )
  def putF( key: K, value: Future[V] )
  def putIfAbsent( key: K, value: V )
  def putIfAbsentF( key: K, fvalue: Future[V] )

  def getEntries: Seq[(K,V)]

  class Keyed(key: K) {
    /**
      * Returns either the cached Future for the key or evaluates the given call-by-name argument
      * which produces either a value instance of type `V` or a `Future[V]`.
      */
    def apply(magnet: ⇒ ValueMagnet[V])(implicit ec: ExecutionContext): Future[V] =
      cache.apply(key, () ⇒ try magnet.future catch { case NonFatal(e) ⇒ Future.failed(e) })

    /**
      * Returns either the cached Future for the key or evaluates the given function which
      * should lead to eventual completion of the promise.
      */
    def apply[U](f: Promise[V] ⇒ U)(implicit ec: ExecutionContext): Future[V] =
      cache.apply(key, () ⇒ { val p = Promise[V](); f(p); p.future })
  }

  /**
    * Returns either the cached Future for the given key or evaluates the given value generating
    * function producing a `Future[V]`.
    */
  def apply(key: K, genValue: () ⇒ Future[V])(implicit ec: ExecutionContext): Future[V]

  /**
    * Retrieves the future instance that is currently in the cache for the given key.
    * Returns None if the key has no corresponding cache entry.
    */
  def get(key: K): Option[Future[V]]

  /**
    * Removes the cache item for the given key. Returns the removed item if it was found (and removed).
    */
  def remove(key: K): Option[Future[V]]

  /**
    * Clears the cache by removing all entries.
    */
  def clear(): Set[K]

  def persist()

  /**
    * Returns the set of keys in the cache, in no particular order
    * Should return in roughly constant time.
    * Note that this number might not reflect the exact keys of active, unexpired
    * cache entries, since expired entries are only evicted upon next access
    * (or by being thrown out by a capacity constraint).
    */
  def keys: Set[K]

  def values: Iterable[Future[V]]

  /**
    * Returns a snapshot view of the keys as an iterator, traversing the keys from the least likely
    * to be retained to the most likely.  Note that this is not constant time.
    *
    * @param limit No more than limit keys will be returned
    */
  def ascendingKeys(limit: Option[Int] = None): Iterator[K]

  /**
    * Returns the upper bound for the number of currently cached entries.
    * Note that this number might not reflect the exact number of active, unexpired
    * cache entries, since expired entries are only evicted upon next access
    * (or by being thrown out by a capacity constraint).
    */
  def size: Int
}

class ValueMagnet[V](val future: Future[V])
object ValueMagnet {
  implicit def fromAny[V](block: V): ValueMagnet[V] = fromFuture(Future.successful(block))
  implicit def fromFuture[V](future: Future[V]): ValueMagnet[V] = new ValueMagnet(future)
}
/**
  * The cache has a defined maximum number of entries it can store. After the maximum capacity is reached new
  * entries cause old ones to be evicted in a last-recently-used manner, i.e. the entries that haven't been accessed for
  * the longest time are evicted first.
  */

//class DeletionListener[K,Future[V]]( val cache: Int ) extends EvictionListener[K,Future[V]] {
//  override def onEviction(key: K, value: V ) {;}
//}
class FutureCache[K,V](val cname: String, val ctype: String, val persistent: Boolean ) extends Cache[K,V] with Loggable {
  val KpG = 1000000L
  val maxCapacity: Long = appParameters(Array(ctype.toLowerCase,cname.toLowerCase,"capacity").mkString("."),"30").toLong * KpG
  val initialCapacity: Int=64
  val cacheFile = DiskCacheFileMgr.getDiskCacheFilePath( ctype, cname )
  require(maxCapacity >= 0, "maxCapacity must not be negative")
  require(initialCapacity <= maxCapacity, "initialCapacity must be <= maxCapacity")

  private[caching] val store = getStore()

  def evictionNotice( key: K, value: Future[V] ) = { logger.info( "Evicting Key %s".format( key.toString ) ) }
  def entrySize( key: K, value: Future[V] ): Int = { 1 }
  def weightedSize: Long = store.weightedSize()

  def capacity_log( key: K, msg: String ) = synchronized {
    logger.info( s"CACHE LOG: %s [$cname-$ctype](%s): size = %d".format( msg, key.toString, weightedSize ) );
  }

  def getStore(): ConcurrentLinkedHashMap[K, Future[V]] = {
    val evictionListener = new EvictionListener[K,Future[V]]{ def onEviction(key: K, value: Future[V] ): Unit = {
      capacity_log( key, "--" )
      evictionNotice(key,value)
    } }
    val sizeWeighter = new EntryWeigher[K,Future[V]]{ def weightOf(key: K, value: Future[V] ): Int = { entrySize(key,value) } }
    val hmap = new ConcurrentLinkedHashMap.Builder[K, Future[V]].initialCapacity(initialCapacity).maximumWeightedCapacity(maxCapacity).listener( evictionListener ).weigher( sizeWeighter ).build()
    if(persistent) restore match {
      case Some( entryArray ) => entryArray.foreach { case (key,value) => hmap.put(key,Future(value)) }
      case None => Unit
    }
    hmap
  }

  def get(key: K) = Option(store.get(key))

  def getEntries: Seq[(K,V)] = {
    val entrySet = store.entrySet.toSet
    val entries = for (entry: java.util.Map.Entry[K, Future[V]] <- entrySet ) yield entry.getValue.value match {
      case Some(value) ⇒ Some( (entry.getKey, value.get) )
      case None => None
    }
    entries.flatten.toSeq
  }

  def persist(): Unit = if( persistent ) {
    Files.createDirectories( Paths.get(cacheFile).getParent )
    val ostr = new ObjectOutputStream ( new FileOutputStream( cacheFile ) )
    val entries = getEntries.toList
    logger.info( " ***Persisting cache %s to file '%s', entries: [ %s ]".format( cname, cacheFile, entries.mkString(",") ) )
    ostr.writeObject( entries )
    ostr.close()
  }

  def clear(): Set[K] = {
    val keys: Set[K] = Set[K](store.keys.toSeq: _*)
    if (persistent) {
      logger.info( " ** Deleting cache directory: " + cacheFile )
      FileUtils.deleteDirectory( Paths.get(cacheFile).getParent.toFile )
    }
    store.clear()
    keys
  }


  protected def restore: Option[ Array[(K,V)] ] = {
    try {
      val istr = new ObjectInputStream(new FileInputStream(cacheFile))
      logger.info(s"Restoring $cname cache map from: " + cacheFile);
      Some( istr.readObject.asInstanceOf[ List[(K,V)] ].toArray )
    } catch {
      case err: Throwable =>
        logger.warn("Can't load persisted cache file '" + cacheFile + "' due to error: " + err.toString );
        None
    }
  }

  def put( key: K, value: V ) = if( store.put(key, Future(value) ) == null ) { capacity_log( key, "++" ) }
  def putF( key: K, fvalue: Future[V] ) = if( store.put(key, fvalue ) == null ) { capacity_log( key, "++" ) }
  def putIfAbsent( key: K, value: V ) = if( store.putIfAbsent(key, Future(value) ) == null ) { capacity_log( key, "++" ) }
  def putIfAbsentF( key: K, fvalue: Future[V] ) = if( store.putIfAbsent(key, fvalue ) == null ) { capacity_log( key, "++" ) }

  def apply(key: K, genValue: () ⇒ Future[V])(implicit ec: ExecutionContext): Future[V] = {
    val promise = Promise[V]()
    store.putIfAbsent(key, promise.future) match {
      case null ⇒
        genValue() andThen {
        case Success(value) =>
          capacity_log( key, "++" )
          promise.complete( Success(value) )
        case Failure(e) =>
          val err = if( e.getCause == null ) e else e.getCause
          logger.warn(s"Failed to add element %s to cache $cname:$ctype due to error %s".format( key.toString, err.toString ) )
          logger.error( "Error Stack Trace:\n" + err.getStackTrace.mkString("\n"))
          store.remove(key, promise.future)
      }
      case existingFuture ⇒ existingFuture
    }
  }

  def keys: Set[K] = store.keySet().asScala.toSet
  def values: Iterable[Future[V]] = store.values().asScala

  def remove( key: K ) = Some( store.remove( key ) )

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