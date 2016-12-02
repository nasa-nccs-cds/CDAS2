package nasa.nccs.utilities
import scala.collection.GenTraversableOnce
import scala.collection.mutable.SortedSet

private trait DAGNodeBase {}
object DNodeRelation extends Enumeration { val Child, Parent, Ancestor, Descendent = Value }
object DNodeDirection extends Enumeration { val Up, Down = Value }

class DAGNode[ T <: DAGNodeBase ] extends DAGNodeBase with Loggable {
  type DNode = DAGNode[T]
  type DNRelation = DNodeRelation.Value
  type DNDirection = DNodeDirection.Value

  private val _children = SortedSet[DNode]()
  private val _parents = SortedSet[DNode]()

  def addChild( node: DNode ) = {
    assert( !(node eq this), " Error, Attempt to add DAG node to itself as child.")
    assert( !(node.hasDescendent(this)), " Error, Attempt to create a circular DAG graph.")
    _children += node;
    node._parents += this;
  }

  private def getRelatives( direction: DNDirection ): IndexedSeq[DNode] = direction match {
    case DNodeDirection.Down => _children.toIndexedSeq
    case DNodeDirection.Up => _parents.toIndexedSeq
  }

  private def getRelatives( relation: DNRelation ): IndexedSeq[DNode] = relation match {
    case DNodeRelation.Child => _children.toIndexedSeq
    case DNodeRelation.Parent => _parents.toIndexedSeq
    case DNodeRelation.Ancestor => accumulate( DNodeDirection.Up ).toIndexedSeq
    case DNodeRelation.Descendent => accumulate( DNodeDirection.Down ).toIndexedSeq
  }

  private def accumulate( direction: DNDirection ): SortedSet[DNode] =
    getRelatives(direction).foldLeft( SortedSet[DNode]() ) { (results,node) => { results += node; results ++= node.accumulate( direction )} }

  def find( relation: DNRelation, p: (DNode) ⇒ Boolean ): Option[DNode] = getRelatives(relation).find( p )
  def filter( relation: DNRelation, p: (DNode) ⇒ Boolean ): IndexedSeq[DNode] = getRelatives(relation).filter( p )
  def filterNot( relation: DNRelation, p: (DNode) ⇒ Boolean ): IndexedSeq[DNode] = getRelatives(relation).filterNot( p )
  def exists( relation: DNRelation, p: (DNode) ⇒ Boolean ): Boolean = getRelatives(relation).exists( p )
  def forall( relation: DNRelation, p: (DNode) ⇒ Boolean ): Boolean = getRelatives(relation).forall( p )
  def foreach( relation: DNRelation, f: (DNode) ⇒ Unit ): Unit = getRelatives(relation).foreach( f )
  def flatten( relation: DNRelation ): IndexedSeq[DNode] = getRelatives(relation)
  def flatMap[B]( relation: DNRelation, f: (DNode) ⇒ GenTraversableOnce[B] ): IndexedSeq[B] = getRelatives(relation).flatMap( f )

  def hasDescendent( dnode: DNode ): Boolean = exists( DNodeRelation.Descendent, _ eq dnode )
  def hasAncestor( dnode: DNode ): Boolean = exists( DNodeRelation.Ancestor, _ eq dnode )

}
