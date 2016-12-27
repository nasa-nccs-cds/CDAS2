package nasa.nccs.utilities
import scala.collection.GenTraversableOnce
import scala.collection.mutable.ListBuffer

object DNodeRelation extends Enumeration { val Child, Parent, Ancestor, Descendent = Value }
object DNodeDirection extends Enumeration { val Up, Down = Value }

object DAGNode {
  def sort[T <: DAGNode]( nodes: List[T] ): List[T] = nodes.sortWith( (n0,n1) => n1.hasDescendent(n0) )
}

class DAGNode extends Loggable {
  type DNRelation = DNodeRelation.Value
  type DNDirection = DNodeDirection.Value

  private val _children = ListBuffer [DAGNode]()
  private val _parents = ListBuffer [DAGNode]()

  def addChild( node: DAGNode ) = {
    assert( !(node eq this), " Error, Attempt to add DAG node to itself as child.")
    if( !hasChild(node) ) {
      assert(!(node.hasDescendent(this)), " Error, Attempt to create a circular DAG graph.")
      _children += node;
      node._parents += this;
    }
  }

  private def getRelatives( direction: DNDirection ): ListBuffer[DAGNode] = direction match {
    case DNodeDirection.Down => _children
    case DNodeDirection.Up => _parents
  }

  private def collectRelatives( relation: DNRelation ): ListBuffer[DAGNode] = relation match {
    case DNodeRelation.Child => _children
    case DNodeRelation.Parent => _parents
    case DNodeRelation.Ancestor => accumulate( DNodeDirection.Up )
    case DNodeRelation.Descendent => accumulate( DNodeDirection.Down )
  }

  private def accumulate( direction: DNDirection ): ListBuffer[DAGNode] =
    getRelatives(direction).foldLeft( ListBuffer[DAGNode]() ) {
      (results,node) => {
        results += node;
        results ++= node.accumulate( direction )
      }
    }

  def find( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): Option[DAGNode] = collectRelatives(relation).find( p )
  def filter( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): List[DAGNode] = collectRelatives(relation).filter( p ).toList
  def filterNot( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): List[DAGNode] = collectRelatives(relation).filterNot( p ).toList
  def exists( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): Boolean = collectRelatives(relation).exists( p )
  def forall( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): Boolean = collectRelatives(relation).forall( p )
  def foreach( relation: DNRelation, f: (DAGNode) ⇒ Unit ): Unit = collectRelatives(relation).foreach( f )
  def flatten( relation: DNRelation ): List[DAGNode] = collectRelatives(relation).toList
  def flatMap[B]( relation: DNRelation, f: (DAGNode) ⇒ GenTraversableOnce[B] ): List[B] = collectRelatives(relation).flatMap( f ).toList

  def hasDescendent( dnode: DAGNode ): Boolean = exists( DNodeRelation.Descendent, _ eq dnode )
  def hasAncestor( dnode: DAGNode ): Boolean = exists( DNodeRelation.Ancestor, _ eq dnode )
  def hasChild( dnode: DAGNode ): Boolean = exists( DNodeRelation.Child, _ eq dnode )
  def hasParent( dnode: DAGNode ): Boolean = exists( DNodeRelation.Parent, _ eq dnode )
  def size( relation: DNRelation ) = collectRelatives(relation).size
  def isRoot = ( size( DNodeRelation.Parent ) == 0 )
}

class LabeledDAGNode(id: String) extends DAGNode with Loggable {
  override def toString: String = s"DAGNode[$id]"
}

class DAGNodeTest {
  val dn1 = new LabeledDAGNode("N1")
  val dn2 = new LabeledDAGNode("N2")
  val dn3 = new LabeledDAGNode("N3")
  val dn4 = new LabeledDAGNode("N4")

  dn1.addChild(dn2)
  dn2.addChild(dn3)
  dn2.addChild(dn4)

  println( dn1.size( DNodeRelation.Descendent ) )
  println( dn2.size( DNodeRelation.Descendent ) )
  println( dn4.size( DNodeRelation.Descendent ) )
  println( dn4.size( DNodeRelation.Ancestor ) )
  println( dn4.hasAncestor( dn1 ) )
  println( dn4.hasDescendent( dn1 ) )
  println( dn1.hasAncestor( dn4 ) )
  println( dn1.hasDescendent( dn4 ) )

  println(  DAGNode.sort( List( dn4, dn2, dn3, dn1 ) ).mkString( ", ") )
}
