package nasa.nccs.esgf.process

import nasa.nccs.caching.{CDASPartitioner, JobRecord}
import nasa.nccs.cdapi.cdm.{CDSDataset, CDSVariable, Collection, PartitionedFragment}
import nasa.nccs.cdapi.kernels.AxisIndices
import nasa.nccs.cdapi.tensors.CDFloatArray.ReduceOpFlt
import nasa.nccs.cdapi.tensors.{CDCoordMap, CDFloatArray}
import nasa.nccs.cds2.loaders.Collections
import ucar.{ma2, nc2}
import org.joda.time.{DateTime, DateTimeZone}
import nasa.nccs.utilities.Loggable

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import scala.collection.{immutable, mutable}
import scala.collection.mutable.HashSet
import scala.xml._
import mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import nasa.nccs.esgf.utilities.numbers.GenericNumber
import nasa.nccs.esgf.utilities.wpsNameMatchers

import scala.util.Random

case class ErrorReport(severity: String, message: String) {
  override def toString = {
    s"ErrorReport { severity: $severity, message: $message }"
  }

  def toXml = {
      <error severity={severity} message={message}/>
  }
}

class TaskRequest(val name: String, val variableMap : Map[String,DataContainer], val domainMap: Map[String,DomainContainer], val workflows: List[WorkflowContainer] = List(), val targetGridSpec: Map[String,String]=Map("id"->"#META") ) {
  val errorReports = new ListBuffer[ErrorReport]()
  val logger = LoggerFactory.getLogger( this.getClass )
  validate()
//  logger.info( s"TaskRequest: name= $name, workflows= " + workflows.toString + ", variableMap= " + variableMap.toString + ", domainMap= " + domainMap.toString )

  def addErrorReport(severity: String, message: String) = {
    val error_rep = ErrorReport(severity, message)
    logger.error(error_rep.toString)
    errorReports += error_rep
  }

  def getJobRec( run_args: Map[String,String] ): JobRecord = {
    val jobIds = for( workflow <- workflows; operation <- workflow.operations ) yield operation.rid
    new JobRecord( jobIds.mkString("-"))
  }

  def isMetadataRequest: Boolean = name.split('.').last.toLowerCase().equals("metadata")

  def getDataAccessMode(): DataAccessMode = name.split('.').last.toLowerCase match {
      case "metadata" =>  DataAccessMode.MetaData
      case "cache" =>     DataAccessMode.Cache
      case _ =>           DataAccessMode.Read
    }

  def getDomain( data_source: DataSource ): Option[DomainContainer] = {
    data_source.domain match {
      case "" => None
      case domain =>
        assert( domainMap.contains( domain ), "Undefined domain for dataset " + data_source.name + ", domain = " + data_source.domain )
        domainMap.get( domain )
    }
  }

  def validate() = {
    for( variable <- inputVariables; if variable.isSource; domid = variable.getSource.domain; vid=variable.getSource.name; if !domid.isEmpty ) {
      if ( !domainMap.contains(domid) && !domid.contains("|") ) {
        var keylist = domainMap.keys.mkString("[",",","]")
        logger.error( s"Error, No $domid in $keylist in variable $vid" )
        throw new Exception( s"Error, Missing domain $domid in variable $vid" )
      }
    }
    for (workflow <- workflows; operation <- workflow.operations; opid = operation.name; var_arg <- operation.inputs; if !var_arg.isEmpty ) {
      if (!variableMap.contains(var_arg)) {
        var keylist = variableMap.keys.mkString("[", ",", "]")
        logger.error(s"Error, No $var_arg in $keylist in operation $opid")
        throw new Exception(s"Error, Missing variable $var_arg in operation $opid")
      }
    }
  }

  override def toString = {
    var taskStr = s"TaskRequest { name='$name', variables = '$variableMap', domains='$domainMap', workflows='$workflows' }"
    if ( errorReports.nonEmpty ) {
      taskStr += errorReports.mkString("\nError Reports: {\n\t", "\n\t", "\n}")
    }
    taskStr
  }

  def toXml = {
    <task_request name={name}>
      <data>
        { inputVariables.map(_.toXml )  }
      </data>
      <domains>
        {domainMap.values.map(_.toXml ) }
      </domains>
      <operation>
        { workflows.map(_.toXml ) }
      </operation>
      <error_reports>
        {errorReports.map(_.toXml ) }
      </error_reports>
    </task_request>
  }

  def inputVariables: Traversable[DataContainer] = {
    for( variableSource <- variableMap.values; if variableSource.isInstanceOf[ DataContainer ] ) yield variableSource.asInstanceOf[DataContainer]
  }
}

object TaskRequest {
  val logger = LoggerFactory.getLogger( this.getClass )
  def apply(process_name: String, datainputs: Map[String, Seq[Map[String, Any]]]) = {
//    logger.info( "TaskRequest--> process_name: %s, datainputs: %s".format( process_name, datainputs.toString ) )
    val data_list: List[DataContainer] = datainputs.getOrElse("variable", List() ).flatMap(DataContainer.factory(_)).toList
    val domain_list: List[DomainContainer] = datainputs.getOrElse("domain", List()).map(DomainContainer(_)).toList
    val operation_list: List[WorkflowContainer] = datainputs.getOrElse("operation", List() ).map(WorkflowContainer( process_name, data_list.map(_.uid), _ ) ).toList
    val variableMap = buildVarMap( data_list, operation_list )
    val domainMap = buildDomainMap( domain_list )
    val gridId = datainputs.getOrElse("grid", data_list.headOption.map( dc => dc.uid ).getOrElse("#META") ).toString
    val gridSpec = Map( "id" -> gridId.toString )
    new TaskRequest( process_name, variableMap, domainMap, operation_list, gridSpec )
  }

  def buildVarMap( data: List[DataContainer], workflow: List[WorkflowContainer] ): Map[String,DataContainer] = {
    var data_var_items = for( data_container <- data ) yield ( data_container.uid -> data_container )
    var op_var_items = for( workflow_container<- workflow; operation<-workflow_container.operations; if !operation.rid.isEmpty ) yield ( operation.rid -> DataContainer(operation) )
    val var_map = Map( op_var_items ++ data_var_items: _* )
//    logger.info( "Created Variable Map: " + var_map.toString + " from data containers: " + data.map( data_container => ( "id:" + data_container.uid ) ).mkString("[ ",", "," ]") )
    for( workflow_container<- workflow; operation<-workflow_container.operations; vid<-operation.inputs; if(!vid.isEmpty)  ) var_map.get( vid ) match {
      case Some(data_container) => data_container.addOpSpec( operation )
      case None => throw new Exception( "Unrecognized variable %s in varlist [%s]".format( vid, var_map.keys.mkString(",") ) )
    }
    var_map
  }

  def buildDomainMap( domain_containers: List[DomainContainer] ): Map[String,DomainContainer] = {
    val domain_map = domain_containers.map( domain_container => domain_container.name -> domain_container ).toMap
//    logger.info( "Created Domain Map: " + domain_map.toString )
    domain_map
  }
}

class ContainerBase {
  val logger = LoggerFactory.getLogger( this.getClass )
  def item_key(map_item: (String, Any)): String = map_item._1

  def normalize(sval: String): String = stripQuotes(sval).toLowerCase

  def stripQuotes(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"")

  def getStringKeyMap( generic_map: Map[_,_] ): Map[String,Any] = {
    assert( generic_map.isEmpty | generic_map.keys.head.isInstanceOf[ String ] )
    generic_map.asInstanceOf[ Map[String,Any] ]
  }

  def key_equals(key_value: String)(map_item: (String, Any)): Boolean = {
    item_key(map_item) == key_value
  }

  def key_equals(key_regex: Regex)(map_item: (String, Any)): Boolean = {
    key_regex.findFirstIn(item_key(map_item)) match {
      case Some(x) => true;
      case None => false;
    }
  }

  //  def key_equals( key_expr: Iterable[Any] )( map_item: (String, Any) ): Boolean = { key_expr.map( key_equals(_)(map_item) ).find( ((x:Boolean) => x) ) }
  def filterMap(raw_metadata: Map[String, Any], key_matcher: (((String, Any)) => Boolean)): Option[Any] = {
    raw_metadata.find(key_matcher) match {
      case Some(x) => Some(x._2)
      case None => None
    }
  }

  def toXml = {
    <container>
      {"<![CDATA[ " + toString + " ]]>"}
    </container>
  }

  def getGenericNumber( opt_val: Option[Any] ): GenericNumber = {
    opt_val match {
      case Some(p) => GenericNumber(p)
      case None =>    GenericNumber()
    }
  }
  def getStringValue( opt_val: Option[Any] ): String = {
    opt_val match {
      case Some(p) => p.toString
      case None => ""
    }
  }
}

class PartitionSpec( val axisIndex: Int, val nPart: Int, val partIndex: Int = 0 ) {
  override def toString =  s"PartitionSpec { axis = $axisIndex, nPart = $nPart, partIndex = $partIndex }"
}

class DataSource(val name: String, val collection: Collection, val domain: String, val fragIdOpt: Option[String] = None ) {
  def this( dsource: DataSource ) = this( dsource.name, dsource.collection, dsource.domain )
  override def toString =  s"DataSource { name = $name, collection = %s, domain = $domain, %s }".format(collection.toString,fragIdOpt.map(", fragment = "+_).getOrElse("") )
  def toXml = <dataset name={name} domain={domain}>{ collection.toXml }</dataset>
  def isDefined = ( !collection.isEmpty && !name.isEmpty )
  def isReadable = ( !collection.isEmpty && !name.isEmpty && !domain.isEmpty )
  def getKey: Option[DataFragmentKey] = fragIdOpt.map( DataFragmentKey.apply(_) )
}

class DataFragmentKey( val varname: String, val collId: String, val origin: Array[Int], val shape: Array[Int] ) extends Serializable {
  override def toString =  "DataFragmentKey{ name = %s, collection = %s, origin = [ %s ], shape = [ %s ] }".format( varname, collId, origin.mkString(", "), shape.mkString(", "))
  def toStrRep =  "%s|%s|%s|%s|%d".format( varname, collId, origin.mkString(","), shape.mkString(","), CDASPartitioner.nProcessors )
  def sameVariable( otherCollId: String, otherVarName: String ): Boolean = { (varname == otherVarName) && (collId == otherCollId) }
  def getRoi: ma2.Section = new ma2.Section(origin,shape)
  def equalRoi( df: DataFragmentKey ): Boolean = ( shape.sameElements(df.shape) && origin.sameElements(df.origin ) )
  def getSize: Int = shape.foldLeft(1)( _ * _ )
  def contains( df: DataFragmentKey ): Boolean = getRoi.contains( df.getRoi )
  def contains( df: DataFragmentKey, admitEquality: Boolean ): Boolean = if( admitEquality ) contains( df ) else containsSmaller( df )
  def containsSmaller( df: DataFragmentKey ): Boolean = ( !equalRoi( df ) && contains( df ) )
}

object DataFragmentKey {
  def parseArray( arrStr: String ): Array[Int] = { arrStr.split(',').map( _.toInt) }
  def apply( fkeyStr: String ): DataFragmentKey = {
    val toks = fkeyStr.split('|')
    new DataFragmentKey( toks(0), toks(1), parseArray(toks(2)), parseArray(toks(3)) )
  }
  def sameVariable( fkeyStr: String, otherCollection: String, otherVarName: String ): Boolean = {
    val toks = fkeyStr.split('|')
    (toks(0) == otherVarName) && (toks(1) == otherCollection)
  }
}

object DataFragmentSpec {

  def offset( section: ma2.Section, newOrigin: ma2.Section ): ma2.Section = {
    assert(newOrigin.getRank == section.getRank, "Invalid Section rank in offset")
    val new_ranges = for (i <- (0 until section.getRank); range = section.getRange(i); origin = newOrigin.getRange(i)) yield range.shiftOrigin(-origin.first())
    new ma2.Section(new_ranges)
  }
}

object MergeDataFragment {
  def apply( df: DataFragment ): MergeDataFragment = new MergeDataFragment( Some( df ) )
  def apply(): MergeDataFragment = new MergeDataFragment()
}
class MergeDataFragment( val wrappedDataFragOpt: Option[DataFragment] = None ) {
  def ++( dfrag: DataFragment ): MergeDataFragment = wrappedDataFragOpt match {
    case None => MergeDataFragment( dfrag )
    case Some( wrappedDataFrag ) => MergeDataFragment( wrappedDataFrag ++ dfrag )
  }
}
// DataFragmentSpec, SectionMerge.Status  DataFragmentSpec, SectionMerge.Status
object DataFragment {
  def combine( reductionOp: ReduceOpFlt, input0: DataFragment, input1: DataFragment ): DataFragment = {
    val ( data, ( fragSpec, mergeStatus) ) = input0.optCoordMap match {
      case Some( coordMap ) =>  ( CDFloatArray.combine( reductionOp, input1.data, input0.data, coordMap.subset(input0.spec.roi) ), input1.spec.combine(input0.spec,false) )
      case None => input1.optCoordMap match {
        case Some( coordMap ) => ( CDFloatArray.combine( reductionOp, input0.data, input1.data, coordMap.subset(input1.spec.roi) ), input0.spec.combine(input1.spec,false) )
        case None => ( CDFloatArray.combine( reductionOp, input0.data, input1.data ), input0.spec.combine(input1.spec,true) )
      }
    }
    new DataFragment( fragSpec, data )
  }
  def combineCoordMaps(a0: DataFragment, a1: DataFragment): Option[CDCoordMap] = a0.optCoordMap.flatMap( coordMap0 => a1.optCoordMap.map( coordMap1 => coordMap0 ++ coordMap1 ))
}

class DataFragment( val spec: DataFragmentSpec, val data: CDFloatArray, val optData: Option[CDFloatArray] = None, val optCoordMap: Option[CDCoordMap] = None ) {
  import DataFragment._
  def ++( dfrag: DataFragment ): DataFragment = {
    new DataFragment( spec.merge(dfrag.spec), data.merge(dfrag.data), optData.map( data1 => data1.merge(dfrag.optData.get) ), combineCoordMaps( this,dfrag ) )
  }
  def getReducedSpec( axes: AxisIndices ): DataFragmentSpec =  spec.reduce(Set(axes.getAxes:_*))
  def getReducedSpec(  axisIndices: Set[Int], newsize: Int = 1  ): DataFragmentSpec =  spec.reduce(axisIndices,newsize)
  def subset( section: ma2.Section ): Option[DataFragment] = spec.cutIntersection( section ) map { dataFragSpec =>
    val new_section = dataFragSpec.getIntersection(section)
    new DataFragment( dataFragSpec, data.section( new_section ), optData.map( data1 => data1.section( new_section ) ), optCoordMap )
  }
}

object SectionMerge {
  type Status = Int
  val Overlap: Status = 0
  val Append: Status = 1
  val Prepend: Status = 1
  def incommensurate( s0: ma2.Section, s1: ma2.Section ) = { "Attempt to combine incommensurate sections: %s vs %s".format( s0.toString, s1.toString ) }
}

class DataFragmentSpec( val varname: String="", val collection: Collection = new Collection, val fragIdOpt: Option[String]=None, val targetGridOpt: Option[TargetGrid]=None, val dimensions: String="", val units: String="",
                        val longname: String="", private val _section: ma2.Section = new ma2.Section(), private val _domSectOpt: Option[ma2.Section], val missing_value: Float, val mask: Option[String] = None ) extends Loggable {
//  logger.info( "DATA FRAGMENT SPEC: section: %s, _domSectOpt: %s".format( _section, _domSectOpt.getOrElse("null").toString ) )
  override def toString =  "DataFragmentSpec { varname = %s, collection = %s, dimensions = %s, units = %s, longname = %s, roi = %s }".format( varname, collection, dimensions, units, longname, roi.toString)
  def sameVariable( otherCollection: String, otherVarName: String ): Boolean = { (varname == otherVarName) && (collection == otherCollection) }
  def toXml = {
    mask match {
      case None => <input varname={varname} longname={longname} units={units} roi={roi.toString} >{collection.toXml}</input>
      case Some(maskId) => <input varname={varname} longname={longname} units={units} roi={roi.toString} mask={maskId} >{collection.toXml}</input>
    }
  }
  def combine( other: DataFragmentSpec, sectionMerge: Boolean = true ): ( DataFragmentSpec, SectionMerge.Status ) = {
    val combined_varname = varname + ":" + other.varname
    val combined_longname = longname + ":" + other.longname
    val ( combined_section, mergeStatus ) = if(sectionMerge) combineRoi( other.roi ) else ( roi, SectionMerge.Overlap )
    ( new DataFragmentSpec( combined_varname, collection, None, targetGridOpt, dimensions, units, combined_longname, combined_section, _domSectOpt, missing_value, mask ) -> mergeStatus )
  }
  def roi = targetGridOpt match {
    case None => new ma2.Section( _section )
    case Some( targetGrid ) => targetGrid.addSectionMetadata( _section )
  }
  def domainSectOpt = _domSectOpt.map( sect => new ma2.Section( sect ) )

  def toBoundsString = { targetGridOpt.map( _.toBoundsString ).getOrElse("") }

  def reshape( newSection: ma2.Section ): DataFragmentSpec = new DataFragmentSpec( varname, collection, fragIdOpt, targetGridOpt, dimensions, units, longname, new ma2.Section(newSection), domainSectOpt, missing_value, mask )

  def getBounds: Array[Double] = targetGridOpt.flatMap( targetGrid => targetGrid.getBounds(roi) ) match {
    case Some( array ) => array
    case None => throw new Exception( "Can't get bounds from FragmentSpec: " + toString )
  }

  private def collapse( range: ma2.Range, newsize: Int = 1 ): ma2.Range = newsize match {
    case 1 => val mid_val = (range.first+range.last)/2; new ma2.Range(range.getName,mid_val,mid_val)
    case ns => val incr = math.round((range.last-range.first)/ns.toFloat); new ma2.Range(range.getName,range.first(),range.last,incr)
  }
  def getRange( dimension_name: String ): Option[ma2.Range] = {
    val dims = dimensions.toLowerCase.split(' ')
    dims.indexOf( dimension_name.toLowerCase ) match {
      case -1 => None
      case x => Option( roi.getRange( x ) )
    }
  }

  def getRangeCF( CFName: String ): Option[ma2.Range] = Option( roi.find(CFName) )

  def getShape = roi.getShape

  def getGridShape: Array[Int] = {
    val grid_axes = List( "x", "y" )
    val ranges = dimensions.toLowerCase.split(' ').flatMap( getRange )
    ranges.map(  range => if(grid_axes.contains(range.getName.toLowerCase)) range.length else 1 )
  }

//  def getAxisType( cfName: String ):  DomainAxis.Type.Value = targetGridOpt.flatMap( _.grid.getAxisSpec(cfName).map( _.getAxisType ) )

  def getAxes: List[DomainAxis] = roi.getRanges.map( (range: ma2.Range) => new  DomainAxis( DomainAxis.fromCFAxisName(range.getName), range.first, range.last, "indices" ) ).toList

  def getKey: DataFragmentKey = {
    new DataFragmentKey( varname, collection.id, roi.getOrigin, roi.getShape )
  }
  def getSize: Int = roi.getShape.product

  def getKeyString: String = getKey.toString

  def domainSpec: DataFragmentSpec = domainSectOpt match {
    case None => this;
    case Some(cutSection) => new DataFragmentSpec( varname, collection, fragIdOpt, targetGridOpt, dimensions, units, longname, roi.intersect(cutSection), domainSectOpt, missing_value, mask )
  }

  def intersectRoi( cutSection: ma2.Section ): ma2.Section = {
    val base_sect = roi;      val raw_intersection = base_sect.intersect(cutSection)
    val ranges = for( ir <- raw_intersection.getRanges.indices; r0 = raw_intersection.getRange(ir); r1 = base_sect.getRange(ir) ) yield new ma2.Range( r1.getName, r0 )
    new ma2.Section( ranges )
  }
  def combineRoi( otherSection: ma2.Section ): ( ma2.Section, SectionMerge.Status ) = {
    logger.info( "\n\nCombine SECTIONS: %s - %s \n\n".format( _section.toString, otherSection.toString ))
    var sectionMerge: SectionMerge.Status = SectionMerge.Overlap
    val new_ranges: IndexedSeq[ma2.Range] = for( iR <- _section.getRanges.indices; r0 = _section.getRange(iR); r1 = otherSection.getRange(iR) ) yield {
      if( r0 == r1 ) { r0 }
      else if( (r0.last + 1) == (r1.first) ) {
        assert( sectionMerge == SectionMerge.Overlap, SectionMerge.incommensurate( _section, otherSection ) )
        sectionMerge = SectionMerge.Append;
        new ma2.Range( r0.first, r1.last, r0.stride )
      }
      else if( (r1.last + 1) == (r0.first) ) {
        assert( sectionMerge == SectionMerge.Overlap, SectionMerge.incommensurate( _section, otherSection ) )
        sectionMerge = SectionMerge.Prepend;
        new ma2.Range( r1.first, r0.last, r0.stride )
      }
      else throw new Exception( SectionMerge.incommensurate( _section, otherSection ) )
    }
    ( new ma2.Section( new_ranges:_* ) -> sectionMerge )
  }

  def cutIntersection( cutSection: ma2.Section ): Option[DataFragmentSpec] =
    if( roi.intersects( cutSection ) ) {
      val intersection = intersectRoi(cutSection)
//      logger.info( "DOMAIN INTERSECTION:  %s <-> %s  => %s".format( roi.toString, cutSection.toString, intersection.toString ))
      Some( new DataFragmentSpec( varname, collection, fragIdOpt, targetGridOpt, dimensions, units, longname, intersection, domainSectOpt, missing_value, mask ) )
    }  else None

  def getReducedSection( axisIndices: Set[Int], newsize: Int = 1 ): ma2.Section = {
    new ma2.Section( roi.getRanges.zipWithIndex.map( rngIndx => if( axisIndices(rngIndx._2) ) collapse( rngIndx._1, newsize ) else rngIndx._1 ):_* )
  }

  def reduce( axisIndices: Set[Int], newsize: Int = 1 ): DataFragmentSpec =  reSection( getReducedSection(axisIndices,newsize) )

  def getIntersection( subsection: ma2.Section  ): ma2.Section = { subsection.intersect( _section ) }
  def intersects( subsection: ma2.Section  ): Boolean = { subsection.intersects( _section ) }

  def getSubSection( subsection: ma2.Section  ): ma2.Section = {
    new ma2.Section( roi.getRanges.zipWithIndex.map( rngIndx => {
      val ss = subsection.getRange(rngIndx._2)
      rngIndx._1.compose( ss )
    } ) )

  }

  def getVariableMetadata(serverContext: ServerContext): Map[String,nc2.Attribute] = {
    var v: CDSVariable =  serverContext.getVariable( collection, varname )
    v.attributes ++ Map( "description" -> new nc2.Attribute("description",v.description), "units"->new nc2.Attribute("units",v.units),
      "fullname"->new nc2.Attribute("fullname",v.fullname), "axes" -> new nc2.Attribute("axes",dimensions),
      "varname" -> new nc2.Attribute("varname",varname), "collection" -> new nc2.Attribute("collection",collection.url) )
  }

  def getDatasetMetadata(serverContext: ServerContext): List[nc2.Attribute] = {
    var dset: CDSDataset = serverContext.getDataset( collection, varname )
    dset.attributes
  }
  def getDataset(serverContext: ServerContext): CDSDataset = {
    serverContext.getDataset( collection, varname )
  }

  def reduceSection( dimensions: Int*  ): DataFragmentSpec = {
    var newSection = roi;
    for(  dim: Int <- dimensions ) { newSection = newSection.setRange( dim, new ma2.Range(0,0,1) ) }
    reSection( newSection )
  }

  def merge( dfSpec: DataFragmentSpec, dimIndex: Int = 0 ): DataFragmentSpec = {
    val combinedRange = roi.getRange(dimIndex).union( dfSpec.roi.getRange(dimIndex) )
    val newSection: ma2.Section = roi.replaceRange( dimIndex, combinedRange )
    reSection( newSection )
  }

  def reSection( newSection: ma2.Section ): DataFragmentSpec = {
//    println( " ++++ ReSection: newSection=(%s), roi=(%s)".format( newSection.toString, roi.toString ) )
    val newRanges = for( iR <- roi.getRanges.indices; r0 = roi.getRange(iR); rNew = newSection.getRange(iR) ) yield new ma2.Range(r0.getName,rNew)
    new DataFragmentSpec( varname, collection, fragIdOpt, targetGridOpt, dimensions, units, longname, new ma2.Section(newRanges), domainSectOpt, missing_value, mask )
  }
  def reSection( fkey: DataFragmentKey ): DataFragmentSpec = reSection( fkey.getRoi )


//  private var dataFrag: Option[PartitionedFragment] = None
  //  def setData( fragment: PartitionedFragment ) = { assert( dataFrag == None, "Overwriting Data Fragment in " + toString ); dataFrag = Option(fragment) }
  //  def getData: Option[PartitionedFragment] = dataFrag
}


object OperationSpecs {
  def apply( op: OperationContext ) = new OperationSpecs( op.name, op.getConfiguration )
}
class OperationSpecs( id: String, val optargs: Map[String,String] ) {
  val ids = mutable.HashSet( id )
  def ==( oSpec: OperationSpecs ) = ( optargs == oSpec.optargs )
  def merge ( oSpec: OperationSpecs  ) = { ids ++= oSpec.ids }
  def getSpec( id: String, default: String = "" ): String = optargs.getOrElse( id, default )
}


class DataContainer(val uid: String, private val source : Option[DataSource] = None, private val operation : Option[OperationContext] = None ) extends ContainerBase {
  assert( source.isDefined || operation.isDefined, s"Empty DataContainer: variable uid = $uid" )
  assert( source.isEmpty || operation.isEmpty, s"Conflicted DataContainer: variable uid = $uid" )
  private val optSpecs = mutable.ListBuffer[ OperationSpecs ]()

  override def toString = {
    val embedded_val: String = if ( source.isDefined ) source.get.toString else operation.get.toString
    s"DataContainer ( $uid ) { $embedded_val }"
  }
  override def toXml = {
    val embedded_xml = if ( source.isDefined ) source.get.toXml else operation.get.toXml
    <dataset uid={uid}> embedded_xml </dataset>
  }
  def isSource = source.isDefined

  def isOperation = operation.isDefined
  def getSource = {
    assert( isSource, s"Attempt to access an operation based DataContainer($uid) as a data source")
    source.get
  }
  def getOperation = {
    assert( isOperation, s"Attempt to access a source based DataContainer($uid) as an operation")
    operation.get
  }

  def addOpSpec( operation: OperationContext ): Unit = {     // used to inform data container what types of ops will be performed on it.
    def mergeOpSpec( oSpecList: mutable.ListBuffer[ OperationSpecs ], oSpec: OperationSpecs ): Unit = oSpecList.headOption match {
      case None => oSpecList += oSpec
      case Some(head) => if( head == oSpec ) head merge oSpec else mergeOpSpec(oSpecList.tail,oSpec)
    }
    mergeOpSpec( optSpecs, OperationSpecs(operation) )
  }
  def getOpSpecs: List[OperationSpecs] = optSpecs.toList
}

object DataContainer extends ContainerBase {
  private val random = new Random( System.currentTimeMillis )

  def apply( operation: OperationContext ): DataContainer = {
      new DataContainer( uid=operation.rid, operation=Some(operation) )
  }
  def absPath( path: String ): String = new java.io.File(path).getAbsolutePath.toLowerCase

  def getCollection(metadata: Map[String, Any]): ( Collection, Option[String] ) = {
    val uri = metadata.getOrElse("uri","").toString
    val varsList: List[String] = metadata.getOrElse("name","").toString.split(",").map( item => stripQuotes( item.split(':').head ) ).toList
    val path =  metadata.getOrElse("path","").toString
    val collection =  metadata.getOrElse("collection","").toString
    val title =  metadata.getOrElse("title","").toString
    val fileFilter = metadata.getOrElse("fileFilter","").toString
    val id = parseUri(uri)
    logger.info( s" >>>>>>>>>>>----> getCollection, uri=$uri, id=$id")
    val colId = if(!collection.isEmpty) { collection } else uri match {
      case colUri if(colUri.startsWith("collection")) => id
      case fragUri if(fragUri.startsWith("fragment")) => id.split('|')(1)
      case x => ""
    }
    val fragIdOpt = if(uri.startsWith("fragment")) Some(id) else None
    Collections.findCollection( colId ) match {
      case Some(collection) =>
        if(!path.isEmpty) { assert( absPath(path).equals(absPath(collection.path)), "Collection %s already exists and its path (%s) does not correspond to the specified path (%s)".format(collection.id,collection.path,path) ) }
        ( collection, fragIdOpt )
      case None =>
        if( path.isEmpty && !collection.isEmpty ) {
          (Collections.addCollection(uri, colId, title, varsList), fragIdOpt)
        } else {
          val fpath = if (new java.io.File(id).isFile) id else path
          if (colId.isEmpty || fpath.isEmpty) logger.warn(s"Unrecognized collection: '$colId', current collections: " + Collections.idSet.mkString(", "))
          (Collections.addCollection(uri, fpath, fileFilter, title, varsList), fragIdOpt)
        }
    }
  }

  def factory(metadata: Map[String, Any]): Array[DataContainer] = {
    try {
      val fullname = metadata.getOrElse("name", "").toString
      val domain = metadata.getOrElse("domain", "").toString
      val (collection, fragIdOpt) = getCollection(metadata)
      val var_names: Array[String] = if (fullname.equals("*")) collection.varNames.toArray else fullname.toString.split(',')
      val base_index = random.nextInt(Integer.MAX_VALUE)

      fragIdOpt match {
        case Some(fragId) =>
          val name_items = var_names.head.split(':')
          val dsource = new DataSource(stripQuotes(name_items.head), collection, normalize(domain), fragIdOpt )
          val vid = normalize(name_items.last)
          Array( new DataContainer(if (vid.isEmpty) s"c-$base_index" else vid, source = Some(dsource)) )
        case None =>
          for ((name, index) <- var_names.zipWithIndex) yield {
            val name_items = name.split(':')
            val dsource = new DataSource(stripQuotes(name_items.head), collection, normalize(domain))
            val vid = normalize(name_items.last)
            new DataContainer(if (vid.isEmpty) s"c-$base_index$index" else vid, source = Some(dsource))
          }
      }
    } catch {
      case e: Exception =>
        logger.error("Error creating DataContainer: " + e.getMessage)
        logger.error(e.getStackTrace.mkString("\n"))
        throw new Exception(e.getMessage, e)
    }
  }

  def parseUri( uri: String ): String = {
    if(uri.isEmpty) "" else {
      val recognizedUrlTypes = List("file", "collection", "fragment")
      val uri_parts = uri.split(":")
      val url_type = normalize(uri_parts.head)
      if (url_type == "http") {
        uri
      } else {
        if (recognizedUrlTypes.contains(url_type)) {
          val value = uri_parts.last.toLowerCase
          if (List("collection", "fragment").contains(url_type)) value.stripPrefix("/").stripPrefix("/") else value
        } else throw new Exception("Unrecognized uri format: " + uri + ", type = " + uri_parts.head + ", nparts = " + uri_parts.length.toString + ", value = " + uri_parts.last)
      }
    }
  }
}

class DomainContainer( val name: String, val axes: List[DomainAxis] = List.empty[DomainAxis], val mask: Option[String]=None ) extends ContainerBase {
  override def toString = {
    s"DomainContainer { name = $name, axes = $axes }"
  }
  def toDataInput: Map[ String, Any ] = Map( axes.map(_.toDataInput):_* ) ++ Map( ("name" -> name) )

  override def toXml = {
    <domain name={name}>
      <axes> { axes.map( _.toXml ) } </axes>
      { mask match { case None => Unit; case Some(maskId) => <mask> { maskId } </mask> } }
    </domain>
  }
}

object DomainAxis extends ContainerBase {
  object Type extends Enumeration { val X, Y, Z, T = Value }
  def fromCFAxisName( cfName: String ): Type.Value =
    cfName.toLowerCase match { case "x" => Type.X; case "y" => Type.Y; case "z" => Type.Z; case "t" => Type.T; }

  def coordAxisName(axistype: DomainAxis.Type.Value): String = {
    import DomainAxis.Type._
    axistype match { case X => "lon"; case Y => "lat"; case Z => "level"; case T => "time" }
  }

  def apply( axistype: Type.Value, start: Int, end: Int ): Option[DomainAxis] = {
    Some( new DomainAxis(  axistype, start, end, "indices" ) )
  }

  def apply( axistype: Type.Value, axis_spec: Option[Any] ): Option[DomainAxis] = {
    axis_spec match {
      case Some(generic_axis_map: Map[_,_]) =>
        val axis_map = getStringKeyMap( generic_axis_map )
        val start = getGenericNumber( axis_map.get("start") )
        val end = getGenericNumber( axis_map.get("end") )
        val system = getStringValue( axis_map.get("system") )
        val bounds = getStringValue( axis_map.get("bounds") )
        Some( new DomainAxis( axistype, start, end, normalize(system), normalize(bounds) ) )
      case Some(sval: String) =>
        val gval = getGenericNumber( Some(sval) )
        Some( new DomainAxis( axistype, gval, gval, "values" ) )
      case None => None
      case _ =>
        val msg = "Unrecognized DomainAxis spec: " + axis_spec.toString
        logger.error( msg )
        throw new Exception(msg)
    }
  }
}

class DomainAxis( val axistype: DomainAxis.Type.Value, val start: GenericNumber, val end: GenericNumber, val system: String, val bounds: String = "" ) extends ContainerBase  {
  import DomainAxis.Type._
  val name =   axistype.toString
  def getCFAxisName: String = axistype match { case X => "X"; case Y => "Y"; case Z => "Z"; case T => "T" }
  def getCoordAxisName: String = DomainAxis.coordAxisName(axistype)
  override def toString = s"DomainAxis { name = $name, start = $start, end = $end, system = $system, bounds = $bounds }"
  def toBoundsString = s"$name:[$start,$end,$system]"
  def toDataInput: (String,Map[String,String]) = (getCoordAxisName -> Map("start" -> start.toString, "end" -> end.toString, "system" -> system) )

  override def toXml = {
    <axis name={name} start={start.toString} end={end.toString} system={system} bounds={bounds} />
  }

  def matches ( axisType: nc2.constants.AxisType ): Boolean = {
    import nc2.constants.AxisType, DomainAxis.Type._
    axistype match {
      case X => List( AxisType.Lon, AxisType.GeoX, AxisType.RadialDistance ).contains(axisType)
      case Y => List( AxisType.Lat, AxisType.GeoY, AxisType.RadialAzimuth ).contains(axisType)
      case Z => List( AxisType.Pressure, AxisType.Height, AxisType.RadialElevation ).contains(axisType)
      case T => ( AxisType.Time == axisType )
    }
  }
}

object DomainContainer extends ContainerBase {
  
  def apply(metadata: Map[String, Any]): DomainContainer = {
    var items = new ListBuffer[ Option[DomainAxis] ]()
    try {
      val name = filterMap(metadata, key_equals("name")) match { case None => ""; case Some(x) => x.toString }
      items += DomainAxis( DomainAxis.Type.Y,   filterMap(metadata,  key_equals( wpsNameMatchers.yAxis )))
      items += DomainAxis( DomainAxis.Type.X,   filterMap(metadata,  key_equals( wpsNameMatchers.xAxis )))
      items += DomainAxis( DomainAxis.Type.Z,   filterMap(metadata,  key_equals( wpsNameMatchers.zAxis )))
      items += DomainAxis( DomainAxis.Type.T,   filterMap(metadata,  key_equals( wpsNameMatchers.tAxis )))
      val mask: Option[String] = filterMap(metadata, key_equals("mask")) match { case None => None; case Some(x) => Some(x.toString) }
      new DomainContainer( normalize(name.toString), items.flatten.toList, mask )
    } catch {
      case e: Exception =>
        logger.error("Error creating DomainContainer: " + e.getMessage )
        logger.error( e.getStackTrace.mkString("\n") )
        throw new Exception( e.getMessage, e )
    }
  }

  def empty( name: String ):  DomainContainer = new DomainContainer( name )
}

class WorkflowContainer(val operations: Iterable[OperationContext] = List() ) extends ContainerBase {
  def this( oc: OperationContext ) = this( List(oc) )
  override def toString = {
    s"WorkflowContainer { operations = $operations }"
  }
  override def toXml = {
    <workflow>  { operations.map( _.toXml ) }  </workflow>
  }
}

object WorkflowContainer extends ContainerBase {
  def apply(process_name: String, uid_list: List[String], metadata: Map[String, Any]): WorkflowContainer = {
    try {
      new WorkflowContainer( OperationContext( process_name, uid_list, metadata ) )
    } catch {
      case e: Exception =>
        val msg = "Error creating WorkflowContainer: " + e.getMessage
        logger.error(msg)
        throw e
    }
  }
}

class OperationContext( val identifier: String, val name: String, val rid: String, val inputs: List[String], private val configuration: Map[String,String] )  extends ContainerBase with ScopeContext  {
  def getConfiguration = configuration
  println( "OperationContext: " + rid )

  override def toString = {
    s"OperationContext { id = $identifier,  name = $name, rid = $rid, inputs = $inputs, configurations = $configuration }"
  }
  override def toXml = {
    <proc id={identifier} name={name} rid={rid} inputs={inputs.toString} configurations={configuration.toString}/>
  }
}

object OperationContext extends ContainerBase  {
  private val random = new Random( System.currentTimeMillis )
  var resultIndex = 0
  def apply( process_name: String, uid_list: List[String], metadata: Map[String, Any] ): OperationContext = {
    val op_inputs: List[String] = metadata.get( "input" ) match {
      case Some( input_values: List[_] ) => input_values.map( _.toString.trim.toLowerCase )
      case Some( input_value: String ) => List( input_value.trim.toLowerCase )
      case None => uid_list.map( _.trim.toLowerCase )
      case x => throw new Exception ( "Unrecognized input in operation spec: " + x.toString )
    }
    val op_name = metadata.getOrElse( "name", process_name ).toString.trim.toLowerCase
    val optargs: Map[String,String] = metadata.filterNot( (item) => List("input","name").contains(item._1) ).mapValues( _.toString.trim.toLowerCase )
    val input = metadata.getOrElse("input","").toString
    val opLongName = op_name + "-" + ( List( input ) ++ optargs.toList.map( item => item._1 + "=" + item._2 )).filterNot( (item) => item.isEmpty ).mkString("(","_",")")
    val dt: DateTime = new DateTime( DateTimeZone.getDefault() )
    val identifier: String = Array( opLongName, dt.toString("MM.dd-hh.mm.ss") ).mkString("-")
    val rid = metadata.getOrElse("result",identifier).toString
    new OperationContext( identifier = identifier, name=op_name, rid = rid, inputs = op_inputs, optargs )
  }
  def generateResultId: String = { resultIndex += 1; "$v"+resultIndex.toString }
}

class TaskProcessor {

}

