package nasa.nccs.esgf.process

import nasa.nccs.cdapi.cdm.{CDSDataset, CDSVariable, Collection, PartitionedFragment}
import ucar.{ma2, nc2}

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

case class ErrorReport(severity: String, message: String) {
  override def toString = {
    s"ErrorReport { severity: $severity, message: $message }"
  }

  def toXml = {
      <error severity={severity} message={message}/>
  }
}

class TaskRequest(val name: String, val variableMap : Map[String,DataContainer], val domainMap: Map[String,DomainContainer], val workflows: List[WorkflowContainer] = List(), val targetGridSpec: Map[String,String] ) {
  val errorReports = new ListBuffer[ErrorReport]()
  val logger = LoggerFactory.getLogger( this.getClass )
  validate()
//  logger.info( s"TaskRequest: name= $name, workflows= " + workflows.toString + ", variableMap= " + variableMap.toString + ", domainMap= " + domainMap.toString )

  def addErrorReport(severity: String, message: String) = {
    val error_rep = ErrorReport(severity, message)
    logger.error(error_rep.toString)
    errorReports += error_rep
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
      if ( !domainMap.contains(domid) ) {
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
    logger.info( "TaskRequest--> process_name: %s, datainputs: %s".format( process_name, datainputs.toString ) )
    val data_list: List[DataContainer] = datainputs.getOrElse("variable", List() ).map(DataContainer(_)).toList
    val domain_list: List[DomainContainer] = datainputs.getOrElse("domain", List()).map(DomainContainer(_)).toList
    val operation_list: List[WorkflowContainer] = datainputs.getOrElse("operation", List(Map("unparsed"->"()"))).map(WorkflowContainer(process_name,_)).toList
    val variableMap = buildVarMap( data_list, operation_list )
    val domainMap = buildDomainMap( domain_list )
    val gridSpec: Map[String,String] = Map( "id" -> datainputs.getOrElse("grid", data_list.head.uid ).toString )
    new TaskRequest( process_name, variableMap, domainMap, operation_list, gridSpec )
  }

  def buildVarMap( data: List[DataContainer], workflow: List[WorkflowContainer] ): Map[String,DataContainer] = {
    var data_var_items = for( data_container <- data ) yield ( data_container.uid -> data_container )
    var op_var_items = for( workflow_container<- workflow; operation<-workflow_container.operations; if !operation.result.isEmpty ) yield ( operation.result -> DataContainer(operation) )
    val var_map = Map( op_var_items ++ data_var_items: _* )
    logger.info( "Created Variable Map: " + var_map.toString )
    for( workflow_container<- workflow; operation<-workflow_container.operations; vid<-operation.inputs; if(!vid.isEmpty)  ) var_map.get( vid ) match {
      case Some(data_container) => data_container.addOpSpec( operation )
      case None => throw new Exception( "Unrecognized variable %s in varlist [%s]".format( vid, var_map.keys.mkString(",") ) )
    }
    var_map
  }

  def buildDomainMap( domain: List[DomainContainer] ): Map[String,DomainContainer] = {
    var domain_items = new ListBuffer[(String,DomainContainer)]()
    for( domain_container <- domain ) domain_items += ( domain_container.name -> domain_container )
    val domain_map = domain_items.toMap[String,DomainContainer]
    logger.info( "Created Domain Map: " + domain_map.toString )
    domain_map
  }
}

class ContainerBase {
  val logger = LoggerFactory.getLogger( this.getClass )
  def item_key(map_item: (String, Any)): String = map_item._1

  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase

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

object containerTest extends App {
  val c = new ContainerBase()
  val tval = Some( 4.7 )
  val fv = c.getGenericNumber( tval )
  println( fv )
}

class PartitionSpec( val axisIndex: Int, val nPart: Int, val partIndex: Int = 0 ) {
  override def toString =  s"PartitionSpec { axis = $axisIndex, nPart = $nPart, partIndex = $partIndex }"
}

class DataSource(val name: String, val collection: String, val domain: String ) {
  def this( dsource: DataSource ) = this( dsource.name, dsource.collection, dsource.domain )
  override def toString =  s"DataSource { name = $name, collection = $collection, domain = $domain }"
  def toXml = <dataset name={name} collection={collection.toString} domain={domain.toString}/>
  def isDefined = ( !collection.isEmpty && !name.isEmpty )
  def isReadable = ( !collection.isEmpty && !name.isEmpty && !domain.isEmpty )
}

class DataFragmentKey( val varname: String, val collection: String, val origin: Array[Int], val shape: Array[Int] ) {
  override def toString =  "%s:%s:%s:%s".format( varname, collection, origin.mkString(","), shape.mkString(","))
  def sameVariable( otherCollection: String, otherVarName: String ): Boolean = { (varname == otherVarName) && (collection == otherCollection) }
  def getRoi: ma2.Section = new ma2.Section(origin,shape)
  def equalRoi( df: DataFragmentKey ): Boolean = ( shape.sameElements(df.shape) && origin.sameElements(df.origin ) )
  def getSize: Int = shape.product
  def contains( df: DataFragmentKey ): Boolean = getRoi.contains( df.getRoi )
  def contains( df: DataFragmentKey, admitEquality: Boolean ): Boolean = if( admitEquality ) contains( df ) else containsSmaller( df )
  def containsSmaller( df: DataFragmentKey ): Boolean = ( !equalRoi( df ) && contains( df ) )
}

object DataFragmentKey {
  def parseArray( arrStr: String ): Array[Int] = { arrStr.split(',').map( _.toInt) }
  def apply( fkeyStr: String ): DataFragmentKey = {
    val toks = fkeyStr.split(':')
    new DataFragmentKey( toks(0), toks(1), parseArray(toks(2)), parseArray(toks(3)) )
  }
  def sameVariable( fkeyStr: String, otherCollection: String, otherVarName: String ): Boolean = {
    val toks = fkeyStr.split(':')
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

class DataFragmentSpec( val varname: String="", val collection: String="", val dimensions: String="", val units: String="", val longname: String="", val roi: ma2.Section = new ma2.Section(), val mask: Option[String] = None, val partitions: Array[PartitionSpec]= Array() )  {
  override def toString =  "DataFragmentSpec { varname = %s, collection = %s, dimensions = %s, units = %s, longname = %s, roi = %s, partitions = [ %s ] }".format( varname, collection, dimensions, units, longname, roi.toString, partitions.map(_.toString).mkString(", "))
  def sameVariable( otherCollection: String, otherVarName: String ): Boolean = { (varname == otherVarName) && (collection == otherCollection) }
  def toXml = {
    mask match {
      case None => <input collection={collection} varname={varname} longname={longname} units={units} roi={roi.toString} />
      case Some(maskId) => <input collection={collection} varname={varname} longname={longname} units={units} roi={roi.toString} mask={maskId} />
    }
  }

  private def collapse( range: ma2.Range, newsize: Int = 1 ): ma2.Range = newsize match {
    case 1 => val mid_val = (range.first+range.last)/2; new ma2.Range(range.getName,mid_val,mid_val)
    case ns => val incr = math.round((range.last-range.first)/ns.toFloat); new ma2.Range(range.getName,range.first(),range.last,incr)
  }
  def getRange( dimension_name: String ): Option[ma2.Range] = {
    val dims = dimensions.toLowerCase.split(' ')
    dims.indexOf( dimension_name.toLowerCase ) match {
      case -1 => None
      case x => Some( roi.getRange( x ) )
    }
  }

  def getKey: DataFragmentKey = {
    new DataFragmentKey( varname, collection, roi.getOrigin, roi.getShape )
  }
  def getSize: Int = roi.getShape.product

  def getKeyString: String = getKey.toString

  def cutIntersection( cutSection: ma2.Section ): DataFragmentSpec =
    new DataFragmentSpec( varname, collection, dimensions, units, longname, roi.intersect(cutSection), mask, partitions )

  def getReducedSection( axisIndices: Set[Int], newsize: Int = 1 ): ma2.Section = {
    new ma2.Section( roi.getRanges.zipWithIndex.map( rngIndx => if( axisIndices(rngIndx._2) ) collapse( rngIndx._1, newsize ) else rngIndx._1 ):_* )
  }

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
      "varname" -> new nc2.Attribute("varname",varname), "collection" -> new nc2.Attribute("collection",collection) )
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

  def reSection( newSection: ma2.Section ): DataFragmentSpec = {
    new DataFragmentSpec( varname, collection, dimensions, units, longname, newSection, mask, partitions )
  }

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
  assert( source.isDefined || operation.isDefined, "Empty DataContainer: variable uid = $uid" )
  assert( source.isEmpty || operation.isEmpty, "Conflicted DataContainer: variable uid = $uid" )
  private val optSpecs = mutable.ListBuffer[ OperationSpecs ]()

  override def toString = {
    val embedded_val: String = if ( source.isDefined ) source.get.toString else operation.get.toString
    s"DataContainer ( $uid ) { $embedded_val }"
  }
  override def toXml = {
    val embedded_xml = if ( source.isDefined ) source.get.toXml else operation.get.toXml
    <dataset uid={uid}> embedded_xml </dataset>
  }
  def isSource = source.isDefined && source.get.isDefined

  def isOperation = operation.isDefined
  def getSource = {
    assert( isSource, s"Attempt to access an operation based DataContainer($uid) as a data source")
    source.get
  }
  def getOperation = {
    assert( isOperation, s"Attempt to access a source based DataContainer($uid) as an operation")
    operation.get
  }

  def addOpSpec( operation: OperationContext ): Unit = {
    def mergeOpSpec( oSpecList: mutable.ListBuffer[ OperationSpecs ], oSpec: OperationSpecs ): Unit = oSpecList.headOption match {
      case None => oSpecList += oSpec
      case Some(head) => if( head == oSpec ) head merge oSpec else mergeOpSpec(oSpecList.tail,oSpec)
    }
    mergeOpSpec( optSpecs, OperationSpecs(operation) )
  }
  def getOpSpecs: List[OperationSpecs] = optSpecs.toList
}

object DataContainer extends ContainerBase {
  def apply( operation: OperationContext ): DataContainer = {
      new DataContainer( uid=operation.result, operation=Some(operation) )
  }
  def apply(metadata: Map[String, Any]): DataContainer = {
    try {
      val uri = filterMap(metadata, key_equals("uri")) match { case None => ""; case Some(x) => x.toString }
      val fullname = filterMap(metadata, key_equals("name")) match { case None => ""; case Some(x) => x.toString }
      val domain = filterMap(metadata, key_equals("domain")) match { case None => ""; case Some(x) => x.toString }
      val name_items = fullname.toString.split(':')
      val dsource = new DataSource( normalize(name_items.head), uri, normalize(domain) )
      new DataContainer(normalize(name_items.last), source = Some(dsource) )
    } catch {
      case e: Exception =>
        logger.error("Error creating DataContainer: " + e.getMessage  )
        logger.error( e.getStackTrace.mkString("\n") )
        throw new Exception( e.getMessage, e )
    }
  }

  def parseUri( uri: String ): String = {
    if(uri.isEmpty) "" else {
      val recognizedUrlTypes = List( "file", "collection" )
      val uri_parts = uri.split("://")
      val url_type = normalize(uri_parts.head)
      if ( recognizedUrlTypes.contains(url_type) && (uri_parts.length == 2) ) uri_parts.last
      else throw new Exception("Unrecognized uri format: " + uri + ", type = " + uri_parts.head + ", nparts = " + uri_parts.length.toString + ", value = " + uri_parts.last)
    }
  }
}

class DomainContainer( val name: String, val axes: List[DomainAxis], val mask: Option[String] ) extends ContainerBase {
  override def toString = {
    s"DomainContainer { name = $name, axes = $axes }"
  }
  override def toXml = {
    <domain name={name}>
      <axes> { axes.map( _.toXml ) } </axes>
      { mask match { case None => Unit; case Some(maskId) => <mask> { maskId } </mask> } }
    </domain>
  }
}

object DomainAxis extends ContainerBase {
  object Type extends Enumeration { val Lat, Lon, Lev, X, Y, Z, T = Value }

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
  def getCFAxisName(): String = axistype match { case Lat => "Y"; case Lon => "X"; case Lev => "Z"; case X => "X"; case Y => "Y"; case Z => "Z"; case T => "T" }

  override def toString = {
    s"DomainAxis { name = $name, start = $start, end = $end, system = $system, bounds = $bounds }"
  }

  override def toXml = {
    <axis name={name} start={start.toString} end={end.toString} system={system} bounds={bounds} />
  }
}

object DomainContainer extends ContainerBase {
  
  def apply(metadata: Map[String, Any]): DomainContainer = {
    var items = new ListBuffer[ Option[DomainAxis] ]()
    try {
      val name = filterMap(metadata, key_equals("name")) match { case None => ""; case Some(x) => x.toString }
      items += DomainAxis( DomainAxis.Type.Lat, filterMap(metadata,  key_equals( wpsNameMatchers.latAxis )))
      items += DomainAxis( DomainAxis.Type.Lon, filterMap(metadata,  key_equals( wpsNameMatchers.lonAxis )))
      items += DomainAxis( DomainAxis.Type.Lev, filterMap(metadata,  key_equals( wpsNameMatchers.levAxis )))
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
}

class WorkflowContainer(val operations: Iterable[OperationContext] = List() ) extends ContainerBase {
  override def toString = {
    s"WorkflowContainer { operations = $operations }"
  }
  override def toXml = {
    <workflow>  { operations.map( _.toXml ) }  </workflow>
  }
}

object WorkflowContainer extends ContainerBase {
  def apply(process_name: String, metadata: Map[String, Any]): WorkflowContainer = {
    try {
      import nasa.nccs.esgf.utilities.wpsOperationParser
      val parsed_data_inputs = wpsOperationParser.parseOp(metadata("unparsed").toString)
      new WorkflowContainer( parsed_data_inputs.map(OperationContext(process_name,_)))
    } catch {
      case e: Exception =>
        val msg = "Error creating WorkflowContainer: " + e.getMessage
        logger.error(msg)
        throw e
    }
  }
}

class OperationContext( val identifier: String, val name: String, val result: String, val inputs: List[String], private val configuration: Map[String,String] )  extends ContainerBase with ScopeContext  {
  def getConfiguration = configuration
  override def toString = {
    s"OperationContext { id = $identifier,  name = $name, result = $result, inputs = $inputs, configurations = $configuration }"
  }
  override def toXml = {
    <proc id={identifier} name={name} result={result} inputs={inputs.toString} configurations={configuration.toString}/>
  }
}

object OperationContext extends ContainerBase  {
  def apply(process_name: String, raw_metadata: Any): OperationContext = {
    raw_metadata match {
      case (ident: String, args: List[_]) =>
        val varlist = new ListBuffer[String]()
        val optargs = new ListBuffer[(String,String)]()
        for( raw_arg<-args; arg=raw_arg.toString ) {
          if(arg contains ":") {
            val arg_items = arg.split(":").map( _.trim.toLowerCase )
            optargs += ( if( arg_items.length > 1 ) ( arg_items(0) -> arg_items(1) ) else ( arg_items(0) -> "" ) )
          }
          else varlist += arg.trim.toLowerCase
        }
        val ids = ident.split("~").map( _.trim.toLowerCase )
        ids.length match {
          case 1 =>
            new OperationContext( identifier = ident, name=process_name, result = ids(0), inputs = varlist.toList, Map(optargs:_*) )
          case 2 =>
            val op_name = if( ids(0).nonEmpty ) ids(0) else process_name
            val identifier = if( ids(0).nonEmpty ) ident else process_name + ident
            new OperationContext( identifier = identifier, name = op_name, result = ids(1), inputs = varlist.toList, Map(optargs:_*) )
          case _ =>
            val msg = "Unrecognized format for Operation id: " + ident
            logger.error(msg)
            throw new Exception(msg)
        }
      case _ =>
        val msg = "Unrecognized format for OperationContext: " + raw_metadata.toString
        logger.error(msg)
        throw new Exception(msg)
    }
  }
}


class TaskProcessor {

}

object enumTest extends App {
  object Type extends Enumeration { val Lat, Lon, Lev, X, Y, Z, T = Value }

  val strType = Type.Lat.toString
  println( strType )

}
