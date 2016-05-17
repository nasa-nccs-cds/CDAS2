import collection.mutable.Stack
import org.scalatest._

class ExampleSpec extends FunSuite with Matchers {

  test( "Stack Test" ) {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    assert( stack.pop() == 2,  ": A Stack should pop values in last-in-first-out order" )
    assert( stack.pop() == 2,  ": A Stack should pop values in last-in-first-out order"  )
  }
}

