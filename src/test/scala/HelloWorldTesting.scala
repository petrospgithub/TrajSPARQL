import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HelloWorldTesting  extends FunSuite {

  def str()= {
    println("Hello World")
  }

  test("Run Single Test file") {
    println("Hello World")
    str()
  }
}
/*
mvn test -Dtest=HelloWorldTesting

mvn test -Dtest=HelloWorldTesting -q //print only warnings! changes log level!!!

 */