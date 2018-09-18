package src.test

import java.io.InputStream

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object ParameterTest {

  def main(args: Array[String]): Unit = {

    // 读取parameters
    val stream : InputStream = getClass.getResourceAsStream("/parameters.json")
    implicit val formats = DefaultFormats

    val parameters = parse(stream)

    println((parameters \ "smooth_parameter" \ "follow_rate_in_addfollow" \ "alpha").extract[Int])

  }

}
