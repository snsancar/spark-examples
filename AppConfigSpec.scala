package org.shellinterview

import com.typesafe.config.ConfigFactory
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success}

class AppConfigSpec extends AnyWordSpec
  with Matchers
  with ArgumentMatchersSugar
  with IdiomaticMockito {

  "fromConfig" should {

    "succeed" in {

      val configString =
        """
          |application {
          |  data-input-path: "/some/input/path"
          |  data-output-path: "/some/output/path"
          |}
          |""".stripMargin

      val expected = AppConfig(
        Application("/some/input/path",
          "/some/output/path")
      )

      val result = AppConfig.fromConfig(ConfigFactory.parseString(configString).resolve())
      result shouldBe Success(expected)
//      result shouldBe a[Failure[_]]
    }
  }

}
