package org.shellinterview

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{GivenWhenThen, Tag}

class ManualE2ETestSpec extends AnyFlatSpec with GivenWhenThen  {

  info("As a Tester or Developer")
  info("I want to be publish 1,000 Messages to Kafka and Start our application consuming")

  object ManualTestTag extends Tag("manual-test")

  "A manual test" should "load 1,000 messages to the Kafka" taggedAs(ManualTestTag) in {

    Given("1,000 messages sent to Kafka")
//    val kafkaMessages: Seq[KafkaMessage] = Seq.fill(1000)(KafkaMessageGenerator.generate())

    When("The messages are published to the Kafka")
//    publishMessages(kafkaMessages, Seq("Manual test"))

    Then("The messages should be processed")
    And("The user should manually check if the messages has been created")
  }
}
