package io.vonsowic.e2e

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.vonsowic.*
import io.vonsowic.test.avro.Address
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.openqa.selenium.By
import org.openqa.selenium.chrome.ChromeDriver
import java.lang.Thread.sleep

const val NUM_OF_EVENTS_PER_PAGE_PER_PARTITION = 10

const val TOPIC_3 = "e2e-display-events-address-3"


@E2ETest
class DisplayEventsTest {


    @Topic("e2e-display-events-address-1")
    @Test
    fun `should show addresses from single partition`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, Address>,
        browser: ChromeDriver
    ) {
        repeat(100) {
            val address = randomAddress()
            producer.send(ProducerRecord("e2e-display-events-address-1", 0, address.id, address)).get()
        }

        browser.openMainPage()
        browser.getElement(By.xpath("//*[text()='e2e-display-events-address-1']")).click()

        assertSoftly {
            it.assertThat(browser.getElements(By.cssSelector("div.item")))
                .hasSize(NUM_OF_EVENTS_PER_PAGE_PER_PARTITION)
        }
    }

    @Topic("e2e-display-events-address-2")
    @Test
    fun `should show addresses from 2 partitions`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, Address>,
        browser: ChromeDriver
    ) {
        repeat(100) {
            val address = randomAddress()
            producer.send(ProducerRecord("e2e-display-events-address-2", it % 2, address.id, address)).get()
        }

        browser.openEventsPage("e2e-display-events-address-2")
        assertSoftly {
            it.assertThat(browser.getElements(By.cssSelector("div.item")))
                .hasSize(NUM_OF_EVENTS_PER_PAGE_PER_PARTITION * 2)
        }
    }

    @DisplayName(
        """
        1. Should display one event
        3. Second event is sent
        3. Start streaming
        4. Second event is display
        5. Third event is sent
        6. Third event is displayed
    """
    )
    @Topic(TOPIC_3)
    @Test
    fun testSSE(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, Address>,
        browser: ChromeDriver
    ) {
        val address = randomAddress()
        producer.send(ProducerRecord(TOPIC_3, address.id, address)).get()
        browser.openEventsPage(TOPIC_3)
        assertThat(browser.getElements(By.cssSelector("div.item"))).hasSize(1)

        producer.send(ProducerRecord(TOPIC_3, address.id, address)).get()
        sleep(1000)
        assertThat(browser.getElements(By.cssSelector("div.item"))).hasSize(1)

        browser.getElement(By.className("play")).click()
        sleep(1000)
        assertThat(browser.getElements(By.cssSelector("div.item"))).hasSize(2)

        producer.send(ProducerRecord(TOPIC_3, address.id, address)).get()
        sleep(1000)
        assertThat(browser.getElements(By.cssSelector("div.item"))).hasSize(3)
    }
}