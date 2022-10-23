package io.vonsowic.e2e

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.vonsowic.*
import io.vonsowic.test.avro.Address
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.openqa.selenium.By
import org.openqa.selenium.chrome.ChromeDriver
import java.lang.Thread.sleep
import java.util.*

const val NUM_OF_EVENTS_PER_PAGE_PER_PARTITION = 10

const val TOPIC_3 = "e2e-display-events-address-3"
const val TOPIC_4 = "e2e-display-events-address-4"
const val TOPIC_5 = "e2e-display-events-address-5"


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
            it.assertThat(browser.getElements(By.cssSelector("div.card")))
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
            it.assertThat(browser.getElements(By.cssSelector("div.card")))
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
        assertThat(browser.getElements(By.cssSelector("div.card"))).hasSize(1)

        producer.send(ProducerRecord(TOPIC_3, address.id, address)).get()
        sleep(1000)
        assertThat(browser.getElements(By.cssSelector("div.card"))).hasSize(1)

        browser.getElement(By.className("play")).click()
        sleep(1000)
        assertThat(browser.getElements(By.cssSelector("div.card"))).hasSize(2)

        producer.send(ProducerRecord(TOPIC_3, address.id, address)).get()
        sleep(1000)
        assertThat(browser.getElements(By.cssSelector("div.card"))).hasSize(3)

        browser.getElement(By.className("pause")).click()
        sleep(1000)
        assertThat(browser.getElements(By.cssSelector("div.card"))).hasSize(3)

        producer.send(ProducerRecord(TOPIC_3, address.id, address)).get()
        sleep(1000)
        assertThat(browser.getElements(By.cssSelector("div.card"))).hasSize(3)
    }

    @Topic(TOPIC_4)
    @Test
    fun `should automatically add new page items on page selector`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, Address>,
        browser: ChromeDriver
    ) {
        repeat(NUM_OF_EVENTS_PER_PAGE_PER_PARTITION) {
            val address = randomAddress()
            producer.send(ProducerRecord(TOPIC_4, 0, address.id, address)).get()
        }

        browser.openEventsPage(TOPIC_4)
        sleep(1000)

        // one page + 5 technical buttons
        assertThat(browser.getElements(By.cssSelector("a.item"))).hasSize(7)

        browser.getElement(By.className("play")).click()
        repeat(NUM_OF_EVENTS_PER_PAGE_PER_PARTITION) {
            val address = randomAddress()
            producer.send(ProducerRecord(TOPIC_4, 0, address.id, address)).get()
        }
        sleep(1000)
        // two pages + 5 technical buttons
        assertThat(browser.getElements(By.cssSelector("a.item"))).hasSize(8)
    }

    @Topic(TOPIC_5)
    @Test
    fun `should use search input to display events matching criteria`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, SpecificRecord>,
        browser: ChromeDriver
    ) {
        val searchedValue = randomUUID()
        repeat(100) {
            producer.send(ProducerRecord(TOPIC_5, randomUUID(), randomUberAvro())).get()
        }

        randomUberAvro()
            .let { uberAvro ->
                uberAvro.aString = "${UUID.randomUUID()}-$searchedValue-${UUID.randomUUID()}".uppercase()
                producer.send(ProducerRecord(TOPIC_5, randomUUID(), uberAvro)).get()
            }

        repeat(100) {
            producer.send(ProducerRecord(TOPIC_5, randomUUID(), randomUberAvro())).get()
        }

        randomUberAvro()
            .let { uberAvro ->
                val key = "${UUID.randomUUID()}-$searchedValue-${UUID.randomUUID()}".lowercase()
                producer.send(ProducerRecord(TOPIC_5, key, uberAvro)).get()
            }

        repeat(100) {
            producer.send(ProducerRecord(TOPIC_5, randomUUID(), randomUberAvro())).get()
        }

        browser.openEventsPage(TOPIC_5)
        sleep(1000)

        browser.getElement(By.tagName("input"))
            .run {
                sendKeys(searchedValue)
                submit()
            }

        sleep(1000)
        assertThat(browser.getElements(By.className("card"))).hasSize(2)
    }
}