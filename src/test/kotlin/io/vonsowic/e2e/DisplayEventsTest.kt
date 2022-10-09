package io.vonsowic.e2e

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.vonsowic.*
import io.vonsowic.test.avro.Address
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.Test
import org.openqa.selenium.By
import org.openqa.selenium.chrome.ChromeDriver

const val NUM_OF_EVENTS_PER_PAGE_PER_PARTITION = 10

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
}