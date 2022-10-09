package io.vonsowic.e2e

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.vonsowic.E2ETest
import io.vonsowic.ProducerOptions
import io.vonsowic.Topic
import io.vonsowic.randomAddress
import io.vonsowic.test.avro.Address
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.Test
import org.openqa.selenium.By
import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.support.ui.WebDriverWait
import java.time.Duration

const val NUM_OF_EVENTS_PER_PAGE_PER_PARTITION = 5

@E2ETest
class DisplayEventsTest {


    @Topic("e2e-display-events-address-1")
    @Test
    fun `should show addresses from single partition`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, Address>,
        browser: ChromeDriver
    ) {
        repeat(10) {
            val address = randomAddress()
            producer.send(ProducerRecord("e2e-display-events-address-1", 0, address.id, address)).get()
        }

        browser.get("http://localhost:8080")
        WebDriverWait(browser, Duration.ofSeconds(5))
            .until { it.findElements(By.xpath("//*[text()='e2e-display-events-address-1']")).isNotEmpty() }
        browser.findElement(By.xpath("//*[text()='e2e-display-events-address-1']")).click()

        WebDriverWait(browser, Duration.ofSeconds(4))
            .until { it.findElements(By.cssSelector("div.item")).isNotEmpty() }
        assertSoftly {
            it.assertThat(browser.findElements(By.cssSelector("div.item")))
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
        repeat(10) {
            val address = randomAddress()
            producer.send(ProducerRecord("e2e-display-events-address-2", it % 2, address.id, address)).get()
        }

        browser.get("http://localhost:8080/topics/e2e-display-events-address-2")
        WebDriverWait(browser, Duration.ofSeconds(4))
            .until { it.findElements(By.cssSelector("div.item")).isNotEmpty() }
        assertSoftly {
            it.assertThat(browser.findElements(By.cssSelector("div.item")))
                .hasSize(NUM_OF_EVENTS_PER_PAGE_PER_PARTITION * 2)
        }
    }
}