package io.vonsowic

import io.github.bonigarcia.wdm.WebDriverManager
import org.junit.jupiter.api.extension.*
import org.openqa.selenium.By
import org.openqa.selenium.WebElement
import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.chrome.ChromeOptions
import org.openqa.selenium.support.ui.WebDriverWait
import java.time.Duration


@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
@IntegrationTest
@ExtendWith(SeleniumExtension::class)
annotation class E2ETest

class SeleniumExtension : BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private lateinit var chromeDriver: ChromeDriver

    override fun beforeAll(context: ExtensionContext?) {
        WebDriverManager.chromedriver().setup()
        val options = ChromeOptions()
        if (isHeadless()) {
            options.addArguments("--headless")
        }

        chromeDriver = ChromeDriver(options)
    }

    private fun isHeadless(): Boolean =
        System.getenv("HEADLESS")?.toBoolean() ?: false

    override fun afterAll(context: ExtensionContext?) {
        chromeDriver.close()
    }

    override fun supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Boolean {
        return parameterContext.parameter.name.lowercase().contains("browser")
    }

    override fun resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Any {
        return chromeDriver
    }
}

fun ChromeDriver.openMainPage() = get("http://localhost:8080")

fun ChromeDriver.openEventsPage(topic: String) = get("http://localhost:8080/topics/${topic}")


fun ChromeDriver.getElements(by: By, duration: Duration = Duration.ofSeconds(5)): List<WebElement> {
    WebDriverWait(this, duration).until { it.findElements(by).isNotEmpty() }
    return findElements(by)
}

fun ChromeDriver.getElement(by: By, duration: Duration = Duration.ofSeconds(5)): WebElement {
    WebDriverWait(this, duration).until { it.findElements(by).isNotEmpty() }
    return findElement(by)
}