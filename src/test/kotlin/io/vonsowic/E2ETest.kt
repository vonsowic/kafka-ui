package io.vonsowic

import io.github.bonigarcia.wdm.WebDriverManager
import org.junit.jupiter.api.extension.*
import org.openqa.selenium.chrome.ChromeDriver


@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
@IntegrationTest
@ExtendWith(SeleniumExtension::class)
annotation class E2ETest

class SeleniumExtension : BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private lateinit var chromeDriver: ChromeDriver

    override fun beforeAll(context: ExtensionContext?) {
        WebDriverManager.chromedriver().setup()
        chromeDriver = ChromeDriver()
    }

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
