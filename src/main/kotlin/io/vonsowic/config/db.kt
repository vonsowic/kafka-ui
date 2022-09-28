package io.vonsowic.config

//import io.micronaut.context.annotation.Factory
//import io.r2dbc.pool.ConnectionPool
//import io.r2dbc.pool.ConnectionPoolConfiguration
//import io.r2dbc.spi.ConnectionFactory
//import jakarta.inject.Singleton
//import java.time.Duration
//
//@Factory
//class DbFactory {
//
//    @Singleton
//    fun connectionPool(connectionFactory: ConnectionFactory): ConnectionPool =
//        ConnectionPoolConfiguration.builder(connectionFactory)
//            .maxIdleTime(Duration.ofMillis(1000))
//            .maxSize(20)
//            .build()
//            .let { ConnectionPool(it) }
//}