package io.vonsowic.utils

import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Status

class ClientException(message: String, val status: HttpStatus = HttpStatus.BAD_REQUEST) : RuntimeException(message)

class AppException(message: String) : RuntimeException(message)