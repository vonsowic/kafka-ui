tasks.register("clean", Exec::class) {
    group = "build"
    commandLine("rm", "-rf", project.buildDir)
}

tasks.register("install", Exec::class) {
    group = "build"
    commandLine("npm", "install")
}

tasks.register("build", Exec::class) {
    group = "build"
    commandLine("npm", "run", "build")
    dependsOn(tasks["install"])
}

tasks.register("test", Exec::class) {
    group = "verification"
//    commandLine("npm", "test", "--no-watch")
    commandLine("echo", "no test executed")
    dependsOn(tasks["build"])
}