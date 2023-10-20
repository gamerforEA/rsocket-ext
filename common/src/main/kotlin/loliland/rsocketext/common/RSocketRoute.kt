package loliland.rsocketext.common

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class RSocketRoute(val value: String)