package org.codefeedr.plugins.travis.util

object TravisExceptions {

  final case class CouldNotGetResourceException(private val message: String = "",
                                                private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

  final case class CouldNotExtractException(private val message: String = "",
                                            private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

  final case class RepoNotActiveException(private val message: String = "",
                                          private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

  final case class BuildNotFoundForTooLongException(private val message: String = "",
                                                    private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

  final case class CouldNotAccessTravisBuildInfo(private val message: String = "",
                                                    private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

}
