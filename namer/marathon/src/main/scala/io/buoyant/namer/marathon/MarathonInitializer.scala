package io.buoyant.namer.marathon

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.conversions.time._
import com.twitter.finagle.param.Label
import com.twitter.finagle.tracing.NullTracer
import com.twitter.finagle.{Stack, Http, Path}
import com.twitter.io.Buf
import com.twitter.util.Return
import io.buoyant.config.types.Port
import io.buoyant.namer.{NamerConfig, NamerInitializer}
import io.buoyant.marathon.v2.{Api, AppIdNamer}

/**
 * Supports namer configurations in the form:
 *
 * <pre>
 * namers:
 * - kind:      io.l5d.marathon
 *   experimental: true
 *   prefix:    /io.l5d.marathon
 *   host:      marathon.mesos
 *   port:      80
 *   uriPrefix: /marathon
 *   ttlMs:     5000
 * </pre>
 */
class MarathonInitializer extends NamerInitializer {
  val configClass = classOf[MarathonConfig]
  override def configId = "io.l5d.marathon"
}

object MarathonInitializer extends MarathonInitializer

case class MarathonSecret(
  login_endpoint: Option[String],
  private_key: Option[String],
  scheme: Option[String],
  uid: Option[String]
)

case class MarathonConfig(
  host: Option[String],
  port: Option[Port],
  dst: Option[String],
  uriPrefix: Option[String],
  ttlMs: Option[Int]
) extends NamerConfig {
  @JsonIgnore
  override val experimentalRequired = true

  @JsonIgnore
  override def defaultPrefix: Path = Path.read("/io.l5d.marathon")

  private[this] def getHost = host.getOrElse("marathon.mesos")
  private[this] def getPort = port match {
    case Some(p) => p.port
    case None => 80
  }
  private[this] def getUriPrefix = uriPrefix.getOrElse("")
  private[this] def getTtl = ttlMs.getOrElse(5000).millis

  private[this] def getDst = dst.getOrElse(s"/$$/inet/$getHost/$getPort")

  private[this] val secretKey = "DCOS_SERVICE_ACCOUNT_CREDENTIAL"
  private[this] def getAuth =
    sys.env.get(secretKey).flatMap { secret =>
      Api.readJson[MarathonSecret](Buf.Utf8(secret)).toOption
    }.collect {
      case MarathonSecret(Some(loginEndpoint), Some(privateKey), Some("RS256"), Some(uid)) =>
        Api.AuthRequest(loginEndpoint, uid, privateKey)
    }

  /**
   * Construct a namer.
   */
  def newNamer(params: Stack.Params) = {
    val service = Http.client
      .withParams(params)
      .configured(Label("namer" + prefix.show))
      .withTracer(NullTracer)
      .newService(getDst)

    new AppIdNamer(Api(service, getHost, getUriPrefix, getAuth), prefix, getTtl)
  }
}
