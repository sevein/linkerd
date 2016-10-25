package io.buoyant.marathon.v2

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.{Address, Path, Service, SimpleFilter, http}
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._
import java.net.URL
import pdi.jwt.{Jwt, JwtAlgorithm}

/**
 * A partial implementation of the Marathon V2 API:
 * https://mesosphere.github.io/marathon/docs/generated/api.html#v2_apps
 */

trait Api {
  def getAppIds(): Future[Api.AppIds]
  def getAddrs(app: Path): Future[Set[Address]]
}

object Api {

  type AppIds = Set[Path]
  type Client = Service[http.Request, http.Response]

  val versionString = "v2"

  case class AuthRequest(
    loginEndpoint: String,
    uid: String,
    privateKey: String
  ) {
    private[this] val jwt = Jwt.encode(s"""{"uid":"$uid"}""", privateKey, JwtAlgorithm.RS256)

    val path = new URL(loginEndpoint).getPath
    val jwtToken = s"""{"uid":"$uid","token":"$jwt"}"""
  }

  def apply(client: Client, host: String, uriPrefix: String, auth: Option[AuthRequest]): Api =
    new AppIdApi(SetHost(host).andThen(client), s"$uriPrefix/$versionString", auth)

  private[v2] val log = Logger.get(getClass.getName)

  private[v2] def rspToApps(rsp: http.Response): Future[Api.AppIds] =
    rsp.status match {
      case http.Status.Ok =>
        val apps = readJson[AppsRsp](rsp.content).map(_.toApps)
        Future.const(apps)
      case http.Status.Unauthorized =>
        val e = UnauthorizedResponse(rsp)
        log.error(s"rspToApps returned Unauthorized with $e")
        Future.exception(e)
      case _ =>
        val e = UnexpectedResponse(rsp)
        log.error(s"rspToApps returned Unexpected with $e")
        Future.exception(e)
    }

  private[v2] def rspToAddrs(rsp: http.Response): Future[Set[Address]] =
    rsp.status match {
      case http.Status.Ok =>
        val addrs = readJson[AppRsp](rsp.content).map(_.toAddresses)
        Future.const(addrs)
      case http.Status.Unauthorized =>
        val e = UnauthorizedResponse(rsp)
        log.error(s"rspToAddrs returned Unauthorized with $e")
        Future.exception(e)
      case _ =>
        val e = UnexpectedResponse(rsp)
        log.error(s"rspToAddrs returned Unexpected with $e")
        Future.exception(e)
    }

  private[v2] case class AuthToken(token: Option[String])

  private[v2] case class UnauthorizedResponse(rsp: http.Response) extends Throwable

  private[v2] case class UnexpectedResponse(rsp: http.Response) extends Throwable

  private[this] val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  def readJson[T: Manifest](buf: Buf): Try[T] = {
    val Buf.ByteArray.Owned(bytes, begin, end) = Buf.ByteArray.coerce(buf)
    Try(mapper.readValue[T](bytes, begin, end - begin))
  }

  private[this] case class SetHost(host: String)
    extends SimpleFilter[http.Request, http.Response] {

    def apply(req: http.Request, service: Service[http.Request, http.Response]) = {
      req.host = host
      service(req)
    }
  }

  private[this] case class Task(
    id: Option[String],
    host: Option[String],
    ports: Option[Seq[Int]]
  )

  private[this] case class App(
    id: Option[String],
    tasks: Option[Seq[Task]]
  )

  private[this] case class AppsRsp(apps: Option[Seq[App]] = None) {

    def toApps: Api.AppIds =
      apps match {
        case Some(apps) =>
          apps.collect { case App(Some(id), _) => Path.read(id) }.toSet
        case None => Set.empty
      }
  }

  private[this] case class AppRsp(app: Option[App] = None) {

    def toAddresses: Set[Address] =
      app match {
        case Some(App(_, Some(tasks))) =>
          tasks.collect {
            case Task(_, Some(host), Some(Seq(port, _*))) =>
              Address(host, port)
          }.toSet

        case _ => Set.empty
      }
  }
}

private class AppIdApi(client: Api.Client, apiPrefix: String, auth: Option[Api.AuthRequest])
  extends Api
  with Closable {

  import Api._

  def close(deadline: Time) = client.close(deadline)

  private[this] val authTokenState = Var[Activity.State[Option[String]]](Activity.Ok(None))
  private[this] val authToken = Activity(authTokenState)

  private[this] def handleTokenResponse(rsp: Try[http.Response]): Future[String] = rsp match {
    case Return(rsp) =>
      rsp.status match {
        case http.Status.Ok =>
          readJson[AuthToken](rsp.content) match {
            case Return(AuthToken(Some(token))) =>
              authTokenState() = Activity.Ok(Some(token))
              Future.value(token)
            case Throw(e) =>
              authTokenState() = Activity.Ok(None)
              log.error(s"refreshToken threw $e")
              Future.exception(e)
            case _ =>
              authTokenState() = Activity.Ok(None)
              val e = UnexpectedResponse(rsp)
              log.error(s"refreshToken returned Unexpected with $e")
              Future.exception(e)
          }
        case http.Status.Unauthorized =>
          authTokenState() = Activity.Ok(None)
          val e = UnauthorizedResponse(rsp)
          log.error(s"refreshToken returned Unauthorized with $e")
          Future.exception(e)
        case _ =>
          authTokenState() = Activity.Ok(None)
          val e = UnexpectedResponse(rsp)
          log.error(s"refreshToken returned Unexpected with $e")
          Future.exception(e)
      }
    case Throw(e) =>
      authTokenState() = Activity.Ok(None)
      log.error(s"refreshToken threw $e")
      Future.exception(e)
  }

  // Given AuthRequest data, attempts authentication on a marathon login
  // endpoint. Upon success, returns a short-lived authToken, and also saves
  // authToken for subsequent API calls.
  private[this] def refreshToken(): Future[String] =
    auth match {
      case None => Future.value("")
      case Some(auth) => synchronized {
        authTokenState.sample() match {
          case Activity.Pending =>
            // Another call to refreshToken() is currently executing, wait for that result.
            authToken.values.toFuture.flatMap {
              case Return(Some(token)) => Future.value(token)
              case Return(None) =>
                log.error(s"authToken was None")
                Future.exception(new Exception("authToken was None"))
              case Throw(e) =>
                log.error(s"authToken threw $e")
                Future.exception(e)
            }
          case _ =>
            // We need a token in the following cases:
            // 1) No token present
            // 2) Token present, but expired.
            // The caller of refreshToken should know this.
            authTokenState() = Activity.Pending
            val req = http.Request(http.Method.Post, auth.path)
            req.setContentTypeJson()
            req.setContentString(auth.jwtToken)

            Trace.letClear(client(req)).transform(handleTokenResponse(_))
        }
      }
    }

  private[this] def prepRequest(path: String): Future[http.Request] = {
    val req = http.Request(path)
    auth match {
      case None =>
        // no auth data, don't need a token
        Future.value(req)
      case Some(auth) =>
        // we have auth data, get a token
        authToken.values.toFuture.flatMap {
          case Return(Some(token)) =>
            req.headerMap.set("Authorization", s"token=$token")
            Future.value(req)
          case Return(None) =>
            refreshToken().map { token =>
              req.headerMap.set("Authorization", s"token=$token")
              req
            }
          case Throw(e) =>
            log.error(s"prepRequest threw $e")
            Future.exception(e)
        }
    }
  }

  def getAppIds(): Future[Api.AppIds] =
    prepRequest(s"$apiPrefix/apps").flatMap { req =>
      Trace.letClear(client(req)).flatMap(rspToApps(_)).rescue {
        case e: UnauthorizedResponse =>
          // cached token may have expired, try again
          refreshToken().flatMap { token =>
            req.headerMap.set("Authorization", s"token=$token")
            Trace.letClear(client(req)).flatMap(rspToApps(_))
          }
      }
    }

  def getAddrs(app: Path): Future[Set[Address]] =
    prepRequest(s"$apiPrefix/apps${app.show}").flatMap { req =>
      Trace.letClear(client(req)).flatMap(rspToAddrs(_)).rescue {
        case e: UnauthorizedResponse =>
          // cached token may have expired, try again
          refreshToken().flatMap { token =>
            req.headerMap.set("Authorization", s"token=$token")
            Trace.letClear(client(req)).flatMap(rspToAddrs(_))
          }
      }
    }
}
