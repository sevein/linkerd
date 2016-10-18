package io.buoyant.marathon.v2

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.{Address, Path, Service, SimpleFilter, http}
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future, Return, Time, Try}

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

  case class Auth(
    path: String,
    content: String
  )

  private[this] case class SetHost(host: String)
    extends SimpleFilter[http.Request, http.Response] {

    def apply(req: http.Request, service: Service[http.Request, http.Response]) = {
      req.host = host
      service(req)
    }
  }

  def apply(client: Client, host: String, uriPrefix: String, auth: Option[Auth]): Api =
    new AppIdApi(SetHost(host).andThen(client), s"$uriPrefix/$versionString", auth)

  private[v2] def rspToApps(rsp: http.Response): Future[Api.AppIds] =
    rsp.status match {
      case http.Status.Ok =>
        val apps = readJson[AppsRsp](rsp.content).map(_.toApps)
        Future.const(apps)
      case http.Status.Unauthorized =>
        Future.exception(UnauthorizedResponse(rsp))
      case _ => Future.exception(UnexpectedResponse(rsp))
    }

  private[v2] def rspToAddrs(rsp: http.Response): Future[Set[Address]] =
    rsp.status match {
      case http.Status.Ok =>
        val addrs = readJson[AppRsp](rsp.content).map(_.toAddresses)
        Future.const(addrs)
      case http.Status.Unauthorized =>
        Future.exception(UnauthorizedResponse(rsp))
      case _ => Future.exception(UnexpectedResponse(rsp))
    }

  private[v2] case class AuthToken(token: Option[String])

  private[v2] case class UnauthorizedResponse(rsp: http.Response) extends Throwable

  private[this] case class UnexpectedResponse(rsp: http.Response) extends Throwable

  private[this] val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  def readJson[T: Manifest](buf: Buf): Try[T] = {
    val Buf.ByteArray.Owned(bytes, begin, end) = Buf.ByteArray.coerce(buf)
    Try(mapper.readValue[T](bytes, begin, end - begin))
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

private class AppIdApi(client: Api.Client, apiPrefix: String, auth: Option[Api.Auth])
  extends Api
  with Closable {

  import Api._

  var authToken: Option[String] = None

  def close(deadline: Time) = client.close(deadline)

  private[this] def refreshToken(): Future[String] =
    auth match {
      case Some(auth) =>
        val req = http.Request(http.Method.Post, auth.path)
        req.setContentTypeJson()
        req.setContentString(auth.content)

        Trace.letClear(client(req)).flatMap { rsp =>
          rsp.status match {
            case http.Status.Ok =>
              readJson[AuthToken](rsp.content) match {
                case Return(AuthToken(Some(token))) =>
                  authToken = Some(token) // side effect
                  Future.const(Return(token))
                case _ => Future.exception(UnauthorizedResponse(rsp))
              }
            case _ => Future.exception(UnauthorizedResponse(rsp))
          }
        }
      case _ => Future.value("")
    }

  private[this] def prepRequest(path: String): Future[http.Request] = {
    val req = http.Request(path)
    (authToken, auth) match {
      case (Some(authToken), Some(auth)) => {
        // already have a token, set it
        req.headerMap.set("Authorization", s"token=$authToken")
        Future.const(Return(req))
      }
      case (None, Some(auth)) => {
        // need a token, query for it
        refreshToken().flatMap { token =>
          req.headerMap.set("Authorization", s"token=$token")
          Future.const(Return(req))
        }
      }

      // don't need a token
      case _ => Future.const(Return(req))
    }
  }

  def getAppIds(): Future[Api.AppIds] =
    prepRequest(s"$apiPrefix/apps").flatMap { req =>
      try {
        Trace.letClear(client(req)).flatMap(rspToApps(_)).rescue {
          case e: UnauthorizedResponse =>
            // token may have expired, try again
            refreshToken().flatMap { token =>
              req.headerMap.set("Authorization", s"token=$token")
              Trace.letClear(client(req)).flatMap(rspToApps(_))
            }
          case e => Future.exception(e)
        }
      } catch {
        case e: Throwable => Future.exception(e)
      }
    }

  def getAddrs(app: Path): Future[Set[Address]] =
    prepRequest(s"$apiPrefix/apps${app.show}").flatMap { req =>
      try {
        Trace.letClear(client(req)).flatMap(rspToAddrs(_)).rescue {
          case e: UnauthorizedResponse =>
            // token may have expired, try again
            refreshToken().flatMap { token =>
              req.headerMap.set("Authorization", s"token=$token")
              Trace.letClear(client(req)).flatMap(rspToAddrs(_))
            }
          case e => Future.exception(e)
        }
      } catch {
        case e: Throwable => Future.exception(e)
      }
    }
}
