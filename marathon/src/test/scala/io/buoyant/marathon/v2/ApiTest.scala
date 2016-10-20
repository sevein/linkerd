package io.buoyant.marathon.v2

import com.fasterxml.jackson.core.JsonParseException
import com.twitter.conversions.time._
import com.twitter.finagle.http.{Response, Request}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Address, Path, Service, http}
import com.twitter.io.Buf
import com.twitter.util.Future
import io.buoyant.test.{Exceptions, Awaits}
import java.net.InetSocketAddress
import org.scalatest.FunSuite

class ApiTest extends FunSuite with Awaits with Exceptions {

  val appsBuf = Buf.Utf8("""
    {
      "apps": [
        {
          "id": "/foo",
          "cmd": null,
          "labels": { "LABEL_FOO": "BAR" }
        },
        {
          "id": "/bar",
          "cmd": "bar cmd"
        },
        {
          "id": "/baz",
          "cmd": null
        }
      ]
    }
  """)

  val appBuf = Buf.Utf8("""
    {
      "app": {
        "id": "/foo",
        "cmd": "foo cmd",
        "tasks": [
          {
            "id": "booksId",
            "host": "1.2.3.4",
            "ports": [7000, 7001, 7002]
          },
          {
            "id": "booksId2",
            "host": "5.6.7.8",
            "ports": [7003, 7004, 7005]
          }
        ]
      }
    }
  """)

  val noApps = Buf.Utf8("""
    {"apps":[]}
  """)

  val appNotFoundBuf = Buf.Utf8("""{"message":"App '/foo' does not exist"}""")

  val privateKeyRSA = """-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEAvzoCEC2rpSpJQaWZbUmlsDNwp83Jr4fi6KmBWIwnj1MZ6CUQ
7rBasuLI8AcfX5/10scSfQNCsTLV2tMKQaHuvyrVfwY0dINk+nkqB74QcT2oCCH9
XduJjDuwWA4xLqAKuF96FsIes52opEM50W7/W7DZCKXkC8fFPFj6QF5ZzApDw2Qs
u3yMRmr7/W9uWeaTwfPx24YdY7Ah+fdLy3KN40vXv9c4xiSafVvnx9BwYL7H1Q8N
iK9LGEN6+JSWfgckQCs6UUBOXSZdreNN9zbQCwyzee7bOJqXUDAuLcFARzPw1EsZ
AyjVtGCKIQ0/btqK+jFunT2NBC8RItanDZpptQIDAQABAoIBAQCsssO4Pra8hFMC
gX7tr0x+tAYy1ewmpW8stiDFilYT33YPLKJ9HjHbSms0MwqHftwwTm8JDc/GXmW6
qUui+I64gQOtIzpuW1fvyUtHEMSisI83QRMkF6fCSQm6jJ6oQAtOdZO6R/gYOPNb
3gayeS8PbMilQcSRSwp6tNTVGyC33p43uUUKAKHnpvAwUSc61aVOtw2wkD062XzM
hJjYpHm65i4V31AzXo8HF42NrAtZ8K/AuQZne5F/6F4QFVlMKzUoHkSUnTp60XZx
X77GuyDeDmCgSc2J7xvR5o6VpjsHMo3ek0gJk5ZBnTgkHvnpbULCRxTmDfjeVPue
v3NN2TBFAoGBAPxbqNEsXPOckGTvG3tUOAAkrK1hfW3TwvrW/7YXg1/6aNV4sklc
vqn/40kCK0v9xJIv9FM/l0Nq+CMWcrb4sjLeGwHAa8ASfk6hKHbeiTFamA6FBkvQ
//7GP5khD+y62RlWi9PmwJY21lEkn2mP99THxqvZjQiAVNiqlYdwiIc7AoGBAMH8
f2Ay7Egc2KYRYU2qwa5E/Cljn/9sdvUnWM+gOzUXpc5sBi+/SUUQT8y/rY4AUVW6
YaK7chG9YokZQq7ZwTCsYxTfxHK2pnG/tXjOxLFQKBwppQfJcFSRLbw0lMbQoZBk
S+zb0ufZzxc2fJfXE+XeJxmKs0TS9ltQuJiSqCPPAoGBALEc84K7DBG+FGmCl1sb
ZKJVGwwknA90zCeYtadrIT0/VkxchWSPvxE5Ep+u8gxHcqrXFTdILjWW4chefOyF
5ytkTrgQAI+xawxsdyXWUZtd5dJq8lxLtx9srD4gwjh3et8ZqtFx5kCHBCu29Fr2
PA4OmBUMfrs0tlfKgV+pT2j5AoGBAKnA0Z5XMZlxVM0OTH3wvYhI6fk2Kx8TxY2G
nxsh9m3hgcD/mvJRjEaZnZto6PFoqcRBU4taSNnpRr7+kfH8sCht0k7D+l8AIutL
ffx3xHv9zvvGHZqQ1nHKkaEuyjqo+5kli6N8QjWNzsFbdvBQ0CLJoqGhVHsXuWnz
W3Z4cBbVAoGAEtnwY1OJM7+R2u1CW0tTjqDlYU2hUNa9t1AbhyGdI2arYp+p+umA
b5VoYLNsdvZhqjVFTrYNEuhTJFYCF7jAiZLYvYm0C99BqcJnJPl7JjWynoNHNKw3
9f6PIOE1rAmPE8Cfz/GFF5115ZKVlq+2BY8EKNxbCIy2d/vMEvisnXI=
-----END RSA PRIVATE KEY-----"""

  val loginEndpoint = "/fake_login_endpoint"
  val authbuf = Buf.Utf8("""{"token":"foo"}""")
  val authUnexpectedBuf = Buf.Utf8("""{"unexpected":"foo"}""")
  val auth = Some(Api.AuthRequest(s"http://example.com$loginEndpoint", "fakeuid", privateKeyRSA))
  val concurrencyLoad = 100

  def stubService(buf: Buf) = Service.mk[Request, Response] { req =>
    val rsp = Response()
    rsp.content = buf
    Future.value(rsp)
  }

  case class stubAuth(status: http.Status, authbuf: Buf, buf: Buf, failRequest: Boolean = false) {
    var auths = 0
    var requests = 0

    def service() =
      Service.mk[Request, Response] { req =>
        val rsp = Response(status)

        rsp.content =
          if (req.path == loginEndpoint) {
            auths += 1
            assert(req.contentString == auth.get.jwtToken)
            authbuf
          } else {
            if (failRequest && requests == 0) {
              rsp.status = http.Status.Unauthorized
            }

            requests += 1
            buf
          }

        Future.sleep(10.milliseconds)(DefaultTimer.twitter).before(
          Future.value(rsp)
        )
      }
  }

  test("getAppIds endpoint returns a seq of app names") {
    val service = stubService(appsBuf)

    val response = await(Api(service, "host", "prefix", None).getAppIds())
    assert(response == Set(
      Path.read("/foo"),
      Path.read("/bar"),
      Path.read("/baz")
    ))
  }

  test("getAppIds endpoint returns an empty seq when there are no apps") {
    val service = stubService(noApps)

    val response = await(Api(service, "host", "prefix", None).getAppIds())
    assert(response.size == 0)
  }

  test("getAppIds endpoint uses auth if provided") {
    val service = stubAuth(http.Status.Ok, authbuf, appsBuf).service()

    val response = await(Api(service, "host", "prefix", auth).getAppIds())
    assert(response == Set(
      Path.read("/foo"),
      Path.read("/bar"),
      Path.read("/baz")
    ))
  }

  test("getAppIds endpoint returns Unauthorized if auth fails") {
    val service = stubAuth(http.Status.Unauthorized, authbuf, appsBuf).service()

    assertThrows[Api.UnauthorizedResponse] {
      await(Api(service, "host", "prefix", auth).getAppIds())
    }
  }

  test("getAppIds endpoint returns Unexpected if auth returns unexpected json") {
    val service = stubAuth(http.Status.Ok, authUnexpectedBuf, appsBuf).service()

    assertThrows[Api.UnexpectedResponse] {
      await(Api(service, "host", "prefix", auth).getAppIds())
    }
  }

  test("getAppIds endpoint returns Unexpected if auth returns bad json") {
    val service = stubAuth(http.Status.Ok, Buf.Utf8("bad json"), appsBuf).service()

    assertThrows[JsonParseException] {
      await(Api(service, "host", "prefix", auth).getAppIds())
    }
  }

  test("getAppIds only auth's once when called multiple times") {
    val stub = stubAuth(http.Status.Ok, authbuf, appsBuf)
    val api = Api(stub.service(), "host", "prefix", auth)

    val responses = await(
      Future.collect(
        Seq.range(0, concurrencyLoad).map { _ =>
          api.getAppIds()
        }
      )
    )

    assert(stub.auths == 1)
    assert(stub.requests == concurrencyLoad)

    assert(responses.forall(_ == Set(
      Path.read("/foo"),
      Path.read("/bar"),
      Path.read("/baz")
    )))
  }

  test("getAppIds endpoint retries auth once after unauthorized") {
    val stub = stubAuth(http.Status.Ok, authbuf, appsBuf, true)
    val response = await(Api(stub.service(), "host", "prefix", auth).getAppIds())

    assert(stub.auths == 2)
    assert(stub.requests == 2)

    assert(response == Set(
      Path.read("/foo"),
      Path.read("/bar"),
      Path.read("/baz")
    ))
  }

  test("getAddrs endpoint returns a seq of addresses") {
    val service = stubService(appBuf)

    val response = await(Api(service, "host", "prefix", None).getAddrs(Path.Utf8("foo")))
    assert(response == Set(
      Address("1.2.3.4", 7000),
      Address("5.6.7.8", 7003)
    ))
  }

  test("getAddrs endpoint returns an empty set of addresses if app not found") {
    val service = stubService(appNotFoundBuf)

    val response = await(Api(service, "host", "prefix", None).getAddrs(Path.Utf8("foo")))
    assert(response.size == 0)
  }

  test("getAddrs endpoint uses auth if provided") {
    val service = stubAuth(http.Status.Ok, authbuf, appBuf).service()

    val response = await(Api(service, "host", "prefix", auth).getAddrs(Path.Utf8("foo")))
    assert(response == Set(
      Address("1.2.3.4", 7000),
      Address("5.6.7.8", 7003)
    ))
  }

  test("getAddrs endpoint returns Unauthorized if auth fails") {
    val service = stubAuth(http.Status.Unauthorized, authbuf, appBuf).service()

    assertThrows[Api.UnauthorizedResponse] {
      await(Api(service, "host", "prefix", auth).getAddrs(Path.Utf8("foo")))
    }
  }

  test("getAddrs endpoint returns Unexpected if auth returns unexpected json") {
    val service = stubAuth(http.Status.Ok, authUnexpectedBuf, appBuf).service()

    assertThrows[Api.UnexpectedResponse] {
      await(Api(service, "host", "prefix", auth).getAddrs(Path.Utf8("foo")))
    }
  }

  test("getAddrs endpoint returns Unexpected if auth returns bad json") {
    val service = stubAuth(http.Status.Ok, Buf.Utf8("bad json"), appBuf).service()

    assertThrows[JsonParseException] {
      await(Api(service, "host", "prefix", auth).getAddrs(Path.Utf8("foo")))
    }
  }

  test("getAddrs only auth's once when called multiple times") {
    val stub = stubAuth(http.Status.Ok, authbuf, appBuf)
    val api = Api(stub.service(), "host", "prefix", auth)

    val responses = await(
      Future.collect(
        Seq.range(0, concurrencyLoad).map { _ =>
          api.getAddrs(Path.Utf8("foo"))
        }
      )
    )

    assert(stub.auths == 1)
    assert(stub.requests == concurrencyLoad)

    assert(responses.forall(_ == Set(
      Address("1.2.3.4", 7000),
      Address("5.6.7.8", 7003)
    )))
  }

  class ClientFailure extends Exception("I have no idea who to talk to")

  test("propagates client failures") {
    val failureService = Service.mk[Request, Response] { req =>
      Future.exception(new ClientFailure)
    }
    assertThrows[ClientFailure] {
      await(Api(failureService, "host", "prefix", None).getAppIds())
    }
  }
}
