package com.bocom.rdss.spark.sdp3x.dolphinscheduler;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.bocom.rdss.spark.sdp3x.dolphinscheduler.DsConfig.*;
import static org.junit.jupiter.api.Assertions.*;

class DsApiClientTest {
  @TempDir Path directory;

  @Test void encodesNativeFormsAndQueryAndChecksHttpAndBusinessErrors() throws Exception {
    AtomicReference<String> request = new AtomicReference<>();
    AtomicReference<String> method = new AtomicReference<>();
    AtomicReference<String> token = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/api", exchange -> {
      method.set(exchange.getRequestMethod());
      token.set(exchange.getRequestHeaders().getFirst("token"));
      java.io.ByteArrayOutputStream received = new java.io.ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int read;
      while ((read = exchange.getRequestBody().read(buffer)) != -1) {
        received.write(buffer, 0, read);
      }
      request.set(new String(received.toByteArray(), StandardCharsets.UTF_8));
      String path = exchange.getRequestURI().getPath();
      String response = "{\"code\":0,\"data\":{\"name\":\"ok\"}}";
      int status = 200;
      if (path.endsWith("/query")) {
        request.set(exchange.getRequestURI().getRawQuery());
      } else if (path.endsWith("/business")) {
        response = "{\"code\":123,\"msg\":\"secret-token rejected\"}";
      } else if (path.endsWith("/invalid")) {
        response = "<html>not JSON</html>";
      } else if (path.endsWith("/redirect")) {
        status = 302;
        exchange.getResponseHeaders().add("Location", "http://127.0.0.1/never-follow");
      } else if (path.endsWith("/http")) {
        status = 500;
      }
      byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(status, bytes.length);
      exchange.getResponseBody().write(bytes);
      exchange.close();
    });
    server.start();
    try {
      DsApiClient client = new DsApiClient("http://127.0.0.1:" + server.getAddress().getPort() + "/api/", "secret-token");
      Map<String, Object> result = object(client.post("/create", map("name", "中文 ' a&b", "taskDefinitionJson", "[]")), "result");
      assertEquals("ok", result.get("name"));
      assertEquals("POST", method.get());
      assertEquals("secret-token", token.get());
      assertTrue(request.get().contains("%E4%B8%AD%E6%96%87"));
      assertTrue(request.get().contains("a%26b"));
      client.get("/query", map("pageNo", 2, "workflowDefinitionCode", 123));
      assertEquals("GET", method.get());
      assertTrue(request.get().contains("pageNo=2"));
      assertTrue(assertThrows(IOException.class, () -> client.get("/http", Collections.emptyMap())).getMessage().contains("500"));
      assertTrue(assertThrows(IOException.class, () -> client.get("/redirect", Collections.emptyMap())).getMessage().contains("302"));
      assertTrue(assertThrows(IOException.class, () -> client.get("/invalid", Collections.emptyMap())).getMessage().contains("invalid JSON"));
      String error = assertThrows(IOException.class, () -> client.post("/business", Collections.emptyMap())).getMessage();
      assertFalse(error.contains("secret-token"));
      assertTrue(error.contains("<redacted>"));
    } finally {
      server.stop(0);
    }
  }

  @Test void validatesConfigurationPlaceholdersTypesDuplicateKeysAndJsonEscapes() throws Exception {
    Path properties = directory.resolve("application.properties");
    Files.write(properties, "name=${SOURCE_NAME}\nSOURCE_NAME=example\nflag=true\nnumber=12\n".getBytes(StandardCharsets.UTF_8));
    DsConfig config = new DsConfig(properties);
    Path yamlFile = directory.resolve("flow.yaml");
    Files.write(yamlFile, "name: ${name}\nflag: ${flag}\nnumber: ${number}\n".getBytes(StandardCharsets.UTF_8));
    Map<String, Object> values = config.read(yamlFile);
    assertEquals("example", values.get("name"));
    assertTrue(bool(values, "flag", false));
    assertEquals(12, number(values, "number", 0));
    assertEquals("example", config.property("name"));
    assertNull(config.optionalProperty("not-configured"));
    Files.write(yamlFile, "name: ${PATH}\n".getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(IllegalArgumentException.class, () -> config.read(yamlFile)).getMessage()
      .contains("Unresolved configuration property: PATH"));
    assertThrows(IllegalArgumentException.class, () -> config.property("missing"));
    assertThrows(IllegalArgumentException.class, () -> bool(map("flag", "maybe"), "flag", false));
    assertThrows(IllegalArgumentException.class, () -> number(map("number", "1.2"), "number", 0));
    Files.write(yamlFile, "name: one\nname: two\n".getBytes(StandardCharsets.UTF_8));
    assertThrows(RuntimeException.class, () -> config.read(yamlFile));
    Files.write(properties, "first=${second}\nsecond=${first}\n".getBytes(StandardCharsets.UTF_8));
    DsConfig circular = new DsConfig(properties);
    assertThrows(IllegalArgumentException.class, () -> circular.property("first"));
    Object special = map("value", "a\n\r\t\"\\\u0001中文", "list", java.util.Arrays.asList(true, 42, null));
    assertEquals(special, yaml().load(json(special)));
    assertThrows(IllegalArgumentException.class, () -> DsApiClient.validateUrl("http://user:password@host/api"));
    assertThrows(IllegalArgumentException.class, () -> DsApiClient.validateUrl("file:///tmp/api"));
    assertThrows(IllegalArgumentException.class, () -> new DsApiClient("http://localhost", "bad\ntoken"));
  }
}
