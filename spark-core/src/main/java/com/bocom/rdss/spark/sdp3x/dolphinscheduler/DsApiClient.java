package com.bocom.rdss.spark.sdp3x.dolphinscheduler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static com.bocom.rdss.spark.sdp3x.dolphinscheduler.DsConfig.*;

/**
 * DolphinScheduler 3.4 原生 REST 客户端，负责 Token 请求头、表单编码及响应校验。
 * 创建、发布、定时切换等写请求不自动重试，避免网络异常时重复创建资源。
 */
class DsApiClient {
  private final String base;
  private final String token;

  /**
   * @param base 当前环境配置的 DS API 根地址，包含服务上下文路径
   * @param token 从同一环境 Properties 的 ds.api.token 读取的 API Token，仅用于认证请求头
   */
  DsApiClient(String base, String token) {
    validateUrl(base);
    require(token != null && !token.isEmpty() && !token.contains("\n") && !token.contains("\r"), "Missing or invalid DS API token");
    this.base = base.replaceAll("/+$", "");
    this.token = token;
  }

  static void validateUrl(String url) {
    URI uri = URI.create(url);
    require(("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) && uri.getHost() != null
      && uri.getUserInfo() == null && uri.getQuery() == null && uri.getFragment() == null,
      "api-url must be an HTTP(S) base URL without credentials, query or fragment");
  }

  Object get(String path, Map<String, Object> query) throws IOException {
    return request("GET", path, query);
  }

  Object post(String path, Map<String, Object> form) throws IOException {
    return request("POST", path, form);
  }

  private Object request(String method, String path, Map<String, Object> parameters) throws IOException {
    String encoded = form(parameters);
    HttpURLConnection connection = (HttpURLConnection) URI.create(base + path
      + (method.equals("GET") && !encoded.isEmpty() ? "?" + encoded : "")).toURL().openConnection();
    connection.setConnectTimeout(15000);
    connection.setReadTimeout(60000);
    connection.setInstanceFollowRedirects(false);
    connection.setRequestMethod(method);
    connection.setRequestProperty("token", token);
    connection.setRequestProperty("Accept", "application/json");
    try {
      if (method.equals("POST")) {
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
        byte[] bytes = encoded.getBytes(StandardCharsets.UTF_8);
        connection.setFixedLengthStreamingMode(bytes.length);
        try (java.io.OutputStream output = connection.getOutputStream()) {
          output.write(bytes);
        }
      }
      int status = connection.getResponseCode();
      if (status < 200 || status >= 300) {
        throw new IOException("DS " + method + " " + path + " returned HTTP " + status);
      }
      String body;
      try (InputStream input = connection.getInputStream(); ByteArrayOutputStream output = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[8192];
        int length;
        while ((length = input.read(buffer)) != -1) {
          if (output.size() + length > 8 * 1024 * 1024) {
            throw new IOException("DS response exceeds 8 MiB");
          }
          output.write(buffer, 0, length);
        }
        body = new String(output.toByteArray(), StandardCharsets.UTF_8);
      }
      Map<String, Object> result;
      try {
        result = object(yaml().load(body), "DS response");
      } catch (RuntimeException e) {
        throw new IOException("DS returned an invalid JSON response for " + path);
      }
      if (number(result, "code", -1) != 0) {
        String message = String.valueOf(result.getOrDefault("msg", "unknown error")).replace(token, "<redacted>");
        throw new IOException("DS rejected " + method + " " + path + ": " + message);
      }
      return result.get("data");
    } finally {
      connection.disconnect();
    }
  }

  private static String form(Map<String, Object> parameters) throws IOException {
    StringBuilder result = new StringBuilder();
    for (Map.Entry<String, Object> entry : parameters.entrySet()) {
      if (result.length() > 0) {
        result.append('&');
      }
      result.append(URLEncoder.encode(entry.getKey(), "UTF-8")).append('=')
        .append(URLEncoder.encode(String.valueOf(entry.getValue()), "UTF-8"));
    }
    return result.toString();
  }
}
