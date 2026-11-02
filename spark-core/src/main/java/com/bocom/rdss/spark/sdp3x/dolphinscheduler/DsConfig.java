package com.bocom.rdss.spark.sdp3x.dolphinscheduler;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.representer.Representer;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DS 配置读取与序列化工具：加载指定环境的 UTF-8 Properties，解析 YAML 中的属性占位符，
 * 提供配置类型校验及 JSON 文件写入。占位符只从该 Properties 文件取值，不回退操作系统环境变量。
 */
final class DsConfig {
  private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^{}]+)}");
  private final Properties properties = new Properties();

  /** @param file 由 env 选定的 properties/application-&lt;env&gt;.properties 文件路径 */
  DsConfig(Path file) throws IOException {
    try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      properties.load(reader);
    }
  }

  String property(String key) {
    String value = properties.getProperty(key);
    require(value != null, "Missing environment property: " + key);
    return expand(value, 0);
  }

  /** 读取可选配置，例如离线 generate 无需填写的 ds.api.token；未配置时返回 null。 */
  String optionalProperty(String key) {
    String value = properties.getProperty(key);
    return value == null ? null : expand(value, 0);
  }

  Map<String, Object> read(Path path) throws IOException {
    Object value = yaml().load(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
    return object(resolve(value), path.toString());
  }

  private Object resolve(Object value) {
    if (value instanceof Map) {
      Map<String, Object> result = new LinkedHashMap<>();
      object(value, "YAML").forEach((key, item) -> result.put(key, resolve(item)));
      return result;
    }
    if (value instanceof List) {
      List<Object> result = new ArrayList<>();
      for (Object item : (List<?>) value) {
        result.add(resolve(item));
      }
      return result;
    }
    return value instanceof String ? expand((String) value, 0) : value;
  }

  private String expand(String value, int depth) {
    require(depth < 20, "Circular or excessively nested property references");
    Matcher matcher = PLACEHOLDER.matcher(value);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      String key = matcher.group(1);
      String replacement = properties.getProperty(key);
      require(replacement != null, "Unresolved configuration property: " + key);
      matcher.appendReplacement(result, Matcher.quoteReplacement(expand(replacement, depth + 1)));
    }
    matcher.appendTail(result);
    return result.toString();
  }

  static Yaml yaml() {
    LoaderOptions options = new LoaderOptions();
    options.setAllowDuplicateKeys(false);
    DumperOptions dumper = new DumperOptions();
    return new Yaml(new SafeConstructor(options), new Representer(dumper), dumper, options);
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> object(Object value, String label) {
    require(value instanceof Map, label + " must be an object/map");
    for (Object key : ((Map<?, ?>) value).keySet()) {
      require(key instanceof String, label + " must have string keys");
    }
    return (Map<String, Object>) value;
  }

  static Map<String, Object> child(Map<String, Object> map, String key) {
    return map.containsKey(key) ? object(map.get(key), key) : new LinkedHashMap<>();
  }

  static List<?> list(Object value, String label) {
    require(value instanceof List, label + " must be a list");
    return (List<?>) value;
  }

  static String text(Map<String, Object> map, String key, String fallback) {
    Object value = map.get(key);
    if (value == null) {
      require(fallback != null, "Required configuration is missing: " + key);
      return fallback;
    }
    require(value instanceof String || value instanceof Number, key + " must be a string/number");
    String result = value.toString().trim();
    require(!result.isEmpty(), key + " must not be empty");
    return result;
  }

  static long number(Map<String, Object> map, String key, long fallback) {
    try {
      return Long.parseLong(text(map, key, Long.toString(fallback)));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be an integer");
    }
  }

  static boolean bool(Map<String, Object> map, String key, boolean fallback) {
    Object value = map.get(key);
    if (value == null) {
      return fallback;
    }
    require("true".equalsIgnoreCase(value.toString()) || "false".equalsIgnoreCase(value.toString()),
      key + " must be true or false");
    return Boolean.parseBoolean(value.toString());
  }

  static Map<String, Object> map(Object... pairs) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (int index = 0; index < pairs.length; index += 2) {
      result.put((String) pairs[index], pairs[index + 1]);
    }
    return result;
  }

  static String json(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Map) {
      List<String> parts = new ArrayList<>();
      object(value, "JSON").forEach((key, item) -> parts.add(json(key) + ":" + json(item)));
      return "{" + String.join(",", parts) + "}";
    }
    if (value instanceof Iterable) {
      List<String> parts = new ArrayList<>();
      for (Object item : (Iterable<?>) value) {
        parts.add(json(item));
      }
      return "[" + String.join(",", parts) + "]";
    }
    if (value instanceof Number || value instanceof Boolean) {
      return value.toString();
    }
    StringBuilder result = new StringBuilder("\"");
    for (char ch : value.toString().toCharArray()) {
      switch (ch) {
        case '"': result.append("\\\""); break;
        case '\\': result.append("\\\\"); break;
        case '\n': result.append("\\n"); break;
        case '\r': result.append("\\r"); break;
        case '\t': result.append("\\t"); break;
        default:
          if (ch < 32) {
            result.append(String.format("\\u%04x", (int) ch));
          } else {
            result.append(ch);
          }
      }
    }
    return result.append('"').toString();
  }

  static void write(Path path, Object value) throws IOException {
    Path temporary = Files.createTempFile(path.toAbsolutePath().getParent(), ".ds-", ".json");
    try {
      Files.write(temporary, (json(value) + "\n").getBytes(StandardCharsets.UTF_8));
      Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
