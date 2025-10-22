package com.netflix.maestro.dsl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.utils.JsonHelper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

public class BaseTest {
  protected ObjectMapper mapper = JsonHelper.objectMapperWithYaml();

  protected <T> T loadObject(String fileName, Class<T> clazz) throws IOException {
    return mapper.readValue(loadYaml(fileName), clazz);
  }

  protected String loadYaml(String fileName) throws IOException {
    try (InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)) {
      if (is == null) {
        return null;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
      }
    }
  }
}
