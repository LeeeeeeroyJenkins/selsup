import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CrptApi {
    private static final String BASE_URL = "https://ismp.crpt.ru/api/v3";
    private static final String CREATE_ENDPOINT = "/doc/create";
    private static final String DOC_TYPE = "LP_INTRODUCE_GOODS";
    private static final String DOCUMENT_FORMAT = "MANUAL";

    private final HttpService httpService;
    private final ObjectMapper objectMapper;
    private final RateLimiter rateLimiter;
    private final AtomicReference<String> tokenRef;

    /**
     * Конструктор клиента API CRPT с настройкой лимита запросов.
     *
     * @param timeUnit единица времени для окна лимита (например, SECONDS, MINUTES)
     * @param requestLimit максимальное число запросов в окне времени
     * @throws IllegalArgumentException если requestLimit <= 0
     */
    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (requestLimit <= 0) {
            throw new IllegalArgumentException("requestLimit должен быть положительным: " + requestLimit);
        }
        this.httpService = new HttpService();
        this.objectMapper = new ObjectMapper();
        this.rateLimiter = new RateLimiter(timeUnit, requestLimit);
        this.tokenRef = new AtomicReference<>();
    }

    /**
     * Устанавливает токен аутентификации. Обязательно вызвать перед операциями API.
     * Время жизни токена — 10 часов согласно спецификации API.
     *
     * @param token Bearer-токен, полученный через endpoint аутентификации
     * @throws IllegalArgumentException если токен null или пустой
     */
    public void setToken(String token) {
        if (token == null || token.trim().isEmpty()) {
            throw new IllegalArgumentException("Токен не может быть null или пустым");
        }
        this.tokenRef.set(token.trim());
    }

    /**
     * Создает документ для ввода товаров в оборот, произведенных в РФ.
     * Документ должен содержать обязательные поля вроде 'pg' или 'product_group' (например, 1 для одежды),
     * массив 'items' с GTIN, количествами и т.д., согласно схеме API.
     * Автоматически устанавливает 'doc_type' в LP_INTRODUCE_GOODS.
     *
     * @param document структура документа как изменяемая Map (будет модифицирована для добавления doc_type)
     * @param signature base64-кодированная УКЭП-подпись JSON документа
     * @return UUID созданного документа
     * @throws IllegalStateException если токен не установлен
     * @throws CrptApiException при ошибках HTTP/JSON
     * @throws InterruptedException если поток прерван во время ожидания лимита
     */
    public String createIntroductionIntoCirculation(Map<String, Object> document, String signature)
            throws CrptApiException, InterruptedException {
        String token = tokenRef.get();
        if (token == null || token.isEmpty()) {
            throw new IllegalStateException("Токен аутентификации должен быть установлен через setToken() перед операциями");
        }

        rateLimiter.acquire();

        // Убеждаемся, что doc_type установлен (можно было бы сделать immutable копию для чистоты, но Map mutable по спецификации)
        document.put("doc_type", DOC_TYPE);

        try {
            String docJson = objectMapper.writeValueAsString(document);
            String base64Doc = Base64.getEncoder().encodeToString(docJson.getBytes(StandardCharsets.UTF_8));

            ObjectNode requestBody = objectMapper.createObjectNode();
            requestBody.put("document_format", DOCUMENT_FORMAT);
            requestBody.put("product_document", base64Doc);
            requestBody.put("type", DOC_TYPE);
            requestBody.put("signature", signature);

            String jsonBody = objectMapper.writeValueAsString(requestBody);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + CREATE_ENDPOINT))
                    .header("Content-Type", "application/json;charset=UTF-8")
                    .header("Accept", "*/*")
                    .header("Authorization", "Bearer " + token)
                    .timeout(Duration.ofMinutes(2))  // Разумный таймаут
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpService.send(request);

            int status = response.statusCode();
            if (status != 200 && status != 201) {
                throw new CrptApiException("Не удалось создать документ: HTTP " + status + " - " + response.body());
            }

            // Парсим ответ: ожидаем {"id": "uuid"} или просто строку UUID
            String body = response.body().trim();
            if (body.startsWith("{") && body.endsWith("}")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> respMap = objectMapper.readValue(body, Map.class);
                return (String) respMap.get("id");
            }
            return body;  // Fallback на простую строку

        } catch (Exception e) {
            throw new CrptApiException("Ошибка при создании документа: " + e.getMessage(), e);
        }
    }

    /**
     * Собственное исключение для ошибок API CRPT.
     */
    public static class CrptApiException extends Exception {
        public CrptApiException(String message) {
            super(message);
        }

        public CrptApiException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Внутренний сервис для HTTP-операций. Легко расширяем для кастомных клиентов или async.
     */
    private static class HttpService {
        private final HttpClient client;

        private HttpService() {
            this.client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
        }

        public HttpResponse<String> send(HttpRequest request) throws InterruptedException, CrptApiException {
            try {
                return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            } catch (java.io.IOException e) {
                throw new CrptApiException("IO-ошибка при HTTP-запросе: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Потокобезопасный rate limiter в стиле token bucket с приближенным sliding window.
     * Использует ConcurrentLinkedQueue для временных меток; busy-wait с микро-паузами, чтобы избежать spinlock.
     * Подходит для умеренной concurrency; для высокой нагрузки лучше внешние либы вроде Resilience4j.
     */
    private static class RateLimiter {
        private final Queue<Long> requestTimestamps = new ConcurrentLinkedQueue<>();
        private final long windowNanos;
        private final int maxRequests;

        public RateLimiter(TimeUnit timeUnit, int maxRequests) {
            this.windowNanos = timeUnit.toNanos(1);
            this.maxRequests = maxRequests;
        }

        /**
         * Захватывает разрешение, блокируясь при превышении лимита.
         *
         * @throws InterruptedException если прерван во время ожидания
         */
        public synchronized void acquire() throws InterruptedException {
            long nowNanos = System.nanoTime();
            pruneOldRequests(nowNanos);

            if (requestTimestamps.size() < maxRequests) {
                requestTimestamps.add(nowNanos);
                return;
            }

            // Лимит превышен: экспоненциальный backoff с чисткой
            long sleepTime = 10;  // Начинаем с 10 мс
            while (true) {
                Thread.sleep(sleepTime);
                if (Thread.interrupted()) {
                    throw new InterruptedException("Прерван во время захвата лимита");
                }
                nowNanos = System.nanoTime();
                pruneOldRequests(nowNanos);
                if (requestTimestamps.size() < maxRequests) {
                    requestTimestamps.add(nowNanos);
                    return;
                }
                sleepTime = Math.min(sleepTime * 2, windowNanos / maxRequests);  // Ограничиваем паузу
            }
        }

        private void pruneOldRequests(long nowNanos) {
            while (!requestTimestamps.isEmpty() && nowNanos - requestTimestamps.peek() > windowNanos) {
                requestTimestamps.poll();
            }
        }
    }
}