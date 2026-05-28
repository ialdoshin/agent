# Метрики сервиса для Grafana

Все метрики — в формате **Prometheus**. Именование по конвенции `<namespace>_<subsystem>_<name>_<unit>`.

Пример неймспейса: `myservice` (заменить на имя сервиса).

---

## 1. Входящие API запросы (Inbound)

Метрики для каждого из 3–4 API, через которые запросы попадают в сервис.

### `myservice_http_requests_total`
**Тип:** Counter

**Описание:** Общее количество входящих HTTP-запросов.

**Labels:**
| Label | Описание | Пример |
|-------|----------|--------|
| `method` | HTTP-метод | `GET`, `POST`, `PUT` |
| `endpoint` | Путь эндпоинта (шаблон, не конкретный ID) | `/api/v1/orders`, `/api/v1/users` |
| `status_code` | HTTP-код ответа | `200`, `400`, `500` |
| `api_name` | Логическое имя API | `orders_api`, `users_api` |

**Grafana:** Rate (`rate(myservice_http_requests_total[1m])`), разбивка по `status_code` и `endpoint`.

---

### `myservice_http_request_duration_seconds`
**Тип:** Histogram

**Описание:** Время обработки входящего запроса от получения до отправки ответа.

**Labels:** те же, что у `http_requests_total` (`method`, `endpoint`, `status_code`, `api_name`)

**Buckets (рекомендуемые):** `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10` (секунды)

**Grafana:**
- p50 / p95 / p99 через `histogram_quantile(0.99, rate(myservice_http_request_duration_seconds_bucket[5m]))`
- Heatmap по `le`-бакетам

---

### `myservice_http_requests_in_flight`
**Тип:** Gauge

**Описание:** Количество одновременно обрабатываемых запросов прямо сейчас.

**Labels:** `endpoint`, `api_name`

**Grafana:** Текущее значение — показывает перегрузку или утечку горутин.

---

### `myservice_http_errors_total`
**Тип:** Counter

**Описание:** Количество запросов, завершившихся ошибкой (4xx/5xx). Можно вывести из `http_requests_total`, но отдельный счётчик удобнее для алертов.

**Labels:** `endpoint`, `api_name`, `error_type` (`client_error` / `server_error`)

---

## 2. Исходящие вызовы внешних API (Outbound)

Метрики для 2 внешних API, которые вызывает сервис.

### `myservice_external_requests_total`
**Тип:** Counter

**Описание:** Количество исходящих вызовов к внешним зависимостям.

**Labels:**
| Label | Описание | Пример |
|-------|----------|--------|
| `target` | Имя внешнего сервиса | `payment_service`, `notification_service` |
| `method` | HTTP-метод или gRPC-метод | `POST`, `GetUser` |
| `endpoint` | Путь или метод | `/v1/charge`, `/health` |
| `status_code` | Код ответа | `200`, `503` |

**Grafana:** Error rate по `target`: `rate(...{status_code=~"5.."}[5m]) / rate(...[5m])`

---

### `myservice_external_request_duration_seconds`
**Тип:** Histogram

**Описание:** Время ответа внешнего API (round-trip, включая сеть).

**Labels:** `target`, `endpoint`, `status_code`

**Buckets:** `0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5` (секунды)

**Grafana:** p95/p99 по `target` — видно, какой внешний сервис тормозит.

---

### `myservice_external_request_retries_total`
**Тип:** Counter

**Описание:** Количество повторных попыток при вызове внешнего API.

**Labels:** `target`, `endpoint`

**Grafana:** Ненулевой рост — сигнал нестабильности внешней зависимости.

---

### `myservice_external_circuit_breaker_state`
**Тип:** Gauge

**Описание:** Состояние circuit breaker для каждого внешнего API (если используется). `0` = closed (норма), `1` = open (блокировка), `2` = half-open.

**Labels:** `target`

**Grafana:** Alert при значении `1` — внешний сервис недоступен.

---

## 3. Kafka

### Consumer

#### `myservice_kafka_consumer_messages_total`
**Тип:** Counter

**Описание:** Количество прочитанных сообщений из Kafka.

**Labels:**
| Label | Описание | Пример |
|-------|----------|--------|
| `topic` | Топик Kafka | `orders.created`, `payments.events` |
| `partition` | Номер партиции | `0`, `1`, `2` |
| `consumer_group` | Группа консюмера | `myservice-consumer-group` |
| `status` | Результат обработки | `success`, `error`, `skipped` |

---

#### `myservice_kafka_consumer_processing_duration_seconds`
**Тип:** Histogram

**Описание:** Время обработки одного сообщения из Kafka (от получения до коммита).

**Labels:** `topic`, `consumer_group`

**Buckets:** `0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5`

**Grafana:** p99 задержки обработки — критично для SLA.

---

#### `myservice_kafka_consumer_lag`
**Тип:** Gauge

**Описание:** Отставание консюмера — разница между последним offset в партиции и текущим прочитанным offset.

**Labels:** `topic`, `partition`, `consumer_group`

**Grafana:** Ключевая метрика. Алерт при росте лага выше порога (например, > 1000 сообщений).

> ⚠️ Эту метрику удобнее собирать через [kafka-exporter](https://github.com/danielqsj/kafka_exporter) на стороне инфраструктуры, а не из приложения. Уточнить с DevOps.

---

#### `myservice_kafka_consumer_errors_total`
**Тип:** Counter

**Описание:** Ошибки при чтении или обработке сообщений.

**Labels:** `topic`, `consumer_group`, `error_type` (`deserialization_error`, `processing_error`, `commit_error`)

---

### Producer

#### `myservice_kafka_producer_messages_total`
**Тип:** Counter

**Описание:** Количество отправленных сообщений в Kafka.

**Labels:**
| Label | Описание | Пример |
|-------|----------|--------|
| `topic` | Целевой топик | `orders.processed` |
| `status` | Результат | `success`, `error` |

---

#### `myservice_kafka_producer_send_duration_seconds`
**Тип:** Histogram

**Описание:** Время от вызова `produce` до подтверждения от брокера (ack).

**Labels:** `topic`

**Buckets:** `0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1`

---

#### `myservice_kafka_producer_errors_total`
**Тип:** Counter

**Описание:** Ошибки продюсера при отправке.

**Labels:** `topic`, `error_type` (`serialization_error`, `broker_unavailable`, `timeout`)

---

#### `myservice_kafka_producer_message_size_bytes`
**Тип:** Histogram

**Описание:** Размер сообщений, отправляемых в Kafka. Помогает следить за раздуванием payload.

**Labels:** `topic`

**Buckets:** `256, 1024, 4096, 16384, 65536, 262144, 1048576` (байты)

---

## 4. Системные и Runtime метрики

Эти метрики либо предоставляются автоматически (Go runtime, node_exporter), либо минимально инструментируются.

### Go Runtime (автоматически через `prometheus/client_golang`)

| Метрика | Тип | Описание |
|---------|-----|----------|
| `go_goroutines` | Gauge | Количество живых горутин — утечка видна сразу |
| `go_gc_duration_seconds` | Summary | Паузы GC — влияет на latency |
| `go_memstats_alloc_bytes` | Gauge | Текущее выделение heap памяти |
| `go_memstats_sys_bytes` | Gauge | Память, запрошенная у ОС |
| `process_cpu_seconds_total` | Counter | CPU-время процесса |
| `process_open_fds` | Gauge | Открытые файловые дескрипторы — утечки соединений |

---

### Пул соединений / DB (если есть)

#### `myservice_db_pool_connections`
**Тип:** Gauge

**Labels:** `state` (`idle`, `in_use`, `wait`)

**Описание:** Состояние пула соединений к БД. `wait` > 0 — сервис упирается в БД.

---

### Бизнес-метрики (опционально, по желанию)

Добавляются под конкретную логику, например:

| Метрика | Тип | Описание |
|---------|-----|----------|
| `myservice_orders_processed_total` | Counter | Бизнес-события (заменить под свою доменную логику) |
| `myservice_cache_hits_total` | Counter | Хиты кэша (labels: `result`: `hit`/`miss`) |
| `myservice_queue_depth` | Gauge | Внутренняя очередь обработки (если есть буферизация) |

---

## 5. Рекомендуемые панели в Grafana

| Панель | Метрики |
|--------|---------|
| **Inbound Overview** | RPS по API, error rate, p95 latency |
| **Inbound Latency** | Heatmap `http_request_duration_seconds`, p50/p95/p99 |
| **External APIs** | Error rate + p99 latency по каждому `target` |
| **Kafka Consumer** | Lag по топикам, RPS обработки, error rate |
| **Kafka Producer** | Throughput, ошибки, размер сообщений |
| **Runtime** | Goroutines, GC pauses, heap, CPU, open FDs |

---

## 6. Алерты (рекомендуемые пороги)

| Алерт | Условие | Severity |
|-------|---------|----------|
| High error rate inbound | `error_rate > 1%` за 5 мин | warning |
| Critical error rate | `error_rate > 5%` за 1 мин | critical |
| High p99 latency | `p99 > 2s` за 5 мин | warning |
| External API degraded | `external p99 > 3s` | warning |
| Kafka consumer lag growing | `lag > 1000` и `rate(lag) > 0` | warning |
| Kafka consumer lag critical | `lag > 10000` | critical |
| Goroutine leak | `go_goroutines > 10000` | warning |
| Circuit breaker open | `circuit_breaker_state == 1` | critical |

---

## 7. Инструментирование в Go

Пример регистрации метрики:

```go
var httpRequestsTotal = prometheus.NewCounterVec(
    prometheus.CounterOpts{
        Namespace: "myservice",
        Subsystem: "http",
        Name:      "requests_total",
        Help:      "Total number of inbound HTTP requests.",
    },
    []string{"method", "endpoint", "status_code", "api_name"},
)

var httpRequestDuration = prometheus.NewHistogramVec(
    prometheus.HistogramOpts{
        Namespace: "myservice",
        Subsystem: "http",
        Name:      "request_duration_seconds",
        Help:      "Duration of inbound HTTP requests.",
        Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
    },
    []string{"method", "endpoint", "status_code", "api_name"},
)
```

Middleware для автоматического сбора:

```go
func MetricsMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        rw := &responseWriter{w, 200}
        next.ServeHTTP(rw, r)
        
        duration := time.Since(start).Seconds()
        status := strconv.Itoa(rw.statusCode)
        
        httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, status, "api_name").Inc()
        httpRequestDuration.WithLabelValues(r.Method, r.URL.Path, status, "api_name").Observe(duration)
    })
}
```
