// EdgeFleet ESP32-CAM firmware (ESP-IDF v5.x, AI-Thinker ESP32-CAM board).
//
// Tasks:
//   heartbeat_task  POSTs telemetry to the control plane every EF_HEARTBEAT_MS
//                   and executes the commands returned in the response.
//   camera_task     Captures QQVGA grayscale frames, runs frame-differencing
//                   motion detection and queues detection events.
//   events_task     Batches detection events and POSTs them to the Kafka REST
//                   proxy (the ESP32 has no native Kafka client).
//
// Tasks communicate only through FreeRTOS queues (message passing), never via
// shared globals guarded by locks.
//
// Firmware safety: images are flashed to the inactive OTA slot. A new image
// boots in ESP_OTA_IMG_PENDING_VERIFY and is only marked valid after its first
// successful heartbeat; if it crashes or cannot reach the control plane before
// the watchdog fires, the bootloader reverts to the previous slot.

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cJSON.h"
#include "esp_app_desc.h"
#include "esp_camera.h"
#include "esp_crt_bundle.h"
#include "esp_event.h"
#include "esp_heap_caps.h"
#include "esp_http_client.h"
#include "esp_https_ota.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_ota_ops.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "esp_wifi.h"
#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"
#include "freertos/queue.h"
#include "freertos/task.h"
#include "nvs.h"
#include "nvs_flash.h"

#include "motion.h"

static const char *TAG = "edgefleet";

#define WIFI_CONNECTED_BIT BIT0
#define RESP_BUF_SIZE 4096
#define EVENT_BATCH 16
#define SEEN_RING 16

static EventGroupHandle_t s_wifi_events;
static QueueHandle_t s_event_q;   // detection_t, camera_task -> events_task
static QueueHandle_t s_config_q;  // runtime_config_t, heartbeat_task -> camera_task

static char s_device_id[24];
static char s_fw_state[12] = "ok";
static int64_t s_config_version;
static int s_error_count;

typedef struct {
    int fps;
    int pixel_threshold;
    float min_ratio;
} runtime_config_t;

typedef struct {
    uint32_t seq;
    int64_t ts_ms;
    motion_result_t m;
} detection_t;

// ---------------------------------------------------------------- NVS config

static void config_load(runtime_config_t *c)
{
    c->fps = 5;
    c->pixel_threshold = 25;
    c->min_ratio = 0.01f;
    nvs_handle_t h;
    if (nvs_open("edgefleet", NVS_READONLY, &h) != ESP_OK) return;
    int32_t v;
    if (nvs_get_i32(h, "fps", &v) == ESP_OK) c->fps = v;
    if (nvs_get_i32(h, "pix_thr", &v) == ESP_OK) c->pixel_threshold = v;
    if (nvs_get_i32(h, "min_ratio_k", &v) == ESP_OK) c->min_ratio = v / 1000.0f;
    int64_t cv;
    if (nvs_get_i64(h, "cfg_ver", &cv) == ESP_OK) s_config_version = cv;
    nvs_close(h);
}

static void config_save(const runtime_config_t *c)
{
    nvs_handle_t h;
    if (nvs_open("edgefleet", NVS_READWRITE, &h) != ESP_OK) return;
    nvs_set_i32(h, "fps", c->fps);
    nvs_set_i32(h, "pix_thr", c->pixel_threshold);
    nvs_set_i32(h, "min_ratio_k", (int32_t)(c->min_ratio * 1000));
    nvs_set_i64(h, "cfg_ver", s_config_version);
    nvs_commit(h);
    nvs_close(h);
}

// Records the version we are about to flash so that after reboot we can tell
// whether the update took or the bootloader rolled back.
static void pending_version_set(const char *v)
{
    nvs_handle_t h;
    if (nvs_open("edgefleet", NVS_READWRITE, &h) != ESP_OK) return;
    if (v) nvs_set_str(h, "pending_ver", v);
    else nvs_erase_key(h, "pending_ver");
    nvs_commit(h);
    nvs_close(h);
}

static void firmware_boot_check(void)
{
    const esp_app_desc_t *app = esp_app_get_description();
    nvs_handle_t h;
    char pending[32] = {0};
    size_t len = sizeof pending;
    if (nvs_open("edgefleet", NVS_READONLY, &h) == ESP_OK) {
        nvs_get_str(h, "pending_ver", pending, &len);
        nvs_close(h);
    }
    if (pending[0] && strcmp(pending, app->version) != 0) {
        ESP_LOGW(TAG, "update to %s did not stick; running %s", pending, app->version);
        strcpy(s_fw_state, "failed");
        s_error_count++;
    }
    pending_version_set(NULL);
}

// ---------------------------------------------------------------- WiFi

static void wifi_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        xEventGroupClearBits(s_wifi_events, WIFI_CONNECTED_BIT);
        esp_wifi_connect();
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *e = (ip_event_got_ip_t *)data;
        ESP_LOGI(TAG, "got ip " IPSTR, IP2STR(&e->ip_info.ip));
        xEventGroupSetBits(s_wifi_events, WIFI_CONNECTED_BIT);
    }
}

static void wifi_init(void)
{
    s_wifi_events = xEventGroupCreate();
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, wifi_event_handler, NULL));
    wifi_config_t wc = {0};
    strlcpy((char *)wc.sta.ssid, CONFIG_EF_WIFI_SSID, sizeof wc.sta.ssid);
    strlcpy((char *)wc.sta.password, CONFIG_EF_WIFI_PASSWORD, sizeof wc.sta.password);
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wc));
    ESP_ERROR_CHECK(esp_wifi_start());
    // Device ID is derived from the station MAC so it is stable across reflashes.
    uint8_t mac[6];
    esp_wifi_get_mac(WIFI_IF_STA, mac);
    snprintf(s_device_id, sizeof s_device_id, "esp32-%02x%02x%02x", mac[3], mac[4], mac[5]);
}

// ---------------------------------------------------------------- HTTP

// POSTs body to url and reads up to RESP_BUF_SIZE-1 bytes of response into resp.
// Returns the HTTP status code or -1.
static int http_post(const char *url, const char *content_type, const char *body, char *resp)
{
    esp_http_client_config_t cfg = {
        .url = url,
        .timeout_ms = 8000,
        .crt_bundle_attach = esp_crt_bundle_attach,
    };
    esp_http_client_handle_t c = esp_http_client_init(&cfg);
    if (!c) return -1;
    esp_http_client_set_method(c, HTTP_METHOD_POST);
    esp_http_client_set_header(c, "Content-Type", content_type);
    if (strlen(CONFIG_EF_DEVICE_TOKEN) > 0) {
        char auth[128];
        snprintf(auth, sizeof auth, "Bearer %s", CONFIG_EF_DEVICE_TOKEN);
        esp_http_client_set_header(c, "Authorization", auth);
    }
    int status = -1;
    int len = strlen(body);
    if (esp_http_client_open(c, len) == ESP_OK && esp_http_client_write(c, body, len) == len) {
        esp_http_client_fetch_headers(c);
        status = esp_http_client_get_status_code(c);
        if (resp) {
            int n = esp_http_client_read_response(c, resp, RESP_BUF_SIZE - 1);
            resp[n > 0 ? n : 0] = '\0';
        }
    }
    esp_http_client_close(c);
    esp_http_client_cleanup(c);
    return status;
}

static void ensure_device_registered(void)
{
    char url[160], body[192];
    snprintf(url, sizeof url, "%s/v1/devices", CONFIG_EF_CONTROL_PLANE_URL);
    snprintf(body, sizeof body,
             "{\"id\":\"%s\",\"kind\":\"esp32-cam\",\"group\":\"esp32\",\"firmware_version\":\"%s\"}",
             s_device_id, esp_app_get_description()->version);
    int st = http_post(url, "application/json", body, NULL);
    ESP_LOGI(TAG, "register %s -> %d", s_device_id, st);  // 201 or 409 (already registered)
}

// ---------------------------------------------------------------- commands

static char s_seen[SEEN_RING][24];
static int s_seen_next;

static bool already_seen(const char *id)
{
    for (int i = 0; i < SEEN_RING; i++)
        if (strcmp(s_seen[i], id) == 0) return true;
    strlcpy(s_seen[s_seen_next], id, sizeof s_seen[0]);
    s_seen_next = (s_seen_next + 1) % SEEN_RING;
    return false;
}

static const char *payload_str(const cJSON *payload, const char *key)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(payload, key);
    return cJSON_IsString(v) ? v->valuestring : NULL;
}

static void do_firmware_update(const char *url, const char *version)
{
    ESP_LOGI(TAG, "OTA %s from %s", version, url);
    strcpy(s_fw_state, "updating");
    esp_http_client_config_t http = {.url = url, .timeout_ms = 30000, .crt_bundle_attach = esp_crt_bundle_attach};
    esp_https_ota_config_t ota = {.http_config = &http};
    pending_version_set(version);
    esp_err_t err = esp_https_ota(&ota);
    if (err == ESP_OK) {
        ESP_LOGI(TAG, "OTA written; rebooting into %s", version);
        vTaskDelay(pdMS_TO_TICKS(200));
        esp_restart();
    }
    ESP_LOGE(TAG, "OTA failed: %s", esp_err_to_name(err));
    pending_version_set(NULL);
    strcpy(s_fw_state, "failed");
    s_error_count++;
}

static void do_firmware_rollback(void)
{
    const esp_partition_t *other = esp_ota_get_next_update_partition(NULL);
    esp_app_desc_t desc;
    if (!other || esp_ota_get_partition_description(other, &desc) != ESP_OK) {
        ESP_LOGE(TAG, "rollback: no valid image in the other slot");
        s_error_count++;
        return;
    }
    ESP_LOGW(TAG, "rolling back to %s in %s", desc.version, other->label);
    if (esp_ota_set_boot_partition(other) == ESP_OK) {
        vTaskDelay(pdMS_TO_TICKS(200));
        esp_restart();
    }
    s_error_count++;
}

static void handle_command(const cJSON *cmd, runtime_config_t *cfg)
{
    const char *id = payload_str(cmd, "id");
    const char *type = payload_str(cmd, "type");
    const cJSON *payload = cJSON_GetObjectItemCaseSensitive(cmd, "payload");
    if (!id || !type || already_seen(id)) return;  // delivery is at-least-once
    ESP_LOGI(TAG, "command %s %s", id, type);

    if (strcmp(type, "set_config") == 0) {
        const char *v;
        if ((v = payload_str(payload, "fps"))) cfg->fps = atoi(v);
        if ((v = payload_str(payload, "motion_threshold"))) cfg->pixel_threshold = atoi(v);
        if ((v = payload_str(payload, "min_motion_ratio"))) cfg->min_ratio = strtof(v, NULL);
        if ((v = payload_str(payload, "config_version"))) s_config_version = strtoll(v, NULL, 10);
        if (cfg->fps < 1) cfg->fps = 1;
        if (cfg->fps > 15) cfg->fps = 15;
        config_save(cfg);
        xQueueOverwrite(s_config_q, cfg);
    } else if (strcmp(type, "update_firmware") == 0) {
        const char *url = payload_str(payload, "url");
        const char *ver = payload_str(payload, "version");
        if (url && ver) do_firmware_update(url, ver);
    } else if (strcmp(type, "rollback_firmware") == 0) {
        do_firmware_rollback();
    } else if (strcmp(type, "reboot") == 0) {
        esp_restart();
    }
}

// ---------------------------------------------------------------- tasks

static void heartbeat_task(void *arg)
{
    runtime_config_t cfg;
    config_load(&cfg);
    xQueueOverwrite(s_config_q, &cfg);

    char url[160];
    snprintf(url, sizeof url, "%s/v1/agent/%s/heartbeat", CONFIG_EF_CONTROL_PLANE_URL, s_device_id);
    char *resp = malloc(RESP_BUF_SIZE);
    bool verified = false;

    xEventGroupWaitBits(s_wifi_events, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
    ensure_device_registered();

    for (;;) {
        xEventGroupWaitBits(s_wifi_events, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
        wifi_ap_record_t ap = {0};
        esp_wifi_sta_get_ap_info(&ap);

        cJSON *hb = cJSON_CreateObject();
        cJSON_AddStringToObject(hb, "firmware_version", esp_app_get_description()->version);
        cJSON_AddStringToObject(hb, "firmware_state", s_fw_state);
        cJSON_AddNumberToObject(hb, "config_version", (double)s_config_version);
        cJSON *t = cJSON_AddObjectToObject(hb, "telemetry");
        cJSON_AddNumberToObject(t, "mem_free_kb", esp_get_free_heap_size() / 1024);
        cJSON_AddNumberToObject(t, "fps", cfg.fps);
        cJSON_AddNumberToObject(t, "rssi_dbm", ap.rssi);
        cJSON_AddNumberToObject(t, "errors", s_error_count);
        cJSON_AddNumberToObject(t, "uptime_sec", (double)(esp_timer_get_time() / 1000000));
        char *body = cJSON_PrintUnformatted(hb);
        cJSON_Delete(hb);

        int status = http_post(url, "application/json", body, resp);
        cJSON_free(body);
        if (status == 200) {
            if (!verified) {
                // First successful round-trip on this image: commit it so the
                // bootloader will not roll back.
                esp_ota_img_states_t st;
                if (esp_ota_get_state_partition(esp_ota_get_running_partition(), &st) == ESP_OK &&
                    st == ESP_OTA_IMG_PENDING_VERIFY) {
                    esp_ota_mark_app_valid_cancel_rollback();
                    ESP_LOGI(TAG, "new firmware verified");
                }
                verified = true;
            }
            cJSON *root = cJSON_Parse(resp);
            const cJSON *cmds = cJSON_GetObjectItemCaseSensitive(root, "commands");
            const cJSON *cmd;
            cJSON_ArrayForEach(cmd, cmds) handle_command(cmd, &cfg);
            cJSON_Delete(root);
        } else {
            ESP_LOGW(TAG, "heartbeat failed: %d", status);
            s_error_count++;
        }
        vTaskDelay(pdMS_TO_TICKS(CONFIG_EF_HEARTBEAT_MS));
    }
}

// AI-Thinker ESP32-CAM pin map.
static esp_err_t camera_init(void)
{
    camera_config_t c = {
        .pin_pwdn = 32, .pin_reset = -1, .pin_xclk = 0,
        .pin_sccb_sda = 26, .pin_sccb_scl = 27,
        .pin_d7 = 35, .pin_d6 = 34, .pin_d5 = 39, .pin_d4 = 36,
        .pin_d3 = 21, .pin_d2 = 19, .pin_d1 = 18, .pin_d0 = 5,
        .pin_vsync = 25, .pin_href = 23, .pin_pclk = 22,
        .xclk_freq_hz = 20000000,
        .ledc_timer = LEDC_TIMER_0, .ledc_channel = LEDC_CHANNEL_0,
        .pixel_format = PIXFORMAT_GRAYSCALE,
        .frame_size = FRAMESIZE_QQVGA,  // 160x120: cheap enough to diff at 15 fps
        .fb_count = 2,
        .fb_location = CAMERA_FB_IN_PSRAM,
        .grab_mode = CAMERA_GRAB_LATEST,
    };
    return esp_camera_init(&c);
}

static void camera_task(void *arg)
{
    if (camera_init() != ESP_OK) {
        ESP_LOGE(TAG, "camera init failed");
        s_error_count++;
        vTaskDelete(NULL);
        return;
    }
    runtime_config_t cfg;
    xQueuePeek(s_config_q, &cfg, portMAX_DELAY);
    uint8_t *prev = NULL;
    size_t prev_len = 0;
    uint32_t seq = 0;
    for (;;) {
        xQueuePeek(s_config_q, &cfg, 0);  // pick up live config changes
        camera_fb_t *fb = esp_camera_fb_get();
        if (!fb) {
            s_error_count++;
            vTaskDelay(pdMS_TO_TICKS(100));
            continue;
        }
        if (prev && prev_len == fb->len) {
            detection_t d = {0};
            if (motion_detect(prev, fb->buf, fb->width, fb->height, cfg.pixel_threshold, cfg.min_ratio, &d.m)) {
                d.seq = ++seq;
                d.ts_ms = esp_timer_get_time() / 1000;
                xQueueSend(s_event_q, &d, 0);  // drop if events_task is behind
            }
        }
        if (!prev || prev_len != fb->len) {
            free(prev);
            prev = heap_caps_malloc(fb->len, MALLOC_CAP_SPIRAM);
            prev_len = fb->len;
        }
        if (prev) memcpy(prev, fb->buf, fb->len);
        esp_camera_fb_return(fb);
        vTaskDelay(pdMS_TO_TICKS(1000 / cfg.fps));
    }
}

static void events_task(void *arg)
{
    detection_t d;
    for (;;) {
        // Block for the first event, then drain up to a batch.
        if (xQueueReceive(s_event_q, &d, portMAX_DELAY) != pdTRUE) continue;
        cJSON *root = cJSON_CreateObject();
        cJSON *records = cJSON_AddArrayToObject(root, "records");
        int n = 0;
        do {
            cJSON *rec = cJSON_CreateObject();
            cJSON_AddStringToObject(rec, "key", s_device_id);
            cJSON *v = cJSON_AddObjectToObject(rec, "value");
            cJSON_AddStringToObject(v, "device_id", s_device_id);
            cJSON_AddNumberToObject(v, "seq", d.seq);
            cJSON_AddStringToObject(v, "label", "motion");
            float conf = d.m.changed_ratio * 10.0f;  // 10% of frame changed -> 1.0
            cJSON_AddNumberToObject(v, "confidence", conf > 1.0f ? 1.0f : conf);
            int bbox[4] = {d.m.x, d.m.y, d.m.w, d.m.h};
            cJSON_AddItemToObject(v, "bbox", cJSON_CreateIntArray(bbox, 4));
            cJSON_AddNumberToObject(v, "uptime_ms", (double)d.ts_ms);
            cJSON_AddItemToArray(records, rec);
            n++;
        } while (n < EVENT_BATCH && xQueueReceive(s_event_q, &d, 0) == pdTRUE);
        char *body = cJSON_PrintUnformatted(root);
        cJSON_Delete(root);
        int st = http_post(CONFIG_EF_EVENTS_URL, "application/vnd.kafka.json.v2+json", body, NULL);
        if (st < 200 || st >= 300) {
            ESP_LOGW(TAG, "event batch (%d) rejected: %d", n, st);
            s_error_count++;
        }
        cJSON_free(body);
    }
}

void app_main(void)
{
    esp_err_t err = nvs_flash_init();
    if (err == ESP_ERR_NVS_NO_FREE_PAGES || err == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        err = nvs_flash_init();
    }
    ESP_ERROR_CHECK(err);
    firmware_boot_check();

    s_event_q = xQueueCreate(64, sizeof(detection_t));
    s_config_q = xQueueCreate(1, sizeof(runtime_config_t));

    wifi_init();
    ESP_LOGI(TAG, "device %s firmware %s", s_device_id, esp_app_get_description()->version);

    xTaskCreate(heartbeat_task, "heartbeat", 8192, NULL, 5, NULL);
    xTaskCreate(camera_task, "camera", 4096, NULL, 4, NULL);
    xTaskCreate(events_task, "events", 6144, NULL, 3, NULL);
}
