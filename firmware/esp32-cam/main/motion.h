// Frame-differencing motion detector. Pure C with no ESP-IDF dependencies so
// it can be unit-tested on the host (see test/test_motion.c).
#pragma once
#include <stdint.h>
#include <stddef.h>

typedef struct {
    int x, y, w, h;       // bounding box of changed pixels
    int changed_pixels;
    float changed_ratio;  // changed_pixels / (width * height)
} motion_result_t;

// Compares two 8-bit grayscale frames. A pixel is "changed" when
// |cur - prev| > pixel_threshold. Returns 1 if changed_ratio >= min_ratio.
int motion_detect(const uint8_t *prev, const uint8_t *cur, int width, int height,
                  int pixel_threshold, float min_ratio, motion_result_t *out);
