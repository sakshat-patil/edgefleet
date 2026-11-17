#include "motion.h"
#include <stdlib.h>

int motion_detect(const uint8_t *prev, const uint8_t *cur, int width, int height,
                  int pixel_threshold, float min_ratio, motion_result_t *out)
{
    int minx = width, miny = height, maxx = -1, maxy = -1, changed = 0;
    // Sample every other pixel in both axes: 4x cheaper, plenty for QQVGA.
    for (int y = 0; y < height; y += 2) {
        const uint8_t *p = prev + (size_t)y * width;
        const uint8_t *c = cur + (size_t)y * width;
        for (int x = 0; x < width; x += 2) {
            if (abs((int)c[x] - (int)p[x]) > pixel_threshold) {
                changed++;
                if (x < minx) minx = x;
                if (x > maxx) maxx = x;
                if (y < miny) miny = y;
                if (y > maxy) maxy = y;
            }
        }
    }
    int sampled = ((width + 1) / 2) * ((height + 1) / 2);
    out->changed_pixels = changed;
    out->changed_ratio = sampled ? (float)changed / (float)sampled : 0.0f;
    if (changed == 0) {
        out->x = out->y = out->w = out->h = 0;
        return 0;
    }
    out->x = minx;
    out->y = miny;
    out->w = maxx - minx + 1;
    out->h = maxy - miny + 1;
    return out->changed_ratio >= min_ratio;
}
