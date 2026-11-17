// Host unit test: cc -I../main ../main/motion.c test_motion.c -o t && ./t
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "motion.h"

#define W 160
#define H 120

int main(void)
{
    static uint8_t a[W * H], b[W * H];
    motion_result_t r;

    memset(a, 100, sizeof a);
    memset(b, 100, sizeof b);
    assert(motion_detect(a, b, W, H, 25, 0.01f, &r) == 0);
    assert(r.changed_pixels == 0);

    // Small sensor noise below threshold is ignored.
    for (int i = 0; i < W * H; i += 7) b[i] = 110;
    assert(motion_detect(a, b, W, H, 25, 0.01f, &r) == 0);

    // A 40x40 bright object appears at (60, 30).
    memset(b, 100, sizeof b);
    for (int y = 30; y < 70; y++)
        for (int x = 60; x < 100; x++) b[y * W + x] = 220;
    assert(motion_detect(a, b, W, H, 25, 0.01f, &r) == 1);
    assert(r.x == 60 && r.y == 30);
    assert(r.w >= 39 && r.w <= 40 && r.h >= 39 && r.h <= 40);
    assert(r.changed_ratio > 0.07f && r.changed_ratio < 0.09f);

    // Same object but min_ratio set above its size -> not reported.
    assert(motion_detect(a, b, W, H, 25, 0.5f, &r) == 0);

    puts("motion tests passed");
    return 0;
}
