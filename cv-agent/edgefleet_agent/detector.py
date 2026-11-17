"""Motion + person detection with OpenCV.

Pipeline per frame:
  1. Downscale to 640px wide, grayscale, Gaussian blur.
  2. MOG2 background subtraction -> foreground mask.
  3. Morphological open/dilate to remove speckle and join blobs.
  4. Contours larger than min_area become "motion" detections.
  5. Optionally run the HOG+SVM people detector inside motion regions only,
     which keeps a Raspberry Pi 4 at ~10 fps instead of ~2 fps full-frame.
"""

from __future__ import annotations

import dataclasses

import cv2
import numpy as np


@dataclasses.dataclass
class Detection:
    label: str
    confidence: float
    bbox: tuple[int, int, int, int]  # x, y, w, h in original-frame pixels


class Detector:
    def __init__(self, motion_threshold: int = 25, min_area: int = 800, detect_people: bool = True,
                 work_width: int = 640, history: int = 300):
        self.min_area = min_area
        self.detect_people = detect_people
        self.work_width = work_width
        self._bg = cv2.createBackgroundSubtractorMOG2(history=history, varThreshold=motion_threshold, detectShadows=False)
        self._kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        self._hog = cv2.HOGDescriptor()
        self._hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())

    def set_threshold(self, value: int) -> None:
        self._bg.setVarThreshold(value)

    def process(self, frame: np.ndarray) -> list[Detection]:
        h, w = frame.shape[:2]
        scale = self.work_width / w if w > self.work_width else 1.0
        small = cv2.resize(frame, (int(w * scale), int(h * scale))) if scale != 1.0 else frame
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY) if small.ndim == 3 else small
        gray = cv2.GaussianBlur(gray, (5, 5), 0)

        mask = self._bg.apply(gray)
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, self._kernel)
        mask = cv2.dilate(mask, self._kernel, iterations=2)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        min_area_scaled = self.min_area * scale * scale
        out: list[Detection] = []
        for c in contours:
            area = cv2.contourArea(c)
            if area < min_area_scaled:
                continue
            x, y, bw, bh = cv2.boundingRect(c)
            box = tuple(int(v / scale) for v in (x, y, bw, bh))
            label, conf = "motion", min(1.0, area / (small.shape[0] * small.shape[1]) * 20)
            if self.detect_people and bh >= 64 and bw >= 32:
                person = self._person_in(gray, x, y, bw, bh)
                if person is not None:
                    label, conf = "person", person
            out.append(Detection(label, round(float(conf), 3), box))  # type: ignore[arg-type]
        return out

    def _person_in(self, gray: np.ndarray, x: int, y: int, w: int, h: int) -> float | None:
        pad = 16
        roi = gray[max(0, y - pad): y + h + pad, max(0, x - pad): x + w + pad]
        if roi.shape[0] < 128 or roi.shape[1] < 64:
            roi = cv2.resize(roi, (max(64, roi.shape[1]), max(128, roi.shape[0])))
        rects, weights = self._hog.detectMultiScale(roi, winStride=(8, 8), padding=(8, 8), scale=1.05)
        if len(rects) == 0:
            return None
        return float(min(1.0, max(np.ravel(weights))))
